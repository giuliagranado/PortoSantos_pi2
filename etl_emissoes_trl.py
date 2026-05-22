"""
ETL Pipeline — Emissões Portuárias por Navio | Porto de Santos (2020–2025)
Metodologia: Transport Research Laboratory (TRL) — Hickman et al. (1999)
Referência: Gerson Bauer, UNIMES, 2023 — seções 4.1 e Tabela 4


═══════════════════════════════════════════════════════════════════════════
MÚLTIPLOS CSVs
  CSV_INPUT aceita três formatos:
    • str com path único   → "atracacoes_2025.csv"
    • str com glob         → "./dados/*.csv"
    • list de paths        → ["2023.csv", "2024.csv", "2025.csv"]
  Todos os CSVs devem ter o mesmo schema (mesmas colunas e tipos).

MODELO DE PARALELISMO
  Fase 1 (PDFs)  → ThreadPoolExecutor
    Motivo: pdfplumber é dominado por I/O de disco; GIL liberado durante
    leitura. Cada thread opera em seu próprio objeto PDF (sem estado compartilhado).

  Fase 2 (CSVs)  → ThreadPoolExecutor para leitura + pré-processamento
    Motivo: pd.read_csv libera o GIL durante I/O; leitura de N arquivos
    grandes em paralelo reduz tempo proporcional ao número de workers.
    Normalização de ANO_MES e mapeamento TRL_CATEGORIA são feitos dentro
    de cada thread para evitar reprocessamento sequencial posterior.

  Fase 3 (cálculos numpy) → sequencial, já vetorizado
    Motivo: np.select e operações aritméticas em Series usam BLAS
    internamente (multicore automático). Adicionar ProcessPoolExecutor
    aqui introduziria overhead de pickling maior que o ganho.

  Fase 4 (export) → sequencial (I/O único de escrita XLSX)

  NOTA sobre ProcessPoolExecutor:
    Para datasets históricos >500k linhas onde o groupby leva >1s,
    ProcessPoolExecutor com chunks pode ajudar na Fase 2. Veja a função
    _groupby_paralelo() que demonstra essa abordagem, desativada por padrão
    porque o overhead de pickling negativa o ganho abaixo de ~200k linhas.
═══════════════════════════════════════════════════════════════════════════
"""

import os
import re
import glob as glob_module
import logging
import threading
import unicodedata
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pdfplumber

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES GLOBAIS — ajuste aqui, não mexa no resto
# ──────────────────────────────────────────────────────────────────────────────

# Input de CSVs: str (path único ou glob) ou list[str]
CSV_INPUT = "./data/exportacao_atracacao.csv"
# Exemplos alternativos:
# CSV_INPUT = "./dados_atracacao/*.csv"
# CSV_INPUT = ["atracacoes_2023.csv", "atracacoes_2024.csv", "atracacoes_2025.csv"]

PDF_DIR          = "./mensarios_pdf"
CSV_FATURAMENTO  = "Faturamento porto.csv"
XLSX_TONELAGEM   = "tonelagem_historica_pdf.xlsx"
XLSX_FINAL       = "emissoes_trl_detalhado_join_2020_2025.xlsx"

# Workers para I/O paralelo (PDFs e CSVs).
# Regra empírica: min(8, cpu_count * 2) para I/O-bound tasks.
# Para discos SSD NVMe, aumentar até 16. Para HDD, manter em 2-4.
N_WORKERS = min(8, (os.cpu_count() or 1) * 2)

# ── Parâmetros TRL ────────────────────────────────────────────────────────────
IDX_SOMA = {
    "CARGA GERAL":    2,
    "GRANEL SOLIDO":  5,
    "GRANEL LIQUIDO": 8,
}

# MAPA TRL RESTRITO - Sem passageiros ou outros
MAPA_TRL = {
    "CARGA GERAL":           "CARGA GERAL",
    "CARGA CONTEINERIZADA":  "CARGA GERAL",
    "ROLL-ON/ROLL-OFF":      "CARGA GERAL",
    "GRANEL SOLIDO":         "GRANEL SOLIDO",
    "GRANEL LIQUIDO":        "GRANEL LIQUIDO",
}

MESES_NORM = {
    "JANEIRO": 1, "FEVEREIRO": 2, "MARCO": 3,
    "ABRIL": 4,   "MAIO": 5,     "JUNHO": 6,
    "JULHO": 7,   "AGOSTO": 8,   "SETEMBRO": 9,
    "OUTUBRO": 10, "NOVEMBRO": 11, "DEZEMBRO": 12,
}

FATOR_OPERACIONAL    = 0.4
DURACAO_MANOBRA_DIAS = 3.0 / 24   # 3h → 0.125 dias

FATORES_EMISSAO = {
    "NOx_KG":  78.0,
    "CO_KG":   28.0,
    "CO2_KG":  3200.0,
    "VOC_KG":  3.6,
    "PM_KG":   1.2,
    "SOx_KG":  10.0,
}

DTYPES_CSV = {
    "MERCADORIA":           "category",
    "PERFIL_EMBARCACAO":    "category",
    "NATUREZA_CARGA":       "category",
    "TIPO_VIAGEM":          "category",
    "ANO_MES":              "str",
    "ANO":                  "int16",
    "MES":                  "int8",
    "QUANTIDADE_ATRACACAO": "int8",
}


# ──────────────────────────────────────────────────────────────────────────────
# LOGGING THREAD-SAFE
# ──────────────────────────────────────────────────────────────────────────────

_LOG_LOCK = threading.Lock()

def _log(msg: str, indent: int = 0) -> None:
    """
    Print thread-safe com indentação opcional.
    O Lock garante que mensagens de threads concorrentes não se entrelacem.
    """
    prefix = "  " * indent
    with _LOG_LOCK:
        print(f"{prefix}{msg}")


# ──────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS
# ──────────────────────────────────────────────────────────────────────────────

def _normalizar(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", str(texto))
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def _limpar_numero(texto: str) -> int:
    limpo = re.sub(r"[^\d]", "", str(texto))
    return int(limpo) if limpo else 0


def _extrair_numericos(linha_sem_mes: str) -> list[int]:
    tokens = re.findall(r"\d+(?:\.\d+)*", linha_sem_mes)
    return [_limpar_numero(t) for t in tokens]


def _identificar_mes(palavra: str) -> int | None:
    return MESES_NORM.get(_normalizar(palavra))


def _eh_linha_total(linha: str) -> bool:
    return re.sub(r"\s+", "", linha).upper().startswith("TOTAL")


def _resolver_csv_inputs(csv_input) -> list[str]:
    """
    Converte CSV_INPUT (str ou list) em lista de paths resolvidos.
    Suporta:
      - path único   : "arquivo.csv"
      - glob pattern : "./dados/*.csv"
      - list de paths: ["a.csv", "b.csv"]
    """
    if isinstance(csv_input, (list, tuple)):
        paths = list(csv_input)
    elif isinstance(csv_input, str):
        paths = sorted(glob_module.glob(csv_input))
        if not paths:
            # Trata como path único (deixa o erro para o read_csv)
            paths = [csv_input]
    else:
        raise TypeError(f"CSV_INPUT deve ser str ou list, recebeu {type(csv_input)}")

    ausentes = [p for p in paths if not os.path.exists(p)]
    if ausentes:
        raise FileNotFoundError(f"Arquivos CSV não encontrados: {ausentes}")

    return paths


# ──────────────────────────────────────────────────────────────────────────────
# FASE 1 — EXTRAÇÃO DE TONELAGEM DOS PDFs (PARALELA)
# ──────────────────────────────────────────────────────────────────────────────

def _localizar_fl07(pdf: pdfplumber.PDF) -> tuple[object | None, int]:
    """
    Localiza FL.07 ('EVOLUÇÃO DA MOVIMENTAÇÃO MENSAL' do Porto total).
    Thread-safe: opera em instâncias locais de pdfplumber (sem estado global).
    """
    candidatos = []
    for i, pagina in enumerate(pdf.pages, start=1):
        texto = pagina.extract_text() or ""
        if re.search(r"FL[\s.\-]*07", texto, re.IGNORECASE):
            return pagina, i
        norm = _normalizar(texto)
        if "EVOLU" in norm and "MENSAL" in norm and "ORGANIZADO" not in norm:
            candidatos.append((i, pagina))

    if candidatos:
        return candidatos[0][1], candidatos[0][0]

    if len(pdf.pages) >= 7:
        return pdf.pages[6], 7

    return None, -1


def _extrair_tonelagem_pdf(caminho: str, ano: int) -> pd.DataFrame:
    """
    Extrai tonelagem por categoria TRL da página FL.07 de um PDF.
    Projetado para execução concorrente: sem estado compartilhado externo.
    Cada chamada abre seu próprio contexto pdfplumber.
    """
    nome = os.path.basename(caminho)
    acumulado = {cat: {"IMP": [0]*12, "EXP": [0]*12} for cat in IDX_SOMA}

    try:
        with pdfplumber.open(caminho) as pdf:
            pagina, num_pag = _localizar_fl07(pdf)
            if pagina is None:
                _log(f"⚠ FL.07 não encontrada: {nome}", indent=2)
                return pd.DataFrame()

            texto = pagina.extract_text() or ""
            if not texto:
                _log(f"⚠ Texto vazio na FL.07: {nome}", indent=2)
                return pd.DataFrame()

            bloco_atual = None
            meses_por_bloco: dict[str, set] = {"IMP": set(), "EXP": set()}

            for linha in texto.split("\n"):
                ls = linha.strip()
                if not ls:
                    continue
                ln = _normalizar(ls)
                # Compacta para detectar cabeçalhos em dois formatos:
                #   Atual  (2025): "IMPORTACAO"
                #   Antigo (2020): "I M P O R T A C A O" -> compacto = "IMPORTACAO"
                ln_compact = re.sub(r"\s+", "", ln)

                if "IMPORTA" in ln_compact and len(ls) < 50:
                    bloco_atual = "IMP"
                    continue
                if "EXPORTA" in ln_compact and len(ls) < 50:
                    bloco_atual = "EXP"
                    continue
                if bloco_atual is None or _eh_linha_total(ls):
                    continue

                partes = ls.split()
                mes_num = _identificar_mes(partes[0])
                if mes_num is None:
                    continue

                valores = _extrair_numericos(" ".join(partes[1:]))
                if len(valores) < 9:
                    _log(f"⚠ {nome} | {bloco_atual} | mês {mes_num}: {len(valores)} valores", indent=3)
                    continue

                for cat, idx in IDX_SOMA.items():
                    acumulado[cat][bloco_atual][mes_num - 1] += valores[idx]
                meses_por_bloco[bloco_atual].add(mes_num)

            # Diagnóstico de completude
            for bloco, meses in meses_por_bloco.items():
                ausentes = sorted(set(range(1, 13)) - meses)
                if ausentes:
                    _log(f"⚠ {nome} | {bloco}: meses ausentes → {ausentes}", indent=3)

    except Exception as exc:
        _log(f"✗ Erro ao processar {nome}: {exc}", indent=2)
        return pd.DataFrame()

    registros = [
        {
            "ANO_MES":         f"{ano}-{mes_idx+1:02d}",
            "TRL_CATEGORIA":   cat,
            "NET_TONNAGE_MES": blocos["IMP"][mes_idx] + blocos["EXP"][mes_idx],
        }
        for cat, blocos in acumulado.items()
        for mes_idx in range(12)
        if blocos["IMP"][mes_idx] + blocos["EXP"][mes_idx] > 0
    ]

    return pd.DataFrame(registros)


def fase1_extrair_pdfs() -> pd.DataFrame:
    """
    Itera sobre PDFs em PDF_DIR e extrai tonelagem mensal por categoria TRL.
    Usa ThreadPoolExecutor: leitura de PDFs é dominada por I/O de disco;
    threads paralelas reduzem tempo total de forma linear com N_WORKERS.
    Cache: se XLSX_TONELAGEM existir, pula reprocessamento.
    """
    _log("\n" + "═" * 70)
    _log("FASE 1 — Extração de tonelagem dos PDFs (paralela)")
    _log("═" * 70)

    if os.path.exists(XLSX_TONELAGEM):
        _log(f"✔ '{XLSX_TONELAGEM}' existe. Carregando cache.", indent=1)
        df = pd.read_excel(
            XLSX_TONELAGEM,
            dtype={"ANO_MES": str, "NET_TONNAGE_MES": "int64"},
        )
        _log(f"→ {len(df):,} registros | {df['ANO_MES'].nunique()} meses.", indent=1)
        return df

    pdfs = sorted(glob_module.glob(os.path.join(PDF_DIR, "*.pdf")))
    if not pdfs:
        raise FileNotFoundError(f"Nenhum PDF em '{PDF_DIR}'.")

    # Extrai (caminho, ano) para cada PDF
    tarefas: list[tuple[str, int]] = []
    for caminho in pdfs:
        match = re.search(r"(20\d{2}|19\d{2})", os.path.basename(caminho))
        if match:
            tarefas.append((caminho, int(match.group(1))))
        else:
            _log(f"⚠ Ano não identificado: {os.path.basename(caminho)}", indent=1)

    _log(f"→ {len(tarefas)} PDFs a processar | {N_WORKERS} workers paralelos.", indent=1)

    frames: list[pd.DataFrame] = []
    erros: list[str] = []

    # ── Extração paralela ──────────────────────────────────────────────────────
    # ThreadPoolExecutor é adequado pois pdfplumber é I/O-bound.
    # Cada thread abre seu próprio arquivo: sem estado compartilhado.
    with ThreadPoolExecutor(max_workers=N_WORKERS) as executor:
        future_to_info = {
            executor.submit(_extrair_tonelagem_pdf, cam, ano): (cam, ano)
            for cam, ano in tarefas
        }

        for future in as_completed(future_to_info):
            cam, ano = future_to_info[future]
            nome = os.path.basename(cam)
            try:
                df_pdf = future.result()
                if df_pdf.empty:
                    erros.append(nome)
                else:
                    frames.append(df_pdf)
                    _log(f"✔ {nome} → {len(df_pdf)} registros", indent=2)
            except Exception as exc:
                erros.append(nome)
                _log(f"✗ {nome}: {exc}", indent=2)

    if erros:
        _log(f"\n⚠ PDFs sem dados: {erros}", indent=1)
    if not frames:
        raise ValueError("Nenhum dado extraído dos PDFs.")

    df_total = (
        pd.concat(frames, ignore_index=True)
        .groupby(["ANO_MES", "TRL_CATEGORIA"], as_index=False)["NET_TONNAGE_MES"]
        .sum()
        .sort_values(["ANO_MES", "TRL_CATEGORIA"])
        .reset_index(drop=True)
    )

    _log(f"\n→ Consolidado: {len(df_total):,} registros | "
         f"{df_total['ANO_MES'].min()} a {df_total['ANO_MES'].max()}", indent=1)
    df_total.to_excel(XLSX_TONELAGEM, index=False)
    _log(f"✔ Cache salvo em '{XLSX_TONELAGEM}'.", indent=1)

    return df_total


# ──────────────────────────────────────────────────────────────────────────────
# FASE 2 — LEITURA DE MÚLTIPLOS CSVs E CRUZAMENTO (PARALELA)
# ──────────────────────────────────────────────────────────────────────────────

def _ler_e_preprocessar_csv(caminho: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Lê um único CSV e aplica pré-processamento inicial dentro da thread.
    Retorna (DataFrame enriquecido, lista de colunas originais).

    Executado em threads paralelas: todas as operações são locais ao DataFrame
    retornado — sem escrita em estado global.
    """
    tam_mb = os.path.getsize(caminho) / 1024 / 1024
    _log(f"Lendo: {os.path.basename(caminho)} ({tam_mb:.1f} MB)...", indent=2)

    df = pd.read_csv(
        caminho,
        sep=",",
        encoding="utf-8",
        low_memory=False,
        dtype=DTYPES_CSV,
    )
    colunas_originais = list(df.columns)

    # Pré-processamento feito aqui para não repetir na thread principal
    df["ANO_MES_NORM"] = df["ANO_MES"].astype(str).str[:7]
    df["TRL_CATEGORIA"] = (
        df["NATUREZA_CARGA"]
        .astype(str).str.upper().str.strip()
        .map(MAPA_TRL)
    )

    _log(f"✔ {os.path.basename(caminho)}: {len(df):,} linhas | "
         f"{df['TRL_CATEGORIA'].notna().sum():,} elegíveis TRL", indent=2)

    return df, colunas_originais


def _groupby_paralelo(df: pd.DataFrame, n_workers: int) -> pd.DataFrame:
    """
    Alternativa com ProcessPoolExecutor para groupby em datasets muito grandes.
    """
    chunk_size = max(len(df) // n_workers, 1)
    chunks = [
        df[df["TRL_CATEGORIA"].notna()].iloc[i : i + chunk_size]
        for i in range(0, len(df), chunk_size)
    ]

    def _groupby_chunk(chunk):
        return (
            chunk.groupby(["ANO_MES_NORM", "TRL_CATEGORIA"], as_index=False, observed=True)
            .size()
            .rename(columns={"size": "N"})
        )

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        parciais = list(executor.map(_groupby_chunk, chunks))

    return (
        pd.concat(parciais, ignore_index=True)
        .groupby(["ANO_MES_NORM", "TRL_CATEGORIA"], as_index=False)["N"]
        .sum()
        .rename(columns={"N": "QTD_NAVIOS_MES"})
    )


# Flag para ativar ProcessPoolExecutor no groupby (datasets > 500k linhas)
USE_PROCESS_POOL = False


def fase2_cruzamento(df_tonelagem: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Cruza o dataset CSV com as tonelagens provenientes do PDF.
    """
    _log("\n" + "═" * 70)
    _log("FASE 2 — Leitura de CSVs (paralela) e cruzamento com tonelagem")
    _log("═" * 70)

    caminhos = _resolver_csv_inputs(CSV_INPUT)
    _log(f"→ {len(caminhos)} CSV(s) identificados | {N_WORKERS} workers.", indent=1)
    for c in caminhos:
        tam = os.path.getsize(c) / 1024 / 1024
        _log(f"  • {os.path.basename(c)} ({tam:.1f} MB)", indent=1)

    resultados: list[pd.DataFrame] = []
    colunas_originais: list[str] = []
    erros_csv: list[str] = []

    with ThreadPoolExecutor(max_workers=min(N_WORKERS, len(caminhos))) as executor:
        future_to_path = {
            executor.submit(_ler_e_preprocessar_csv, cam): cam
            for cam in caminhos
        }

        for future in as_completed(future_to_path):
            cam = future_to_path[future]
            try:
                df_csv, cols_orig = future.result()
                resultados.append(df_csv)
                if not colunas_originais:
                    colunas_originais = cols_orig
            except Exception as exc:
                erros_csv.append(os.path.basename(cam))
                _log(f"✗ Erro ao ler {os.path.basename(cam)}: {exc}", indent=2)

    if erros_csv:
        _log(f"⚠ CSVs com erro de leitura: {erros_csv}", indent=1)
    if not resultados:
        raise RuntimeError("Nenhum CSV foi lido com sucesso.")

    _log(f"→ Concatenando {len(resultados)} DataFrame(s)...", indent=1)
    df = pd.concat(resultados, ignore_index=True)
    _log(f"✔ Total: {len(df):,} linhas | {df.shape[1]} colunas.", indent=1)

    # 1. Eliminação de Registros Inválidos (Apenas Cargueiros)
    linhas_iniciais = len(df)
    df = df[df["TRL_CATEGORIA"].notna()]
    _log(f"→ Navios de carga elegíveis TRL: {len(df):,} (Removidos {linhas_iniciais - len(df):,} não-cargueiros)", indent=1)

    # 2. Filtragem Temporal Estrita (Somente meses presentes nos PDFs)
    meses_pdf = df_tonelagem["ANO_MES"].unique()
    linhas_apos_carga = len(df)
    df = df[df["ANO_MES_NORM"].isin(meses_pdf)]
    _log(f"→ Viagens dentro dos anos/meses dos PDFs: {len(df):,} (Removidas {linhas_apos_carga - len(df):,} sem PDF correspondente)", indent=1)

    _log("→ Calculando QTD_NAVIOS_MES por (ANO_MES_NORM, TRL_CATEGORIA)...", indent=1)

    if USE_PROCESS_POOL and len(df) > 500_000:
        _log(f"  [ProcessPoolExecutor ativo — {len(df):,} linhas > 500k]", indent=1)
        df_contagem = _groupby_paralelo(df, N_WORKERS)
    else:
        df_contagem = (
            df.groupby(["ANO_MES_NORM", "TRL_CATEGORIA"], as_index=False, observed=True)
            .size()
            .rename(columns={"size": "QTD_NAVIOS_MES"})
        )

    _log(f"✔ {len(df_contagem):,} combinações únicas.", indent=1)

    # 3. Inner Join (Sem Fallback/Imputação)
    df_ton_com_qtd = df_tonelagem.merge(
        df_contagem,
        left_on=["ANO_MES", "TRL_CATEGORIA"],
        right_on=["ANO_MES_NORM", "TRL_CATEGORIA"],
        how="inner",
    ).drop(columns=["ANO_MES_NORM"], errors="ignore")

    _log("→ Join final com todos os navios...", indent=1)
    n_antes = len(df)
    df_merged = df.merge(
        df_ton_com_qtd[["ANO_MES", "TRL_CATEGORIA", "NET_TONNAGE_MES", "QTD_NAVIOS_MES"]],
        left_on=["ANO_MES_NORM", "TRL_CATEGORIA"],
        right_on=["ANO_MES", "TRL_CATEGORIA"],
        how="inner",
        suffixes=("", "_PDF"),
    ).drop(columns=["ANO_MES_PDF", "ANO_MES_NORM"], errors="ignore").rename(columns={"ANO_MES_x": "ANO_MES"})

    _log(f"✔ Linhas antes: {n_antes:,} | após inner join: {len(df_merged):,}", indent=1)
    return df_merged, colunas_originais


# ──────────────────────────────────────────────────────────────────────────────
# FASE 2.5 — JOIN COM FATURAMENTO ANUAL
# ──────────────────────────────────────────────────────────────────────────────

def fase2_5_faturamento(df: pd.DataFrame) -> pd.DataFrame:
    """
    Left Join por ANO com Faturamento porto.csv.
    """
    _log("\n" + "═" * 70)
    _log("FASE 2.5 — Join com faturamento anual do porto")
    _log("═" * 70)

    if not os.path.exists(CSV_FATURAMENTO):
        _log(f"⚠ '{CSV_FATURAMENTO}' não encontrado — coluna omitida.", indent=1)
        return df

    df_fat = pd.read_csv(CSV_FATURAMENTO, quotechar='"')
    df_fat["FATURAMENTO_RMIL"] = (
        df_fat["Faturamento"].astype(str).str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float).astype("Int64")
    )
    df_fat["ANO"] = df_fat["Ano"].astype("int16")
    df_fat = df_fat[["ANO", "FATURAMENTO_RMIL"]]

    cobertura = f"{df_fat['ANO'].min()}–{df_fat['ANO'].max()}"
    _log(f"→ {len(df_fat)} anos com faturamento ({cobertura}) | unidade: R$ mil", indent=1)

    anos_csv = sorted(df["ANO"].unique())
    sem_fat  = [a for a in anos_csv if int(a) not in df_fat["ANO"].tolist()]
    if sem_fat:
        _log(f"⚠ Anos no CSV sem faturamento: {sem_fat} → NaN", indent=1)

    n_antes = len(df)
    df = df.merge(df_fat, on="ANO", how="left")
    assert len(df) == n_antes, "Join de faturamento duplicou linhas."

    n_com = df["FATURAMENTO_RMIL"].notna().sum()
    _log(f"✔ FATURAMENTO_RMIL: {n_com:,} com valor | {df['FATURAMENTO_RMIL'].isna().sum():,} NaN",
         indent=1)

    return df


# ──────────────────────────────────────────────────────────────────────────────
# FASE 3 — CÁLCULO DE EMISSÕES (VETORIZADO — SEQUENCIAL)
# ──────────────────────────────────────────────────────────────────────────────

def _consumo_max_por_perfil(gross: pd.Series, trl_cat: pd.Series) -> pd.Series:
    """
    Equações TRL (Hickman, 1999 / Bauer, 2023 Tabela 4) via np.select.
    """
    return np.select(
        [trl_cat == "GRANEL SOLIDO", trl_cat == "GRANEL LIQUIDO"],
        [20.186 + 0.00049 * gross, 14.685 + 0.00079 * gross],
        default=9.8197 + 0.00143 * gross,
    )


def fase3_calcular_emissoes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica fórmulas TRL vetorizadas e soma o total de poluentes.
    """
    _log("\n" + "═" * 70)
    _log("FASE 3 — Cálculo TRL vetorizado (numpy/BLAS)")
    _log("═" * 70)

    _log("A → NET_TONNAGE_NAVIO = NET_TONNAGE_MES / QTD_NAVIOS_MES", indent=1)
    df["NET_TONNAGE_NAVIO"] = df["NET_TONNAGE_MES"] / df["QTD_NAVIOS_MES"]

    _log("B → GROSS_TONNAGE_NAVIO = NET_TONNAGE_NAVIO / 0.80", indent=1)
    df["GROSS_TONNAGE_NAVIO"] = df["NET_TONNAGE_NAVIO"] / 0.80

    _log("C → CONSUMO_MAX por equação TRL (np.select)", indent=1)
    df["CONSUMO_MAX"] = _consumo_max_por_perfil(
        df["GROSS_TONNAGE_NAVIO"], df["TRL_CATEGORIA"]
    )

    _log(f"D → CONSUMO_EFETIVO = CONSUMO_MAX × {FATOR_OPERACIONAL} × {DURACAO_MANOBRA_DIAS}", indent=1)
    df["CONSUMO_EFETIVO_NAVIO"] = df["CONSUMO_MAX"] * FATOR_OPERACIONAL * DURACAO_MANOBRA_DIAS

    _log("E → Emissões por poluente (kg):", indent=1)
    for col, fator in FATORES_EMISSAO.items():
        df[col] = df["CONSUMO_EFETIVO_NAVIO"] * fator
        _log(f"   {col:10s} × {fator}", indent=1)

    _log("F → TOTAL_POLUENTES_KG = Soma horizontal de todos os poluentes", indent=1)
    colunas_poluentes = list(FATORES_EMISSAO.keys())
    df["TOTAL_POLUENTES_KG"] = df[colunas_poluentes].sum(axis=1)

    n_calc = df["NOx_KG"].notna().sum()
    _log(f"\n✔ Emissões calculadas: {n_calc:,} de {len(df):,} linhas.", indent=1)
    return df


# ──────────────────────────────────────────────────────────────────────────────
# FASE 4 — EXPORTAÇÃO FINAL (XLSX) E INTENSIDADE
# ──────────────────────────────────────────────────────────────────────────────

def fase4_exportar(df: pd.DataFrame, colunas_originais: list[str]) -> None:
    """
    Seleciona colunas finais, incluindo as geradas do Join entre CSV e PDF,
    arredonda poluentes e salva o formato XLSX.
    """
    _log("\n" + "═" * 70)
    _log("FASE 4 — Exportação final (Excel)")
    _log("═" * 70)

    # Inclui as colunas individuais e a nova coluna totalizadora
    colunas_poluentes = list(FATORES_EMISSAO.keys()) + ["TOTAL_POLUENTES_KG"]
    col_fat = ["FATURAMENTO_RMIL"] if "FATURAMENTO_RMIL" in df.columns else []

    # Adicionando as colunas calculadas/joinadas dos PDFs que antes eram descartadas
    colunas_calculadas = [
        "TRL_CATEGORIA", "NET_TONNAGE_MES", "QTD_NAVIOS_MES",
        "NET_TONNAGE_NAVIO", "GROSS_TONNAGE_NAVIO",
        "CONSUMO_MAX", "CONSUMO_EFETIVO_NAVIO"
    ]

    # Ordem final: [originais] + [calculadas_join] + [faturamento] + [poluentes]
    colunas_finais = colunas_originais + colunas_calculadas + col_fat + colunas_poluentes
    df_final = df[[c for c in colunas_finais if c in df.columns]].copy()

    df_final[colunas_poluentes] = df_final[colunas_poluentes].round(4)

    _log(f"→ Salvando Excel de Join ({XLSX_FINAL}) com openpyxl...", indent=1)
    df_final.to_excel(XLSX_FINAL, index=False, engine="openpyxl")
    
    _log(f"✔ Arquivo exportado com sucesso.", indent=1)

    # ── Sumário ───────────────────────────────────────────────────────────────
    _log("\n  ── Cobertura de emissões ────────────────────────────────────")
    n_calc = df_final["NOx_KG"].notna().sum()
    _log(f"     Com emissão : {n_calc:>10,}")
    _log(f"     Sem emissão : {df_final['NOx_KG'].isna().sum():>10,}  (passageiros/outros)")

    _log("\n  ── Emissões totais acumuladas ───────────────────────────────")
    for pol, total in df_final[colunas_poluentes].sum().items():
        _log(f"     {pol:20s}: {total:>18,.2f} kg  ({total/1_000_000:>8.2f} t)")
    
    if "FATURAMENTO_RMIL" in df_final.columns:
        _log("\n  ── Intensidade de Emissão (NOx por R$ mil) ─────────────────")
        intensidade = df_final.groupby("ANO").agg(
            NOx_total=("NOx_KG", "sum"),
            Fat=("FATURAMENTO_RMIL", "first")
        )
        intensidade["NOx_por_Rmil"] = (
            intensidade["NOx_total"] / intensidade["Fat"]
        ).round(6)
        _log(intensidade[["NOx_total", "Fat", "NOx_por_Rmil"]].to_string())
    _log("")


# ──────────────────────────────────────────────────────────────────────────────
# ORQUESTRADOR
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    import time
    t_inicio = time.perf_counter()

    _log("╔══════════════════════════════════════════════════════════════════╗")
    _log("║  ETL Emissões TRL | Porto de Santos 2020–2025                  ║")
    _log(f"║  Workers I/O: {N_WORKERS:<3} | ProcessPool: {'ON' if USE_PROCESS_POOL else 'OFF':<3}                           ║")
    _log("╚══════════════════════════════════════════════════════════════════╝")

    df_tonelagem           = fase1_extrair_pdfs()
    df_cruzado, cols_orig  = fase2_cruzamento(df_tonelagem)
    df_com_fat             = fase2_5_faturamento(df_cruzado)
    df_emissoes            = fase3_calcular_emissoes(df_com_fat)
    fase4_exportar(df_emissoes, cols_orig)

    elapsed = time.perf_counter() - t_inicio
    _log("╔══════════════════════════════════════════════════════════════════╗")
    _log("║  Pipeline concluído.                                            ║")
    _log(f"║  Tempo total: {elapsed:.1f}s                                          ║")
    _log(f"║  → {XLSX_FINAL:<56}║")
    _log("╚══════════════════════════════════════════════════════════════════╝")


if __name__ == "__main__":
    main()