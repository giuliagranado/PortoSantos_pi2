"""
API — Emissões Portuárias | Porto de Santos (2020-2025)
==========================================================
Expõe os dados gerados pelo pipeline ETL (etl_emissoes_trl.py)
do projeto PortoSantos_pi2 via endpoints REST.

Como rodar:
    pip install -r requirements.txt
    uvicorn main:app --reload

Depois acesse:
    http://localhost:8000/docs   -> documentação interativa (Swagger)
"""

import os
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query

# ──────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ──────────────────────────────────────────────────────────────

# Caminho para o arquivo gerado pelo ETL do projeto original.
# Copie o emissoes_trl_detalhado_join_2020_2025.xlsx para essa pasta,
# ou ajuste o caminho abaixo.
XLSX_PATH = "emissoes_trl_detalhado_join_2020_2025.xlsx"

app = FastAPI(
    title="API - Emissões Porto de Santos",
    description="Dados de emissões de navios no Porto de Santos, calculados via metodologia TRL.",
    version="1.0.0",
)


def carregar_dados() -> pd.DataFrame:
    """
    Carrega o Excel gerado pelo ETL. Se o arquivo não existir
    (ex: você ainda não rodou o etl_emissoes_trl.py ou não copiou
    o resultado pra cá), usa dados de exemplo para a API funcionar
    mesmo assim.
    """
    if os.path.exists(XLSX_PATH):
        return pd.read_excel(XLSX_PATH)

    # Dados fake apenas para demonstração/teste da API
    return pd.DataFrame([
        {"ANO": 2023, "TRL_CATEGORIA": "CARGA GERAL", "NOx_KG": 1200.5,
         "CO2_KG": 49230.0, "TOTAL_POLUENTES_KG": 53100.2, "FATURAMENTO_RMIL": 985000},
        {"ANO": 2023, "TRL_CATEGORIA": "GRANEL SOLIDO", "NOx_KG": 2400.1,
         "CO2_KG": 98500.0, "TOTAL_POLUENTES_KG": 106200.7, "FATURAMENTO_RMIL": 985000},
        {"ANO": 2024, "TRL_CATEGORIA": "GRANEL LIQUIDO", "NOx_KG": 3100.9,
         "CO2_KG": 127300.0, "TOTAL_POLUENTES_KG": 137800.3, "FATURAMENTO_RMIL": 1023000},
    ])


# ──────────────────────────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────────────────────────

@app.get("/")
def home():
    """Endpoint raiz — status da API."""
    fonte = "arquivo real" if os.path.exists(XLSX_PATH) else "dados de exemplo (mock)"
    return {
        "mensagem": "API de Emissões - Porto de Santos",
        "fonte_dados": fonte,
        "docs": "/docs",
    }


@app.get("/emissoes")
def listar_emissoes(
    ano: Optional[int] = Query(None, description="Filtrar por ano, ex: 2024"),
    categoria: Optional[str] = Query(
        None, description="Filtrar por categoria TRL, ex: GRANEL SOLIDO"
    ),
    limite: int = Query(100, le=1000, description="Máximo de registros retornados"),
):
    """Lista os registros de emissões, com filtros opcionais."""
    df = carregar_dados()

    if ano is not None:
        df = df[df["ANO"] == ano]
    if categoria is not None:
        df = df[df["TRL_CATEGORIA"].str.upper() == categoria.upper()]

    if df.empty:
        raise HTTPException(status_code=404, detail="Nenhum registro encontrado para esse filtro.")

    return df.head(limite).to_dict(orient="records")


@app.get("/emissoes/resumo")
def resumo_por_ano():
    """Retorna o total de poluentes emitidos por ano."""
    df = carregar_dados()
    resumo = (
        df.groupby("ANO")["TOTAL_POLUENTES_KG"]
        .sum()
        .round(2)
        .reset_index()
        .to_dict(orient="records")
    )
    return resumo


@app.get("/emissoes/{ano}")
def emissoes_por_ano(ano: int):
    """Retorna todos os registros de emissões de um ano específico."""
    df = carregar_dados()
    resultado = df[df["ANO"] == ano]

    if resultado.empty:
        raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado para o ano {ano}.")

    return resultado.to_dict(orient="records")


@app.get("/categorias")
def listar_categorias():
    """Lista as categorias TRL disponíveis nos dados."""
    df = carregar_dados()
    return sorted(df["TRL_CATEGORIA"].unique().tolist())
