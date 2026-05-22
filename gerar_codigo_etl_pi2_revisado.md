# ETL Pipeline — Emissões Portuárias TRL (2005–2025)
## Prompt Revisado · Arquiteto de Dados Sênior · Python/Pandas/pdfplumber

Escreva um pipeline ETL completo para calcular estimativas de emissão por navio (row-by-row)
usando a Metodologia TRL (Hickman, 1999 — replicada por Gerson Bauer, UNIMES, 2023),
SEM PERDER NENHUMA LINHA OU COLUNA do dataset original.

---

### ⚠️ REGRAS GLOBAIS

1. **Preservação total**: o DataFrame final DEVE conter todas as colunas e linhas do CSV original.
   Linhas sem categoria TRL válida (passageiros, outros, sem mercadoria) recebem `NaN`
   nas colunas de emissão — jamais são removidas.
2. **Eficiência de memória**: o arquivo histórico completo tem ~170 MB.
   Use `dtype` otimizado (`category` para strings repetitivas, `int32`/`float32` onde possível)
   na leitura inicial. Se necessário, use `chunksize` para montar o groupby auxiliar antes de
   ler o CSV inteiro em memória.
3. **Sem deleção de intermediários**: não use `os.remove()` ou equivalente em nenhum
   arquivo gerado pelo pipeline.

---

### 📊 CONTEXTO DOS DADOS

#### Input 1 — PDFs dos Mensários Estatísticos
- **Pasta**: `./mensarios_pdf/`
- **Página alvo**: "EVOLUÇÃO DA MOVIMENTAÇÃO MENSAL" — identificada pelo marcador `FL.07`
  no texto da página (não FL.08, que é só o Porto Organizado).
- **Estrutura real da tabela** (confirmada no mensário DEZ/2025):
  - Cada **linha** = um mês (JANEIRO a DEZEMBRO)
  - Cada linha tem **12 valores numéricos** em sequência fixa:
    ```
    [0] CG Longo Curso  [1] CG Cabotagem  [2] CG SOMA  ← índice 2
    [3] SG Longo Curso  [4] SG Cabotagem  [5] SG SOMA  ← índice 5
    [6] LG Longo Curso  [7] LG Cabotagem  [8] LG SOMA  ← índice 8
    [9] TT Longo Curso [10] TT Cabotagem [11] TT SOMA
    ```
  - Há duas seções: `IMPORTAÇÃO` e `EXPORTAÇÃO` (detectar por palavra-chave < 25 chars)
  - A linha `T O T A L` (com espaços) é o total anual — **ignorar**
- **Extração**: para cada mês, somar `SOMA_IMP + SOMA_EXP` nos índices 2, 5 e 8
- **Output do PDF**: DataFrame com `ANO_MES` (formato `'YYYY-MM'`), `TRL_CATEGORIA` e
  `NET_TONNAGE_MES` (inteiro). Persistir em `tonelagem_historica_pdf.xlsx` (não reprocessar
  se já existir).

#### Input 2 — CSV Principal de Atracações
- **Arquivo**: `emissoes_portuarias_2025_tratado.csv` (arquivo de 2025; para o histórico
  completo usar `atracacoes_historico.csv`)
- **Colunas presentes** (não alterar, não remover):
  `QUANTIDADE_ATRACACAO`, `MERCADORIA`, `ANO`, `PERFIL_EMBARCACAO`, `NATUREZA_CARGA`,
  `MES`, `DESATRACACAO`, `ATRACACAO`, `NUMERO_VIAGEM`, `TIPO_VIAGEM`, `ANO_MES`
- **`ANO_MES` no CSV está no formato `'YYYY-MM-DD'`** (ex.: `'2025-01-01'`).
  Antes de qualquer join, normalizar para `'YYYY-MM'` em uma coluna auxiliar `ANO_MES_NORM`.
- **`NATUREZA_CARGA`** é a coluna de classificação de carga. Seus valores devem ser mapeados
  para as 3 categorias TRL do PDF via coluna auxiliar `TRL_CATEGORIA`:

  | NATUREZA_CARGA no CSV       | TRL_CATEGORIA   |
  |-----------------------------|-----------------|
  | CARGA GERAL                 | CARGA GERAL     |
  | CARGA CONTEINERIZADA        | CARGA GERAL     |
  | ROLL-ON/ROLL-OFF            | CARGA GERAL     |
  | GRANEL SOLIDO               | GRANEL SOLIDO   |
  | GRANEL LIQUIDO              | GRANEL LIQUIDO  |
  | PASSAGEIROS                 | `NaN`           |
  | OUTROS / SEM MERCADORIA / VERIFICAR | `NaN`   |

  > **Justificativa**: o PDF FL.07 agrupa Conteinerizada e Carga Solta sob "Carga Geral"
  > (confirmado em FL.24). Roll-on/Roll-off integra o mesmo grupo no cálculo TRL
  > (Bauer, 2023, seção 4.1). Passageiros são **explicitamente excluídos** do cálculo
  > pelo próprio Bauer (p. 58, seção 4.1) — mas as linhas devem ser **preservadas** no CSV.

---

### ⚙️ ESPECIFICAÇÕES DO PIPELINE

#### FASE 1 — Extração dos PDFs
Função `fase1_extrair_pdfs()`:
- Verifica se `tonelagem_historica_pdf.xlsx` existe → carrega sem reprocessar
- Itera sobre `*.pdf` em `./mensarios_pdf/`, extrai o ano do nome do arquivo
- Localiza a página FL.07 por marcador no texto (fallback: busca por texto descritivo
  excluindo "ORGANIZADO"; segundo fallback: posição 7)
- Detecta blocos IMP/EXP por palavra-chave com comprimento < 25 caracteres
- Ignora a linha `T O T A L` (remover espaços antes de comparar)
- Identifica meses pela primeira palavra da linha (normalizar: remover acentos, uppercase)
- Extrai valores numéricos dos tokens após o nome do mês; usa índices 2, 5, 8
- Soma IMP + EXP por (mês, categoria); gera `ANO_MES` no formato `'YYYY-MM'`
- Output: `ANO_MES | TRL_CATEGORIA | NET_TONNAGE_MES`

#### FASE 2 — Leitura do CSV e Mapa de Distribuição
Função `fase2_cruzamento(df_tonelagem)`:
1. Ler CSV com `dtype` otimizado (`category` para strings, `low_memory=False`)
2. Criar `ANO_MES_NORM` = primeiros 7 chars de `ANO_MES` → formato `'YYYY-MM'`
3. Criar `TRL_CATEGORIA` via mapeamento da tabela acima
4. Criar DataFrame auxiliar `df_contagem`: agrupar CSV por `(ANO_MES_NORM, TRL_CATEGORIA)`,
   contar linhas → coluna `QTD_NAVIOS_MES`. **Incluir apenas linhas com TRL_CATEGORIA não-nula**
   (passageiros não têm tonelagem no PDF e não devem diluir o denominador).
5. Merge 1: `df_tonelagem` LEFT JOIN `df_contagem` por `(ANO_MES → ANO_MES_NORM, TRL_CATEGORIA)`
   → produz `df_ton_com_qtd`
6. Merge 2: CSV original LEFT JOIN `df_ton_com_qtd` por `(ANO_MES_NORM, TRL_CATEGORIA)`
   → o DataFrame resultante **contém todas as colunas originais** + `NET_TONNAGE_MES`,
   `QTD_NAVIOS_MES`, `TRL_CATEGORIA`, `ANO_MES_NORM`

#### FASE 3 — Cálculo Individual por Navio (Vetorizado)
Função `fase3_calcular_emissoes(df)`:

**A. Tonelagem por navio**
```
NET_TONNAGE_NAVIO  = NET_TONNAGE_MES / QTD_NAVIOS_MES
GROSS_TONNAGE_NAVIO = NET_TONNAGE_NAVIO / 0.80
```
> `NET_TONNAGE_MES` é a tonelagem de carga movimentada do mensário, usada como proxy
> para NRT (arqueação líquida), replicando a simplificação de Bauer (2023, p. 56–57).

**B. Consumo na Potência Máxima (t/dia)** — via `np.select` sobre `TRL_CATEGORIA`:
```
GRANEL SOLIDO : CONSUMO_MAX = 20.186 + (0.00049 × GROSS_TONNAGE_NAVIO)
GRANEL LIQUIDO: CONSUMO_MAX = 14.685 + (0.00079 × GROSS_TONNAGE_NAVIO)
default       : CONSUMO_MAX =  9.8197 + (0.00143 × GROSS_TONNAGE_NAVIO)
```
(default cobre CARGA GERAL e qualquer categoria não mapeada)

**C. Consumo Efetivo na Manobra (3 horas)**
```
CONSUMO_EFETIVO_NAVIO = CONSUMO_MAX × 0.4 × 0.125
```
- Fator operacional: 0.4 (40 % da potência máxima em manobra)
- Duração: 3 h = 3/24 = 0.125 dias
- Trecho: fundeadouro → atracação → desatracação → saída do canal (Bauer, 2023, p. 57)

**D. Emissões por poluente (kg)** — multiplicar `CONSUMO_EFETIVO_NAVIO` pelos fatores
de emissão TRL para motores de baixa rotação (2-stroke low-speed):
```
NOx_KG  = CONSUMO_EFETIVO_NAVIO × 78
CO_KG   = CONSUMO_EFETIVO_NAVIO × 28
CO2_KG  = CONSUMO_EFETIVO_NAVIO × 3200
VOC_KG  = CONSUMO_EFETIVO_NAVIO × 3.6
PM_KG   = CONSUMO_EFETIVO_NAVIO × 1.2
SOx_KG  = CONSUMO_EFETIVO_NAVIO × 10   ← equivale a 20 g/kg × 0.5 (teor S% MARPOL)
```
Linhas sem `TRL_CATEGORIA` válida recebem automaticamente `NaN` em todas as colunas
de emissão (propagação natural de NaN nas operações aritméticas).

#### FASE 4 — Exportação Final
Função `fase4_exportar(df)`:
- Remover as colunas auxiliares de cálculo:
  `NET_TONNAGE_MES`, `QTD_NAVIOS_MES`, `NET_TONNAGE_NAVIO`, `GROSS_TONNAGE_NAVIO`,
  `CONSUMO_MAX`, `CONSUMO_EFETIVO_NAVIO`, `ANO_MES_NORM`, `TRL_CATEGORIA`
- Manter: todas as colunas originais do CSV + `NOx_KG`, `CO_KG`, `CO2_KG`, `VOC_KG`,
  `PM_KG`, `SOx_KG`
- Salvar como `emissoes_trl_detalhado_2005_2025.csv` (sem index, encoding `utf-8-sig`)
- Imprimir: total de linhas, contagem de linhas com emissão calculada, sumário por TRL_CATEGORIA

---

### 📌 REFERÊNCIAS METODOLÓGICAS
- Hickman, J. et al. (1999). *Methodology for calculating transport emissions and energy
  consumption — Part C: Ship Transport*. Transport Research Laboratory (TRL).
- Bauer, G. (2023). *A poluição do ar gerada pelos navios e a ocorrência de eventos de
  saúde relacionados a doenças respiratórias no município de Santos*. Dissertação de
  Mestrado Profissional, UNIMES. Seções 4.1 e Tabela 4.
