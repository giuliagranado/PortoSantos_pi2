# Porto Santos - PI II

Projeto de Pesquisa desenvolvido na disciplina **Projeto Integrador II** do curso de Ciência de Dados da **Fatec Rubens Lara**.

O projeto analisa a **emissão de poluentes dos navios que atracam no Porto de Santos** entre 2020 e 2025, cruzando dados de atracação, tonelagem movimentada e faturamento do porto, aplicando a metodologia **TRL (Transport Research Laboratory)** — Hickman et al. (1999) — para estimar as emissões por embarcação.

## 📋 Sobre o projeto

O pipeline de ETL (`etl_emissoes_trl.py`) realiza as seguintes etapas:

1. **Extração de tonelagem (PDFs)** — lê os mensários do porto (`mensarios_pdf/`) e extrai a movimentação mensal por categoria de carga (carga geral, granel sólido e granel líquido).
2. **Cruzamento com CSV de atracações** — lê o(s) CSV(s) de atracação, filtra apenas navios de carga elegíveis e cruza com os dados de tonelagem extraídos dos PDFs.
3. **Join com faturamento** — incorpora o faturamento anual do porto (`Faturamento porto.csv`) para calcular indicadores de intensidade de emissão.
4. **Cálculo de emissões (TRL)** — aplica as equações do modelo TRL para estimar o consumo de combustível e as emissões de poluentes (NOx, CO, CO₂, VOC, PM, SOx) por navio.
5. **Exportação** — gera a planilha final consolidada `emissoes_trl_detalhado_join_2020_2025.xlsx`.

O pipeline foi desenhado com paralelismo (via `ThreadPoolExecutor`) nas etapas de leitura de PDFs e CSVs, já que são operações limitadas por I/O, e processamento vetorizado (numpy/pandas) na etapa de cálculo.

## 📁 Estrutura do repositório

```
PortoSantos_pi2/
├── data/                                              # Dados de entrada (CSVs de atracação)
├── mensarios_pdf/                                     # Mensários do porto em PDF
├── etl_emissoes_trl.py                                # Script principal do pipeline ETL
├── Faturamento porto.csv                              # Faturamento anual do porto
├── tonelagem_historica_pdf.xlsx                       # Cache de tonelagem extraída dos PDFs
├── emissoes_trl_detalhado_join_2020_2025.xlsx         # Resultado final do pipeline
├── gerar_codigo_etl_pi2_revisado.md                   # Notas/documentação de desenvolvimento
└── Emissão de poluição dos navios no Porto de Santos...pdf  # Relatório final da pesquisa
```

## 🛠️ Tecnologias utilizadas

- **Python 3**
- [pandas](https://pandas.pydata.org/) — manipulação de dados
- [numpy](https://numpy.org/) — cálculos vetorizados
- [pdfplumber](https://github.com/jsvine/pdfplumber) — extração de dados dos PDFs
- [openpyxl](https://openpyxl.readthedocs.io/) — exportação para Excel

## ▶️ Como executar

1. Clone o repositório:
   ```bash
   git clone https://github.com/giuliagranado/PortoSantos_pi2.git
   cd PortoSantos_pi2
   ```

2. Instale as dependências:
   ```bash
   pip install pandas numpy pdfplumber openpyxl
   ```

3. Verifique se os arquivos de entrada estão nos diretórios esperados (`data/`, `mensarios_pdf/`, `Faturamento porto.csv`).

4. Execute o pipeline:
   ```bash
   python etl_emissoes_trl.py
   ```

O resultado será salvo em `emissoes_trl_detalhado_join_2020_2025.xlsx`.

## 📚 Referências

- Hickman, J. et al. (1999) — *Methodology for calculating transport emissions and energy consumption*, Transport Research Laboratory (TRL).
- Bauer, G. (2023) — UNIMES.

