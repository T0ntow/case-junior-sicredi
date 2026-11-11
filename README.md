# Case Júnior - Pipeline Lakehouse Básico em Databricks

Pipeline de Engenharia de Dados desenvolvido no Databricks, utilizando
PySpark, Delta Lake e SQL, com arquitetura Medallion (Bronze, Silver e
Gold).

## 1. Visão geral

O projeto implementa um fluxo de ingestão, tratamento, validação e
disponibilização analítica de dados fictícios relacionados a clientes,
contas e transações PIX.

A solução está organizada em três camadas:

-   **Bronze:** ingestão dos arquivos CSV, preservando os dados de
    origem e adicionando metadados técnicos.
-   **Silver:** limpeza, padronização, conversão de tipos, validação de
    regras de qualidade, deduplicação e direcionamento de registros
    inválidos para quarentena.
-   **Gold:** construção de visões analíticas enriquecidas e indicadores
    consolidados por cliente.

As tabelas são armazenadas no formato Delta Lake e organizadas em
schemas separados no catálogo `workspace`.

## 2. Tecnologias utilizadas

-   Databricks
-   Apache Spark / PySpark
-   Delta Lake
-   SQL
-   Python
-   Git

## 3. Arquitetura

``` text
workspace
├── lakehouse
│   └── raw                 # Volume com os arquivos CSV de origem
├── bronze
│   ├── bronze_clientes
│   ├── bronze_contas
│   └── bronze_transacoes_pix
├── silver
│   ├── silver_clientes
│   ├── silver_contas
│   ├── silver_transacoes_pix
│   ├── quarantine_clientes
│   ├── quarantine_contas
│   └── quarantine_transacoes_pix
└── gold
    ├── gold_transacoes_pix
    └── gold_resumo_cliente
```

O Volume de entrada permanece em `workspace.lakehouse.raw`. Os schemas
`workspace.bronze`, `workspace.silver` e `workspace.gold` armazenam as
tabelas de cada camada.

### Fluxo de dados

``` text
Arquivos CSV
    |
    v
Volume workspace.lakehouse.raw
    |
    v
Bronze — ingestão e metadados
    |
    v
Silver — limpeza, validação e deduplicação
    |                         |
    |                         +--> Quarentena
    v
Gold — enriquecimento e agregações
    |
    v
Consultas analíticas SQL
```

## 4. Estrutura do repositório

``` text
case-junior-sicredi/
├── src/
│   ├── utils.py
│   ├── bronze.py
│   ├── silver.py
│   └── gold.py
├── notebooks/
│   ├── 00_validar_ambiente.ipynb
│   ├── 01_pipeline_bronze.ipynb
│   ├── 01_pipeline_silver.ipynb
│   ├── 02_pipeline_gold.ipynb
│   └── 03_consultas_analiticas.ipynb
└── README.md
```

Os módulos em `src/` concentram as funções reutilizáveis de
processamento. Os notebooks documentam e orquestram a execução, além de
apresentar as validações e consultas.

## 5. Dados de entrada

Os arquivos CSV utilizados no processamento devem estar no Volume:

``` text
/Volumes/workspace/lakehouse/raw/
```

Arquivos esperados:

-   `clientes.csv`
-   `contas.csv`
-   `transacoes_pix.csv`

Os dados são fictícios e utilizados exclusivamente para fins de
desenvolvimento e avaliação técnica.

## 6. Camadas do Lakehouse

### 6.1 Bronze

O módulo `src/bronze.py` realiza a leitura dos arquivos CSV e grava as
tabelas Delta no schema `workspace.bronze`.

Os dados de origem são preservados, com adição de metadados técnicos:

-   `arquivo_origem`
-   `data_ingestao`
-   `timestamp_processamento`

Tabelas geradas:

-   `workspace.bronze.bronze_clientes`
-   `workspace.bronze.bronze_contas`
-   `workspace.bronze.bronze_transacoes_pix`

### 6.2 Silver

O módulo `src/silver.py` aplica as regras de qualidade e padronização.

Principais tratamentos:

-   Normalização de campos textuais.
-   Conversão e validação de tipos.
-   Validação matemática dos dígitos verificadores do CPF.
-   Verificação de integridade referencial entre clientes, contas e
    transações.
-   Remoção de registros duplicados.
-   Separação de registros inválidos em tabelas de quarentena.

Tabelas geradas:

  -----------------------------------------------------------------------------------------------------------------
  Entidade                Tabela tratada                             Tabela de quarentena
  ----------------------- ------------------------------------------ ----------------------------------------------
  Clientes                `workspace.silver.silver_clientes`         `workspace.silver.quarantine_clientes`

  Contas                  `workspace.silver.silver_contas`           `workspace.silver.quarantine_contas`

  Transações PIX          `workspace.silver.silver_transacoes_pix`   `workspace.silver.quarantine_transacoes_pix`
  -----------------------------------------------------------------------------------------------------------------

### 6.3 Gold

O módulo `src/gold.py` constrói duas tabelas analíticas.

**`workspace.gold.gold_transacoes_pix`**

Visão detalhada das transações PIX, enriquecida com informações de
contas e clientes. Contém dados da transação, tipo e status da conta,
identificação do cliente, nome, cidade e estado.

**`workspace.gold.gold_resumo_cliente`**

Visão consolidada por cliente, com os indicadores:

-   `total_transacionado`: soma dos valores das transações.
-   `quantidade_transacoes`: quantidade de transações do cliente.
-   `ticket_medio`: média dos valores das transações, arredondada para
    duas casas decimais.
-   `data_ultima_transacao`: data mais recente de transação.

A construção do resumo parte da base de clientes e utiliza `LEFT JOIN`
com os indicadores de transação, preservando clientes sem movimentação.
Para esses clientes, os indicadores numéricos são preenchidos com zero e
a data da última transação permanece nula.

## 7. Execução

Execute os notebooks na seguinte ordem, em um ambiente Databricks com
acesso ao catálogo e ao Volume utilizados pelo projeto:

1.  `00_validar_ambiente.ipynb` --- valida o ambiente, os schemas e o
    Volume de entrada.
2.  `01_pipeline_bronze.ipynb` --- executa a ingestão dos arquivos CSV.
3.  `02_pipeline_silver.ipynb` --- executa os tratamentos, validações,
    deduplicação e quarentena.
4.  `03_pipeline_gold.ipynb` --- executa a construção e persistência das
    tabelas analíticas.
5.  `04_consultas_analiticas.ipynb` --- executa as consultas SQL sobre a
    camada Gold.

Os módulos Python devem estar disponíveis no caminho de importação do
repositório no Databricks.

## 8. Resultados da execução

Volumes observados na execução de referência:

  Entidade           Bronze   Silver   Quarentena   Duplicados removidos
  ---------------- -------- -------- ------------ ----------------------
  Clientes               31       24            6                      1
  Contas                 35       26            9                      0
  Transações PIX        200      147           50                      3

Resultados da camada Gold:

  Tabela                    Registros   Colunas
  ----------------------- ----------- ---------
  `gold_transacoes_pix`           147        12
  `gold_resumo_cliente`            24         8

Os números representam a execução de referência com os arquivos
sintéticos utilizados durante o desenvolvimento.

## 9. Consultas analíticas SQL

O notebook `03_consultas_analiticas.ipynb` contém cinco consultas:

1.  **Top 10 clientes por volume financeiro:** ordenação decrescente do
    total transacionado.
2.  **Movimentação financeira por dia:** quantidade e valor total de
    transações agrupados por data.
3.  **Ticket médio por estado:** comparação de quantidade, ticket médio
    e volume financeiro por estado.
4.  **Clientes sem transações:** identificação de clientes cujo
    indicador de quantidade é zero.
5.  **Ranking de cidades por volume financeiro:** agregação por cidade e
    estado com a Window Function `DENSE_RANK()`.

Na amostra de referência, a consulta de clientes sem transações não
retornou registros, pois todos os 24 clientes possuem pelo menos uma
transação. Esse resultado é esperado para os dados utilizados.

## 10. Observações

-   Os dados utilizados são fictícios.
-   Os schemas e caminhos apresentados correspondem ao ambiente
    Databricks utilizado durante o desenvolvimento.
-   A execução dos notebooks depende da disponibilidade dos arquivos CSV
    no Volume indicado.
-   As contagens apresentadas são resultados da amostra de referência e
    podem variar caso os arquivos de entrada sejam modificados.
