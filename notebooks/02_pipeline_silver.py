# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Processamento da Camada Silver
# MAGIC
# MAGIC ## 1. Objetivo
# MAGIC
# MAGIC Tratar e validar os dados provenientes da camada Bronze, elevando a qualidade, a consistência e a confiabilidade das informações antes de disponibilizá-las para análises na camada Gold.
# MAGIC
# MAGIC ## 2. Estratégia de processamento
# MAGIC
# MAGIC O processamento é desenvolvido em PySpark, com regras implementadas em funções reutilizáveis no módulo `src/silver.py`.
# MAGIC
# MAGIC As principais operações são:
# MAGIC
# MAGIC - Padronização de campos textuais.
# MAGIC - Conversão e validação de tipos de dados.
# MAGIC - Validação matemática dos CPFs.
# MAGIC - Verificação de integridade referencial entre clientes, contas e transações.
# MAGIC - Deduplicação por chave de negócio.
# MAGIC - Separação dos registros inválidos em tabelas de quarentena, com o respectivo motivo de rejeição.
# MAGIC
# MAGIC ## 3. Estrutura de saída
# MAGIC
# MAGIC **Tabelas Silver**
# MAGIC - `silver_clientes`
# MAGIC - `silver_contas`
# MAGIC - `silver_transacoes_pix`
# MAGIC
# MAGIC **Tabelas de quarentena**
# MAGIC - `quarantine_clientes`
# MAGIC - `quarantine_contas`
# MAGIC - `quarantine_transacoes_pix`
# MAGIC
# MAGIC O processamento segue a dependência entre as entidades: clientes → contas → transações PIX.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tratamento de clientes
# MAGIC
# MAGIC Os registros de `bronze_clientes` passam por limpeza dos campos textuais, conversão da data de nascimento, validação de CPF e deduplicação por `id_cliente`.
# MAGIC
# MAGIC A validação do CPF considera o formato de 11 dígitos numéricos e o cálculo dos dígitos verificadores. Os registros são direcionados para:
# MAGIC
# MAGIC - **Silver:** clientes aprovados nas regras de validação, sem duplicidade de chave.
# MAGIC - **Quarentena:** registros rejeitados, acompanhados do motivo da rejeição.

# COMMAND ----------

import importlib
import src.silver

importlib.reload(src.silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1. Execução do processamento
# MAGIC
# MAGIC Execução das etapas de limpeza, validação, deduplicação e persistência das tabelas de clientes.

# COMMAND ----------

df_silver_clientes, df_quarantine_clientes = src.silver.process_clients(spark)

print("Clientes na Silver:", df_silver_clientes.count())
print("Clientes em quarentena:", df_quarantine_clientes.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2. Validação dos resultados
# MAGIC
# MAGIC Conferência dos volumes, verificação de duplicidades por `id_cliente` e análise dos motivos de rejeição.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver_clientes' AS tabela, COUNT(*) AS registros
# MAGIC FROM workspace.lakehouse.silver_clientes
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_clientes', COUNT(*)
# MAGIC FROM workspace.lakehouse.quarantine_clientes;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_cliente, COUNT(*) AS quantidade
# MAGIC FROM workspace.lakehouse.silver_clientes
# MAGIC GROUP BY id_cliente
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motivo_rejeicao, COUNT(*) AS quantidade
# MAGIC FROM workspace.lakehouse.quarantine_clientes
# MAGIC GROUP BY motivo_rejeicao
# MAGIC ORDER BY quantidade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tratamento de contas
# MAGIC
# MAGIC Os registros de `bronze_contas` passam pela padronização dos campos textuais e pela conversão da data de abertura.
# MAGIC
# MAGIC Também é verificada a integridade referencial com `silver_clientes`, garantindo que cada conta válida esteja associada a um cliente existente na camada Silver. Registros inválidos são encaminhados para quarentena com o motivo correspondente.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1. Execução do processamento
# MAGIC
# MAGIC Execução das etapas de limpeza, validação de integridade referencial, deduplicação e persistência das tabelas de contas.

# COMMAND ----------

df_silver_contas, df_quarantine_contas = src.silver.process_accounts(spark)

print("Contas na Silver:", df_silver_contas.count())
print("Contas em quarentena:", df_quarantine_contas.count())

display(df_silver_contas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2. Validação dos resultados
# MAGIC
# MAGIC Conferência dos volumes, verificação de duplicidades por `id_conta`, validação da integridade referencial e análise dos motivos de rejeição.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver_contas' AS tabela, COUNT(*) AS registros
# MAGIC FROM workspace.lakehouse.silver_contas
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_contas', COUNT(*)
# MAGIC FROM workspace.lakehouse.quarantine_contas;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_conta, COUNT(*) AS quantidade
# MAGIC FROM workspace.lakehouse.silver_contas
# MAGIC GROUP BY id_conta
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS clientes_nao_encontrados
# MAGIC FROM workspace.lakehouse.silver_contas a
# MAGIC LEFT JOIN workspace.lakehouse.silver_clientes c
# MAGIC     ON a.id_cliente = c.id_cliente
# MAGIC WHERE c.id_cliente IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT motivo_rejeicao, COUNT(*) AS quantidade
# MAGIC FROM workspace.lakehouse.quarantine_contas
# MAGIC GROUP BY motivo_rejeicao
# MAGIC ORDER BY quantidade DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tratamento de transações PIX
# MAGIC
# MAGIC Os registros de `bronze_transacoes_pix` passam por padronização dos campos textuais e conversão dos identificadores, datas e valores monetários.
# MAGIC
# MAGIC As regras de qualidade contemplam:
# MAGIC
# MAGIC - Validação dos campos obrigatórios (`id_transacao`, `id_conta`, `data_transacao` e `valor`).
# MAGIC - Verificação da existência da conta associada em `silver_contas`.
# MAGIC - Deduplicação por `id_transacao`.
# MAGIC - Encaminhamento dos registros inválidos para quarentena, com o motivo da rejeição.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1. Execução do processamento
# MAGIC
# MAGIC Execução integrada das etapas de limpeza, classificação, validação, deduplicação e persistência das tabelas de transações PIX.

# COMMAND ----------

df_silver_transacoes, df_quarantine_transacoes = src.silver.process_transactions(spark)

print("Transações na Silver:", df_silver_transacoes.count())
print("Transações em quarentena:", df_quarantine_transacoes.count())

display(df_silver_transacoes.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2. Análise dos registros em quarentena
# MAGIC
# MAGIC Agrupamento dos registros rejeitados por motivo para identificar as principais ocorrências de qualidade dos dados.

# COMMAND ----------

display(
    df_quarantine_transacoes
    .groupBy("motivo_rejeicao")
    .count()
    .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3. Validação da qualidade dos dados
# MAGIC
# MAGIC Verificação da ausência de duplicidades, de valores nulos nos campos obrigatórios e de referências a contas inexistentes na tabela Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC **Verificação de duplicidades por `id_transacao`.** O resultado esperado é não retornar registros.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_transacao, COUNT(*) AS quantidade
# MAGIC FROM workspace.lakehouse.silver_transacoes_pix
# MAGIC GROUP BY id_transacao
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC **Verificação de valores nulos nos campos obrigatórios.** O resultado esperado é zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS registros_invalidos
# MAGIC FROM workspace.lakehouse.silver_transacoes_pix
# MAGIC WHERE id_transacao IS NULL
# MAGIC    OR id_conta IS NULL
# MAGIC    OR data_transacao IS NULL
# MAGIC    OR valor IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC **Verificação da integridade referencial com `silver_contas`.** O resultado esperado é zero.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS contas_nao_encontradas
# MAGIC FROM workspace.lakehouse.silver_transacoes_pix t
# MAGIC LEFT JOIN workspace.lakehouse.silver_contas c
# MAGIC     ON t.id_conta = c.id_conta
# MAGIC WHERE c.id_conta IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Conferência geral dos volumes
# MAGIC
# MAGIC Comparação dos registros de origem (Bronze) com os registros aprovados (Silver) e rejeitados (quarentena). A diferença corresponde aos registros excedentes removidos durante a deduplicação, conforme as regras aplicadas no processamento.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'Clientes' AS entidade,
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_clientes) AS bronze,
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.silver_clientes) AS silver,
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_clientes) AS quarentena,
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_clientes)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.silver_clientes)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_clientes) AS duplicados_removidos
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Contas',
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_contas),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.silver_contas),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_contas),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_contas)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.silver_contas)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_contas)
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'Transações PIX',
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_transacoes_pix),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.silver_transacoes_pix),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_transacoes_pix),
# MAGIC     (SELECT COUNT(*) FROM workspace.lakehouse.bronze_transacoes_pix)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.silver_transacoes_pix)
# MAGIC       - (SELECT COUNT(*) FROM workspace.lakehouse.quarantine_transacoes_pix)
# MAGIC ORDER BY entidade;