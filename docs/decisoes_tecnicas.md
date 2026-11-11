# Decisões Técnicas do Projeto

Durante o desenvolvimento do pipeline de dados, foram tomadas algumas decisões visando garantir organização, qualidade, rastreabilidade e facilidade de manutenção.

### 1. Arquitetura Medallion

Foi adotada a arquitetura Medallion, dividida em três camadas:

- **Bronze:** armazenamento dos dados originais, preservando os valores recebidos e adicionando metadados de ingestão.
- **Silver:** aplicação das regras de limpeza, padronização, conversão de tipos, validação de CPF, remoção de duplicidades e verificação de integridade referencial.
- **Gold:** disponibilização de dados consolidados e enriquecidos, preparados para consultas analíticas.

Essa separação facilita a identificação de problemas, a manutenção e a evolução do pipeline.

### 2. Utilização do formato Delta Lake

Foi utilizado o formato Delta Lake para armazenar as tabelas, aproveitando recursos como transações ACID, consistência dos dados e integração com o ambiente Databricks.

### 3. Preservação dos dados na camada Bronze

Os dados foram mantidos em sua representação original na Bronze, sem aplicação de regras de negócio ou transformações de limpeza. Foram adicionadas informações de rastreabilidade, como arquivo de origem e data de ingestão.

### 4. Tratamento de dados inválidos e duplicados

Na Silver, foram implementadas validações para identificar registros inválidos, além de regras de deduplicação baseadas nas chaves de cada entidade.

Os registros que não atenderam aos critérios de qualidade foram direcionados para tabelas de quarentena, permitindo preservar essas informações para análise, em vez de descartá-las.

### 5. Integridade referencial

Foi considerada a relação entre clientes, contas e transações. Contas associadas a clientes inválidos e transações sem contas válidas foram direcionadas para quarentena.

Contas encerradas foram mantidas, pois podem possuir transações históricas que ainda precisam ser consideradas nas análises.

### 6. Construção das tabelas Gold

Foram criadas duas tabelas analíticas:

- `gold_transacoes_pix`: reúne as transações enriquecidas com informações das contas e dos clientes.
- `gold_resumo_cliente`: apresenta indicadores financeiros por cliente, como valor total movimentado, quantidade de transações, ticket médio e data da última transação.

O indicador de valor total transacionado considera a soma dos valores de envio e recebimento, representando a movimentação financeira bruta do cliente, e não seu saldo líquido.

### 7. Organização e reutilização do código

As regras de processamento foram separadas em módulos Python dentro de `src/`, enquanto os notebooks ficaram responsáveis pela execução e validação das etapas. Essa abordagem evita a concentração de toda a lógica nos notebooks e facilita a reutilização e manutenção do código.

### 8. Orquestração

Foi utilizado o Databricks Workflows para executar as etapas do pipeline em sequência, respeitando as dependências entre Bronze, Silver e Gold. As consultas analíticas foram mantidas em um notebook separado, por representarem a etapa de consumo dos dados.