# Relatório de divergências da massa de dados

**Objetivo:** documentar as inconsistências intencionais presentes nos CSVs atuais e confrontá-las com os resultados observados nas camadas Bronze e Silver.

## 1. Resumo da massa atual

| Arquivo | Linhas de dados | Observação |
|---|---:|---|
| `clientes.csv` | 31 | 30 IDs distintos e 1 linha excedente por duplicidade |
| `contas.csv` | 35 | 35 IDs de conta distintos |
| `transacoes_pix.csv` | 200 | 4 IDs de transação duplicados, cada um com 2 ocorrências |

**Total:** 266 linhas de dados, sem contar cabeçalhos.

### Resultados observados na pipeline

| Entidade | Bronze | Silver | Quarentena | Duplicados removidos |
|---|---:|---:|---:|---:|
| Clientes | 31 | 24 | 6 | 1 |
| Contas | 35 | 26 | 9 | 0 |
| Transações PIX | 200 | 147 | 50 | 3 |

Os números acima correspondem à execução atual informada no notebook. A contagem de duplicados removidos representa os registros excedentes removidos do conjunto tratado; não deve ser somada cegamente à quarentena sem considerar a ordem de classificação.

## 2. `clientes.csv`

### 2.1 Duplicidade

- `id_cliente = 6` aparece duas vezes.
- As duas linhas representam o mesmo cadastro, com o mesmo CPF e demais dados cadastrais.
- Uma ocorrência possui espaços extras no campo `nome`.
- Há 31 linhas no arquivo e 30 IDs distintos.
- Resultado observado: 24 registros na Silver, 6 na quarentena e 1 registro excedente removido por duplicidade.

### 2.2 CPFs inválidos

Há 3 CPFs fora do padrão de 11 dígitos numéricos:

| `id_cliente` | CPF informado | Divergência |
|---:|---|---|
| 3 | `12345` | Menos de 11 dígitos |
| 4 | `ABC12345678` | Contém caracteres não numéricos |
| 5 | `123456789012` | Mais de 11 dígitos |

Além desses, a validação matemática dos dígitos verificadores implementada no pipeline identificou mais 3 CPFs numericamente formatados, mas inválidos:

| `id_cliente` | CPF informado | Divergência |
|---:|---|---|
| 7 | `95376724256` | Dígitos verificadores inválidos |
| 14 | `15098393052` | Dígitos verificadores inválidos |
| 21 | `19136193977` | Dígitos verificadores inválidos |

**Total rejeitado pela regra atual de CPF: 6 clientes.**

Importante: o inventário inicial considerava apenas formato e quantidade de dígitos. A implementação atual é mais restritiva, pois também valida os dígitos verificadores. Essa diferença deve ser mantida explícita na documentação do desafio para justificar a regra adotada.

### 2.3 Espaços extras em nomes

Foram encontrados espaços excedentes em 3 linhas:

- `id_cliente = 1`: espaços no início e no fim do nome.
- `id_cliente = 2`: espaços repetidos entre partes do nome e espaço ao final.
- `id_cliente = 6`: uma das ocorrências duplicadas possui espaços no início e no fim.

Tratamento aplicado na Silver: remoção de espaços externos e normalização de espaços internos, conforme a implementação atual.

## 3. `contas.csv`

### 3.1 Espaços e variação de caixa

Foram identificadas duas ocorrências com espaços externos e caixa inconsistente:

| `id_conta` | Campo | Valor de entrada |
|---:|---|---|
| 1 | `tipo_conta` | `  corrente ` |
| 2 | `status_conta` | ` ativa ` |

Tratamento esperado/aplicado: `trim` e conversão para maiúsculas.

### 3.2 Relacionamentos e integridade referencial

- O arquivo contém 35 contas e 30 IDs distintos de clientes.
- Os clientes de IDs 1 a 5 possuem duas contas cada: uma entre as contas 1–5 e outra entre as contas 31–35.
- Não há IDs de conta duplicados.
- Todos os `id_cliente` presentes no CSV de contas existem no CSV de clientes.
- Entretanto, 9 contas estão associadas a clientes rejeitados na Silver por CPF inválido.

Contas associadas aos clientes rejeitados:

| `id_conta` | `id_cliente` |
|---:|---:|
| 3 | 3 |
| 4 | 4 |
| 5 | 5 |
| 7 | 7 |
| 14 | 14 |
| 21 | 21 |
| 33 | 3 |
| 34 | 4 |
| 35 | 5 |

Resultado observado: 26 contas na Silver e 9 em quarentena.

A ausência de correspondência no CSV de origem não é o problema: a rejeição ocorre porque o cliente relacionado não foi aprovado na Silver. Isso demonstra a propagação da regra de qualidade entre as entidades.

### 3.3 Status das contas

A massa contém contas `ATIVA` e `ENCERRADA`. Contas encerradas devem ser preservadas, pois podem possuir transações históricas. A Gold mantém o status da conta na visão detalhada, sem descartar transações históricas apenas por esse status.

## 4. `transacoes_pix.csv`

### 4.1 Duplicidade de IDs

Os IDs abaixo aparecem duas vezes cada:

| `id_transacao` | Ocorrências |
|---:|---:|
| 1 | 2 |
| 2 | 2 |
| 3 | 2 |
| 4 | 2 |

- São 4 chaves duplicadas e 8 linhas envolvidas.
- Existem 4 ocorrências excedentes em relação a uma linha por chave no arquivo bruto.
- Na execução atual, foram contabilizados 3 registros duplicados removidos do conjunto válido da Silver.

**Por que 3, e não 4?** As duas ocorrências do `id_transacao = 4` referenciam as contas 5 e 14. Ambas estão associadas a clientes rejeitados na Silver, portanto as duas ocorrências seguem para quarentena por integridade referencial. Os outros três grupos duplicados (IDs 1, 2 e 3) possuem registros válidos e resultam em 3 ocorrências excedentes removidas do conjunto Silver.

A regra de deduplicação deve ser determinística. Se a implementação atual utiliza `dropDuplicates`, vale revisar/documentar o critério de sobrevivência caso os registros duplicados possam apresentar atributos divergentes.

### 4.2 Valores inválidos

| `id_transacao` | Valor | Divergência |
|---:|---|---|
| 11 | vazio | Valor ausente |
| 21 | vazio | Valor ausente |

Resultado observado: 2 registros em quarentena por valor inválido.

### 4.3 Datas inválidas

| `id_transacao` | `data_transacao` | Divergência |
|---:|---|---|
| 31 | `2026-15-45` | Data impossível |
| 41 | `data_invalida` | Texto não conversível para data |

Resultado observado: 2 registros em quarentena por data inválida.

### 4.4 Contas sem correspondência na Silver

O arquivo possui uma transação que referencia uma conta inexistente na origem:

| `id_transacao` | `id_conta` |
|---:|---:|
| 200 | 9999 |

Além dela, transações que referenciam as 9 contas rejeitadas na Silver também não encontram correspondência em `silver_contas`.

Distribuição das transações associadas às contas rejeitadas ou inexistentes:

| `id_conta` | Quantidade de transações |
|---:|---:|
| 3 | 9 |
| 4 | 4 |
| 5 | 2 |
| 7 | 7 |
| 14 | 4 |
| 21 | 5 |
| 33 | 2 |
| 34 | 5 |
| 35 | 7 |
| 9999 | 1 |
| **Total** | **46** |

Resultado observado: 46 registros em quarentena por conta não encontrada na Silver. Desses, 45 referenciam contas existentes no CSV, mas rejeitadas devido à qualidade do cliente relacionado; 1 referencia a conta inexistente `9999`.

### 4.5 Espaços extras em descrição

- `id_transacao = 1` possui espaços no início e no fim de `descricao`.

Tratamento esperado/aplicado: `trim` nas colunas textuais pertinentes.

### 4.6 Distribuição por tipo de movimento

| `tipo_movimento` | Quantidade |
|---|---:|
| `RECEBIMENTO` | 110 |
| `ENVIO` | 90 |
| **Total** | **200** |

A distribuição atual é diferente da registrada no inventário anterior (112 recebimentos e 88 envios). O valor atualizado, calculado a partir do CSV atual, é **110 RECEBIMENTO e 90 ENVIO**.

## 5. Conciliação dos resultados da Silver

A quarentena de transações pode ser explicada pelos seguintes grupos, sem sobreposição observada entre eles:

| Motivo | Registros |
|---|---:|
| Conta não encontrada na Silver (inclui conta 9999) | 46 |
| Data inválida | 2 |
| Valor inválido | 2 |
| **Total em quarentena** | **50** |

Após a separação dos 50 registros inválidos, restam 150 registros elegíveis. A deduplicação remove 3 ocorrências excedentes, resultando em **147 transações na Silver**.

Para contas, as 35 linhas de Bronze são divididas em 26 contas válidas e 9 contas em quarentena.

Para clientes, as 31 linhas de Bronze resultam em 24 clientes válidos, 6 CPFs rejeitados e 1 ocorrência excedente removida por duplicidade. A interpretação exata depende da ordem implementada para deduplicação e classificação; a contagem observada deve ser mantida como resultado de execução e a ordem das regras documentada no código.

## 6. Regras e decisões que devem permanecer documentadas

1. A Bronze preserva todas as linhas recebidas e adiciona `arquivo_origem`, `data_ingestao` e `timestamp_processamento`.
2. A Silver padroniza strings, converte tipos, valida CPF, verifica integridade referencial, deduplica e direciona inválidos para quarentena.
3. A validação de CPF atual inclui dígitos verificadores, indo além do requisito mínimo de 11 dígitos numéricos descrito no inventário inicial.
4. A qualidade do cliente afeta a validade das contas relacionadas; a qualidade das contas afeta a validade das transações relacionadas.
5. Contas encerradas não são descartadas apenas pelo status.
6. Duplicidades devem ter critério determinístico de sobrevivência. No caso do ID 4, ambas as linhas são rejeitadas por conta inválida antes de compor a Silver.
7. A quarentena atual apresenta um motivo principal por registro. Se houver múltiplos problemas na mesma linha, a precedência das validações deve estar documentada.
8. A Gold utiliza as tabelas Silver como origem e mantém a visão histórica das transações.

## 7. Resultados observados na Gold

| Tabela | Registros | Colunas |
|---|---:|---:|
| `workspace.gold.gold_transacoes_pix` | 147 | 12 |
| `workspace.gold.gold_resumo_cliente` | 24 | 8 |

A tabela `gold_resumo_cliente` preserva os 24 clientes válidos. Na amostra atual, todos possuem pelo menos uma transação; portanto, a consulta de clientes sem transações retorna zero linhas. Isso não invalida o uso de `LEFT JOIN` na construção da Gold.

## 8. Checklist atualizado

- [x] Bronze contém 31 clientes, 35 contas e 200 transações.
- [x] Bronze adiciona os três metadados técnicos.
- [x] Silver padroniza strings.
- [x] Silver converte datas e valores.
- [x] Silver valida CPF, incluindo dígitos verificadores.
- [x] Silver trata duplicidades.
- [x] Silver separa registros inválidos em quarentena.
- [x] Silver valida integridade referencial entre clientes, contas e transações.
- [x] Gold utiliza dados tratados da Silver.
- [x] Gold gera visão detalhada e resumo por cliente.
- [x] Consultas SQL analíticas executadas.