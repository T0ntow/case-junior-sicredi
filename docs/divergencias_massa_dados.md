# Relatório de divergências da massa de dados

**Projeto:** `case-junior-sicredi`\
**Objetivo:** registrar as inconsistências intencionais dos arquivos CSV
para validar posteriormente as camadas Bronze e Silver.

> Este documento é um inventário inicial. As divergências devem ser
> preservadas na massa de entrada e tratadas somente nas etapas
> correspondentes do pipeline. A Bronze deve manter os dados recebidos;
> a Silver deve aplicar as regras de qualidade documentadas.

## 1. Resumo da massa de dados

  Arquivo                  Registros esperados/gerados Observação
  ---------------------- ----------------------------- ----------------------------------------------
  `clientes.csv`                                    31 30 clientes distintos mais 1 linha duplicada
  `contas.csv`                                      35 35 contas
  `transacoes_pix.csv`                             200 200 transações

**Total:** 266 linhas de dados, sem contar os cabeçalhos.

## 2. `clientes.csv`

### 2.1 Duplicidade de chave

-   `id_cliente = 6` aparece duas vezes.
-   A segunda ocorrência está no final do arquivo.
-   Os dados cadastrais e o CPF são iguais; a segunda linha contém
    espaços extras no nome.
-   Regra a validar na Silver: manter somente um registro por
    `id_cliente`, usando critério determinístico caso existam versões
    divergentes.

### 2.2 CPFs fora do padrão de tamanho/conteúdo

    `id_cliente` Valor informado   Divergência
  -------------- ----------------- ---------------------------------
               3 `12345`           Menos de 11 dígitos
               4 `ABC12345678`     Contém caracteres não numéricos
               5 `123456789012`    Mais de 11 dígitos

Observação: os demais CPFs foram gerados com 11 dígitos numéricos, mas
são fictícios e não devem ser tratados como documentos reais. O
requisito do desafio pede validação de tamanho e presença de 11 dígitos
numéricos; não exige validação dos dígitos verificadores.

### 2.3 Espaços extras em nomes

Foram introduzidos espaços extras intencionalmente:

-   `id_cliente = 1`: espaços no início e no fim de `nome`.
-   `id_cliente = 2`: espaços repetidos entre partes do nome e espaço ao
    final.
-   A segunda ocorrência de `id_cliente = 6` também possui espaços no
    início e no fim.

Regra a validar na Silver: aplicar `trim` e, se adotado como regra de
padronização, normalizar sequências internas de espaços.

## 3. `contas.csv`

### 3.1 Espaços e variação de caixa

  ------------------------------------------------------------------------
             `id_conta` Campo            Valor de entrada Tratamento
                                                          esperado
  --------------------- ---------------- ---------------- ----------------
                      1 `tipo_conta`     `corrente`       Remover espaços
                                                          e converter para
                                                          maiúsculas

                      2 `status_conta`   `ativa`          Remover espaços
                                                          e converter para
                                                          maiúsculas
  ------------------------------------------------------------------------

Regra a validar na Silver: padronizar strings com `trim` e caixa alta.

### 3.2 Relacionamentos

-   Existem 35 contas para 30 clientes distintos.
-   Os clientes de IDs 1 a 5 possuem duas contas cada: uma entre as
    contas 1--5 e outra entre as contas 31--35.
-   Esse relacionamento de um cliente para várias contas é intencional e
    deve ser preservado.
-   Há contas com status `ATIVA` e `ENCERRADA`.
-   Contas encerradas não devem ser descartadas automaticamente: podem
    possuir transações históricas.

### 3.3 Integridade referencial

Pela massa apresentada, os `id_cliente` das contas estão entre 1 e 30,
correspondendo aos clientes cadastrados. A validação formal desse
relacionamento deverá ser executada no pipeline.

## 4. `transacoes_pix.csv`

Validações realizadas sobre o arquivo recebido, com 200 linhas e 6
colunas.

### 4.1 IDs de transação duplicados

Os seguintes IDs aparecem duas vezes:

    `id_transacao`   Ocorrências
  ---------------- -------------
                 1             2
                 2             2
                 3             2
                 4             2

-   São 4 chaves duplicadas.
-   São 8 linhas envolvidas nessas duplicidades.
-   Isso representa 4 ocorrências excedentes em relação a uma linha por
    chave.

Regra a validar na Silver: deduplicar por `id_transacao` com critério
determinístico. Não depender de `dropDuplicates` sem definir qual
registro deve prevalecer.

### 4.2 Valores nulos

    `id_transacao` Valor
  ---------------- -------
                11 vazio
                21 vazio

Regra a validar na Silver: converter `valor` para tipo decimal e
encaminhar registros sem valor para quarentena, registrando o motivo.

### 4.3 Datas inválidas

    `id_transacao` `data_transacao`
  ---------------- ------------------
                31 `2026-15-45`
                41 `data_invalida`

Regra a validar na Silver: converter a data para o tipo apropriado e
encaminhar valores que não puderem ser convertidos para quarentena.

### 4.4 Conta inexistente

-   `id_transacao = 200` referencia `id_conta = 9999`.
-   O arquivo `contas.csv` contém contas com IDs de 1 a 35; portanto, a
    conta 9999 não está cadastrada.

Regra a validar na Silver: verificar a integridade referencial com
`contas` e encaminhar a transação sem conta correspondente para
quarentena.

### 4.5 Espaços extras em descrição

-   `id_transacao = 1` possui espaços no início e no fim de `descricao`.

Regra a validar na Silver: aplicar `trim` nas colunas textuais
pertinentes.

### 4.6 Distribuição por tipo de movimento

  `tipo_movimento`     Quantidade
  ------------------ ------------
  `RECEBIMENTO`               112
  `ENVIO`                      88
  **Total**               **200**

Essa distribuição é informativa e não representa, por si só, uma
divergência.

## 5. Regras de tratamento propostas para a Silver

  -----------------------------------------------------------------------
  Entidade                Condição                Ação proposta
  ----------------------- ----------------------- -----------------------
  Clientes                `id_cliente` duplicado  Deduplicar com critério
                                                  determinístico

  Clientes                CPF diferente de 11     Quarentena
                          dígitos numéricos       

  Clientes                Espaços extras em       Padronizar
                          strings                 

  Contas                  Espaços/caixa           Padronizar
                          inconsistentes          

  Contas                  `id_cliente` sem        Quarentena, caso
                          correspondência         encontrado na validação

  Transações              `id_transacao`          Deduplicar com critério
                          duplicado               determinístico

  Transações              `valor` nulo ou não     Quarentena
                          conversível             

  Transações              Data inválida           Quarentena

  Transações              `id_conta` sem          Quarentena
                          correspondência         

  Transações              Espaços extras em       Padronizar
                          strings                 
  -----------------------------------------------------------------------

### Observações importantes

1.  A camada Bronze deve preservar os registros recebidos, incluindo os
    inválidos, e adicionar os metadados técnicos exigidos
    (`arquivo_origem`, `data_ingestao` e `timestamp_processamento`).
2.  A quarentena deve registrar o motivo da rejeição. Se um registro
    apresentar mais de um problema, definir se será registrado um motivo
    principal ou uma lista de motivos.
3.  A ordem das validações e da deduplicação precisa ser documentada. Em
    particular, deve-se decidir como tratar duplicatas quando uma das
    ocorrências é inválida.
4.  Não remover contas encerradas apenas pelo status: transações
    históricas podem estar associadas a elas.
5.  Os números deste documento descrevem a massa atual e devem ser
    confirmados por testes automatizados na execução do pipeline.

## 6. Checklist de validação futura

-   [ ] Bronze contém todas as linhas de origem dos três CSVs.
-   [ ] Bronze possui os três metadados técnicos exigidos.
-   [ ] Silver padroniza espaços e caixa das strings.
-   [ ] Silver converte datas e valores para tipos adequados.
-   [ ] CPFs são validados por 11 dígitos numéricos.
-   [ ] Duplicidades são tratadas com regra determinística.
-   [ ] Registros inválidos são separados em quarentena.
-   [ ] Quarentena informa o motivo de cada rejeição.
-   [ ] Integridade referencial entre clientes, contas e transações é
    validada.
-   [ ] Gold utiliza somente dados tratados e relacionamentos válidos.
-   [ ] Reexecuções não criam duplicações indevidas.

------------------------------------------------------------------------

**Status:** inventário inicial registrado; regras ainda precisam ser
implementadas e comprovadas por execução.
