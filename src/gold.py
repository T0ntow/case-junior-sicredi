
import logging

from pyspark.sql import functions as F

from src.utils import write_delta

logger = logging.getLogger(__name__)

SILVER_SCHEMA = "workspace.silver"
GOLD_SCHEMA = "workspace.gold"


def build_gold_transactions(
    df_transacoes,
    df_contas,
    df_clientes
):
    logger.info("Construindo visão analítica de transações PIX.")

    df_resultado = (
        df_transacoes.alias("t")
        .join(
            df_contas.alias("c"),
            F.col("t.id_conta").cast("string") == F.col("c.id_conta"),
            "inner"
        )
        .select(
            "t.*",
            F.col("c.tipo_conta"),
            F.col("c.status_conta"),
            F.col("c.id_cliente")
        )
        .alias("tc")
        .join(
            df_clientes.alias("cl"),
            F.col("tc.id_cliente") == F.col("cl.id_cliente"),
            "inner"
        )
        .select(
            F.col("tc.id_transacao"),
            F.col("tc.data_transacao"),
            F.col("tc.valor"),
            F.col("tc.tipo_movimento"),
            F.col("tc.descricao"),
            F.col("tc.id_conta"),
            F.col("tc.tipo_conta"),
            F.col("tc.status_conta"),
            F.col("tc.id_cliente"),
            F.col("cl.nome").alias("nome_cliente"),
            F.col("cl.cidade"),
            F.col("cl.estado")
        )
    )

    return df_resultado


def build_gold_client_summary(
    df_clientes,
    df_gold_transacoes
):
    logger.info("Construindo resumo financeiro por cliente.")

    df_resumo = (
        df_gold_transacoes
        .groupBy("id_cliente")
        .agg(
            F.sum("valor").alias("total_transacionado"),
            F.count("id_transacao").alias("quantidade_transacoes"),
            F.round(F.avg("valor"), 2).alias("ticket_medio"),
            F.max("data_transacao").alias("data_ultima_transacao")
        )
    )

    df_resultado = (
        df_clientes
        .select(
            "id_cliente",
            F.col("nome").alias("nome_cliente"),
            "cidade",
            "estado"
        )
        .join(df_resumo, on="id_cliente", how="left")
        .fillna({
            "total_transacionado": 0,
            "quantidade_transacoes": 0,
            "ticket_medio": 0
        })
    )

    return df_resultado


def run_gold(spark):
    logger.info("Iniciando pipeline Gold.")

    df_clientes = spark.table(
        f"{SILVER_SCHEMA}.silver_clientes"
    )

    df_contas = spark.table(
        f"{SILVER_SCHEMA}.silver_contas"
    )

    df_transacoes = spark.table(
        f"{SILVER_SCHEMA}.silver_transacoes_pix"
    )

    df_gold_transacoes = build_gold_transactions(
        df_transacoes,
        df_contas,
        df_clientes
    )

    df_gold_resumo = build_gold_client_summary(
        df_clientes,
        df_gold_transacoes
    )

    write_delta(
        df_gold_transacoes,
        f"{GOLD_SCHEMA}.gold_transacoes_pix",
        mode="overwrite"
    )

    write_delta(
        df_gold_resumo,
        f"{GOLD_SCHEMA}.gold_resumo_cliente",
        mode="overwrite"
    )

    logger.info("Pipeline Gold finalizada com sucesso.")

    return {
        "gold_transacoes_pix": df_gold_transacoes,
        "gold_resumo_cliente": df_gold_resumo
    }
