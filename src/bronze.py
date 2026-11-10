
import logging

from src.utils import (
    read_csv,
    add_bronze_metadata,
    write_delta
)

logger = logging.getLogger(__name__)

RAW_PATH = "/Volumes/workspace/lakehouse/raw"
TARGET_SCHEMA = "workspace.lakehouse"

FILES = {
    "clientes": "clientes.csv",
    "contas": "contas.csv",
    "transacoes_pix": "transacoes_pix.csv"
}


def ingest_file(spark, dataset_name, filename):
    source_path = f"{RAW_PATH}/{filename}"
    target_table = f"{TARGET_SCHEMA}.bronze_{dataset_name}"

    logger.info("Iniciando ingestão: %s", dataset_name)

    df = read_csv(spark, source_path)

    df_bronze = add_bronze_metadata(
        df,
        source_file=filename
    )

    write_delta(
        df_bronze,
        table_name=target_table,
        mode="overwrite"
    )

    logger.info("Ingestão concluída: %s", target_table)


def run_bronze(spark):
    logger.info("Iniciando pipeline Bronze.")

    for dataset_name, filename in FILES.items():
        ingest_file(spark, dataset_name, filename)

    logger.info("Pipeline Bronze finalizado com sucesso.")
