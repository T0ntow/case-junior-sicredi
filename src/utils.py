
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# LOGGING

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# LEITURA DE ARQUIVOS
# LEITURA DE ARQUIVOS
def read_csv(spark, path: str) -> DataFrame:
    """
    Lê um arquivo CSV preservando os valores como strings.

    Args:
        spark: Sessão Spark utilizada na leitura.
        path: Caminho completo do arquivo CSV.

    Returns:
        DataFrame com os dados originais.
    """

    try:
        logger.info("Iniciando leitura do arquivo: %s", path)

        df = (
            spark.read
            .format("csv")
            .option("header", True)
            .option("inferSchema", False)
            .option("mode", "PERMISSIVE")
            .load(path)
        )

        logger.info(
            "Leitura concluída. Registros: %s",
            df.count()
        )

        return df

    except Exception:
        logger.exception("Erro ao realizar leitura do arquivo: %s", path)
        raise


# METADADOS TÉCNICOS
def add_bronze_metadata(
    df: DataFrame,
    source_file: str
) -> DataFrame:
    """
    Adiciona metadados técnicos aos registros da Bronze.
    """

    return (
        df
        .withColumn("arquivo_origem", F.lit(source_file))
        .withColumn("data_ingestao", F.current_date())
        .withColumn(
            "timestamp_processamento",
            F.current_timestamp()
        )
    )


# ESCRITA DELTA
def write_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "overwrite"
) -> None:
    """
    Persiste um DataFrame em uma tabela Delta do Unity Catalog.

    Args:
        df: DataFrame que será persistido.
        table_name: Nome completo da tabela.
        mode: Modo de escrita.
    """

    try:
        logger.info("Iniciando escrita da tabela: %s", table_name)

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

        logger.info(
            "Tabela %s gravada com sucesso. Registros: %s",
            table_name,
            df.count()
        )

    except Exception:
        logger.exception("Erro ao gravar tabela: %s", table_name)
        raise
