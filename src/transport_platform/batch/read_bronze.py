from pyspark.sql import SparkSession

from transport_platform.core.config import BRONZE_PATH
from transport_platform.core.logger import logger


def run() -> None:
    """Read and show bronze records."""
    spark = SparkSession.builder.appName("ReadBronze").master("local[*]").getOrCreate()
    try:
        df = spark.read.parquet(BRONZE_PATH)
        df.show(truncate=False)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()