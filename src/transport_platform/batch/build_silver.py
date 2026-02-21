from pyspark.sql import SparkSession

from transport_platform.core.config import BRONZE_PATH, SILVER_PATH
from transport_platform.core.logger import logger


def run() -> None:
    """Build the silver layer."""
    spark = (
    SparkSession.builder
    .appName("BuildSilver")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .getOrCreate()
)

    try:
        logger.info("Reading bronze from %s", BRONZE_PATH)
        bronze = spark.read.parquet(BRONZE_PATH)

        silver = bronze.dropDuplicates()

        logger.info("Writing silver to %s", SILVER_PATH)
        silver.write.mode("overwrite").parquet(SILVER_PATH)

        logger.info("Silver layer built successfully.")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()
