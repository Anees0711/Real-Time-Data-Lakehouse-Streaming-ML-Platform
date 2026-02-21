from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

from transport_platform.core.config import GOLD_PATH, SILVER_PATH
from transport_platform.core.logger import logger


def run() -> None:
    """Build gold features."""
    logger.info("Gold aggregation job started...")

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
        df = spark.read.parquet(SILVER_PATH)

        gold = (
            df.groupBy("station")
            .agg(
                avg("delay_minutes").alias("avg_delay"),
                count("*").alias("num_events"),
            )
        )

        logger.info("Writing gold metrics to %s", GOLD_PATH)
        gold.write.mode("overwrite").parquet(GOLD_PATH)

        logger.info("Gold layer built successfully.")
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()
