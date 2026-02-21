from pyspark.sql import SparkSession

from transport_platform.core.config import GOLD_PATH
from transport_platform.core.logger import logger


def run() -> None:
    """Read and show gold metrics."""
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
        df = spark.read.parquet(GOLD_PATH)
        df.show()
        df.printSchema()
        df.groupBy("station").count().show()
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()
