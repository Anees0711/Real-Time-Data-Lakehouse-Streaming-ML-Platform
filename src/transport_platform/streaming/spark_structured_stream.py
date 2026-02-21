from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

from transport_platform.core.config import (
    BRONZE_PATH,
    CHECKPOINT_LOCATION,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
)
from transport_platform.core.logger import logger


def build_spark() -> SparkSession:
    """Create and configure SparkSession."""
    return (
        SparkSession.builder
        .appName("TransportStream")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .getOrCreate()
    )


def run() -> None:
    """Start streaming query."""
    logger.info("Streaming job starting...")

    spark = build_spark()

    schema = (
        StructType()
        .add("station", StringType())
        .add("delay_minutes", IntegerType())
        .add("timestamp", DoubleType())
    )

    try:
        raw = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        parsed = (
            raw.selectExpr("CAST(value AS STRING) AS json")
            .select(from_json(col("json"), schema).alias("data"))
            .select("data.*")
            .dropna()
            .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
        )

        query = (
            parsed.writeStream
            .format("parquet")
            .option("path", BRONZE_PATH)
            .option("checkpointLocation", CHECKPOINT_LOCATION)
            .outputMode("append")
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("Streaming query started. Writing to %s", BRONZE_PATH)
        query.awaitTermination()

    except Exception:
        logger.exception("Streaming job failed.")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()
