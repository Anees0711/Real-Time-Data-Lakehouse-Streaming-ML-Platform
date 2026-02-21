import os

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transport-events")

BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "s3a://fr-transport-lakehouse/bronze/events_v2/",
)
SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "s3a://fr-transport-lakehouse/silver/events_v2/",
)
GOLD_PATH = os.getenv(
    "GOLD_PATH",
    "s3a://fr-transport-lakehouse/gold/station_metrics/",
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/opt/airflow/checkpoints/events_v2/",
)

