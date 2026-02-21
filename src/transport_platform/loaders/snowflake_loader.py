from __future__ import annotations

import os
import pyarrow.parquet as pq
import pandas as pd
import snowflake.connector

from transport_platform.core.config import load_config
from transport_platform.core.logger import setup_logging


def main() -> None:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    cfg = load_config()

    gold_local_path = os.getenv("GOLD_LOCAL_PARQUET", "").strip()
    if not gold_local_path:
        raise ValueError("Set GOLD_LOCAL_PARQUET to a parquet file path for this loader (beginner mode).")

    df = pq.read_table(gold_local_path).to_pandas()

    conn = snowflake.connector.connect(
        account=cfg.snowflake_account,
        user=cfg.snowflake_user,
        password=cfg.snowflake_password,
        warehouse=cfg.snowflake_warehouse,
        database=cfg.snowflake_database,
        schema=cfg.snowflake_schema,
        role=cfg.snowflake_role,
    )

    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gold_features (
              event_date STRING,
              event_hour STRING,
              route_id STRING,
              avg_delay_seconds FLOAT,
              max_delay_seconds FLOAT,
              tu_events INT,
              alerts_count INT
            )
            """
        )

        # Beginner-load: insert rows (OK for small sample). Production: COPY INTO from S3 stage.
        for _, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO gold_features
                (event_date, event_hour, route_id, avg_delay_seconds, max_delay_seconds, tu_events, alerts_count)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    str(row.get("event_date")),
                    str(row.get("event_hour")),
                    str(row.get("route_id")),
                    float(row.get("avg_delay_seconds") or 0),
                    float(row.get("max_delay_seconds") or 0),
                    int(row.get("tu_events") or 0),
                    int(row.get("alerts_count") or 0),
                ),
            )
    finally:
        conn.close()

    print(f"Inserted {len(df)} rows into Snowflake gold_features")


if __name__ == "__main__":
    main()
