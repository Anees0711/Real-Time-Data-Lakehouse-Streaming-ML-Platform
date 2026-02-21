from __future__ import annotations

import os
import pandas as pd
import pyarrow.parquet as pq
from google.cloud import bigquery

from transport_platform.core.config import load_config
from transport_platform.core.logger import setup_logging


def main() -> None:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))
    cfg = load_config()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cfg.bq_sa_json_path
    client = bigquery.Client(project=cfg.bq_project, location=cfg.bq_location)

    table_id = f"{cfg.bq_project}.{cfg.bq_dataset}.gold_features"

    # For beginner simplicity: read latest gold parquet locally or via downloaded file.
    # In production: use BigQuery load job from GCS (recommended).
    gold_local_path = os.getenv("GOLD_LOCAL_PARQUET", "").strip()
    if not gold_local_path:
        raise ValueError("Set GOLD_LOCAL_PARQUET to a parquet file path for this loader (beginner mode).")

    table = pq.read_table(gold_local_path)
    df = table.to_pandas()

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")


if __name__ == "__main__":
    main()
