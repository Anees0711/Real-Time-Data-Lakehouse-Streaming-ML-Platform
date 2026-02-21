import joblib
import json
from datetime import datetime
import os
import snowflake.connector

from .dataset import load_dataset
from .features import build_features
from .model import train_model

# Create folders safely
os.makedirs("src/transport_platform/ml/artifacts", exist_ok=True)
os.makedirs("src/transport_platform/ml/metrics", exist_ok=True)


def get_connection():
    return snowflake.connector.connect(
        user="anees0711",
        password="Revenged@12345",
        account="YDQTCNC-IW80910",
        warehouse="COMPUTE_WH",
        database="FR_TRANSPORT",
        schema="PUBLIC",
    )


def run():
    print("Training pipeline started...")

    df = load_dataset()
    X, y = build_features(df)

    model, mae = train_model(X, y)

    # Save model artifact
    model_path = "src/transport_platform/ml/artifacts/model.joblib"
    joblib.dump(model, model_path)

    # Save metrics JSON
    metrics = {
        "mae": float(mae),
        "timestamp": datetime.utcnow().isoformat()
    }

    metrics_path = "src/transport_platform/ml/metrics/metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)

    print("Model saved:", model_path)
    print("Metrics saved:", metrics_path)

    
    conn = get_connection()
    rows_seen = len(df)

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO MODEL_RUNS
        (run_ts, model_name, source_table, rows_seen, mae, artifact_path)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        datetime.utcnow(),
        "station_delay_model",
        "STATION_METRICS_GOLD",
        rows_seen,
        float(mae),
        model_path
    ))

    cur.close()
    conn.close()

    print("Training metadata logged to Snowflake")


if __name__ == "__main__":
    run()
