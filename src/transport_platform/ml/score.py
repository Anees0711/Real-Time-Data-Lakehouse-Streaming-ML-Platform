from __future__ import annotations

import os
import joblib
import pandas as pd

from transport_platform.core.logger import setup_logging


def main() -> None:
    setup_logging(os.getenv("LOG_LEVEL", "INFO"))

    model_path = "artifacts/model.joblib"
    if not os.path.exists(model_path):
        raise FileNotFoundError("Train model first: python -m platform.ml.train")

    csv_path = os.getenv("GOLD_FEATURES_CSV", "").strip()
    if not csv_path:
        raise ValueError("Set GOLD_FEATURES_CSV to score.")

    df = pd.read_csv(csv_path).fillna(0)

    X = df.drop(columns=["avg_delay_seconds"], errors="ignore")
    model = joblib.load(model_path)
    preds = model.predict(X)

    out = df.copy()
    out["predicted_avg_delay"] = preds
    os.makedirs("outputs", exist_ok=True)
    out.to_csv("outputs/scored_features.csv", index=False)

    print("Saved scored features to outputs/scored_features.csv")


if __name__ == "__main__":
    main()
