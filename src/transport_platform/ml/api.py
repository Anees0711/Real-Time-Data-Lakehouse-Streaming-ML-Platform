from fastapi import FastAPI
import joblib
import numpy as np

app = FastAPI(title="Transport ML API")

model = joblib.load("src/transport_platform/ml/artifacts/model.joblib")


@app.get("/")
def root():
    return {"status": "API running"}


@app.post("/predict")
def predict(num_events: int):
    X = np.array([[num_events]])
    prediction = model.predict(X)[0]

    return {
        "num_events": num_events,
        "predicted_delay": float(prediction)
    }
