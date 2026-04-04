"""Inference service — runs anomaly detection on preprocessed data."""

import math
import os
import time as _time
import uuid
from contextlib import asynccontextmanager

import structlog
from common.config import ServiceSettings
from common.dapr_helpers import get_state, publish, save_state
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import Alert, AlertLevel, InferenceResult, ModelUpdateEvent, ProcessedData
from fastapi import FastAPI, Request
from inference.model_loader import ModelManager
from prometheus_client import Counter, Gauge, Histogram

settings = ServiceSettings(service_name="inference")
setup_logging(settings.log_level)
logger = structlog.get_logger()

MODEL_REGISTRY_NAME = os.getenv("MODEL_REGISTRY_NAME", "anomaly-detector")

# --- Model Manager (ONNX) ---
model_manager = ModelManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load the latest model from MLflow on startup."""
    loaded = await model_manager.try_load_latest(MODEL_REGISTRY_NAME)
    if loaded:
        await logger.ainfo("startup_model_loaded", model=model_manager.info)
    else:
        await logger.ainfo("startup_no_model", fallback="zscore")
    yield


app = FastAPI(title="Inference Service", version="0.1.0", lifespan=lifespan)
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)

# --- Prometheus Metrics ---
INFERENCE_COUNT = Counter(
    "inference_predictions_total",
    "Total number of inference predictions",
    ["model_version", "is_anomaly"],
)
INFERENCE_LATENCY = Histogram(
    "inference_latency_seconds",
    "Inference prediction latency in seconds",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
ANOMALY_SCORE = Histogram(
    "inference_anomaly_score",
    "Distribution of anomaly scores",
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0],
)
ANOMALY_TOTAL = Counter(
    "inference_anomalies_total",
    "Total number of anomalies detected",
    ["sensor_id", "level"],
)
MODEL_INFO = Gauge(
    "inference_model_loaded",
    "Whether a trained model is loaded (1) or using fallback (0)",
)

# --- Simple anomaly detection fallback (no trained model needed) ---
ANOMALY_THRESHOLD = 0.85


def detect_anomaly(features: list[float]) -> tuple[float, float, dict[str, float]]:
    """Z-score based anomaly detection. Returns (score, confidence, contributions)."""
    if not features:
        return 0.0, 0.0, {}
    mean = sum(features) / len(features)
    variance = sum((x - mean) ** 2 for x in features) / max(len(features), 1)
    std = math.sqrt(variance) if variance > 0 else 0.001
    z_scores = [(x - mean) / std for x in features]
    score = max(abs(z) for z in z_scores) / 3.0  # normalize to ~0-1
    score = min(score, 1.0)
    confidence = 1.0 - variance  # higher variance = lower confidence
    contributions = {f"f{i}": round(abs(z), 3) for i, z in enumerate(z_scores)}
    return round(score, 4), round(max(0.1, min(1.0, confidence)), 4), contributions


# --- Dapr Subscription ---
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "preprocessed-data", "route": "/predict"},
        {"pubsubname": "pubsub", "topic": "model-updates", "route": "/model-update"},
    ]


@app.post("/model-update")
async def model_update(request: Request) -> dict:
    """Handle model update notifications from Dapr pub/sub."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    event = ModelUpdateEvent(**raw)

    await logger.ainfo(
        "model_update_received",
        model_name=event.model_name,
        model_version=event.model_version,
        model_uri=event.model_uri,
    )

    try:
        await model_manager.load_model(
            model_name=event.model_name,
            model_version=event.model_version,
            model_uri=event.model_uri,
            input_dim=event.input_dim,
            anomaly_threshold=event.anomaly_threshold,
        )
        return {"status": "SUCCESS", "model_version": event.model_version}
    except Exception as e:
        await logger.aerror("model_load_failed", error=str(e))
        return {"status": "RETRY", "message": str(e)}


@app.post("/predict")
async def predict(request: Request) -> dict:
    """Run inference on preprocessed data."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    data = ProcessedData(**raw)

    # Use ONNX model if loaded, otherwise fall back to Z-score
    t0 = _time.perf_counter()
    if model_manager.is_loaded:
        score, confidence, contributions = model_manager.predict(data.features)
        model_version = model_manager.model_version
        is_anomaly = score > ANOMALY_THRESHOLD
        MODEL_INFO.set(1)
    else:
        score, confidence, contributions = detect_anomaly(data.features)
        model_version = "zscore-v1"
        is_anomaly = score > ANOMALY_THRESHOLD
        MODEL_INFO.set(0)
    INFERENCE_LATENCY.observe(_time.perf_counter() - t0)
    ANOMALY_SCORE.observe(score)
    INFERENCE_COUNT.labels(model_version=model_version, is_anomaly=str(is_anomaly)).inc()

    result = InferenceResult(
        sensor_id=data.sensor_id,
        timestamp=data.timestamp,
        prediction=score,
        confidence=confidence,
        is_anomaly=is_anomaly,
        model_version=model_version,
        feature_contributions=contributions,
    )

    # Publish result
    await publish("inference-results", result.model_dump(mode="json"))

    # Save latest result to state store
    await save_state(
        f"inference:last:{data.sensor_id}",
        result.model_dump(mode="json"),
    )

    # Trigger alert if anomaly
    if is_anomaly:
        level = AlertLevel.CRITICAL if score > 0.95 else AlertLevel.WARNING
        alert = Alert(
            alert_id=str(uuid.uuid4())[:8],
            sensor_id=data.sensor_id,
            timestamp=data.timestamp,
            level=level,
            message=f"Anomaly detected: score={score}, confidence={confidence}",
            inference_result=result,
        )
        await publish("alerts", alert.model_dump(mode="json"))
        ANOMALY_TOTAL.labels(sensor_id=data.sensor_id, level=level.value).inc()

    await logger.ainfo(
        "inference",
        sensor_id=data.sensor_id,
        score=score,
        is_anomaly=is_anomaly,
        model=model_version,
    )
    return {"status": "SUCCESS", "is_anomaly": is_anomaly}


@app.get("/results/{sensor_id}")
async def get_latest_result(sensor_id: str) -> dict:
    """Get the latest inference result for a sensor."""
    state = await get_state(f"inference:last:{sensor_id}")
    if state is None:
        return {"error": "not found"}
    return state


@app.get("/model/status")
async def model_status() -> dict:
    """Return current model status."""
    return model_manager.info
