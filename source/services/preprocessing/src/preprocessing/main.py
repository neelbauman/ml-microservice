"""Preprocessing service — normalizes and extracts features from raw data."""

import structlog
from common.config import ServiceSettings
from common.dapr_helpers import publish, save_state
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import ProcessedData, SensorData
from fastapi import FastAPI, Request

settings = ServiceSettings(service_name="preprocessing")
setup_logging(settings.log_level)
logger = structlog.get_logger()

app = FastAPI(title="Preprocessing Service", version="0.1.0")
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)


def normalize(values: dict[str, float]) -> tuple[list[float], list[str]]:
    """Simple min-max style normalization for demo."""
    names = sorted(values.keys())
    raw = [values[k] for k in names]
    lo, hi = min(raw) if raw else 0, max(raw) if raw else 1
    span = hi - lo if hi != lo else 1.0
    features = [(v - lo) / span for v in raw]
    return features, names


# --- Dapr Subscription ---
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "raw-data", "route": "/process"},
    ]


@app.post("/process")
async def process(request: Request) -> dict:
    """Handle raw-data events from Dapr pub/sub."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    sensor = SensorData(**raw)

    features, names = normalize(sensor.values)
    processed = ProcessedData(
        sensor_id=sensor.sensor_id,
        timestamp=sensor.timestamp,
        features=features,
        feature_names=names,
        original_values=sensor.values,
    )

    await publish("preprocessed-data", processed.model_dump(mode="json"))
    await save_state(
        f"preprocessing:last:{sensor.sensor_id}",
        {"features": features, "feature_names": names},
    )

    await logger.ainfo("processed", sensor_id=sensor.sensor_id, n_features=len(features))
    return {"status": "ok"}
