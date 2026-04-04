"""Ingestion service — receives sensor data and publishes to Dapr pub/sub."""


import structlog
from common.config import ServiceSettings
from common.dapr_helpers import publish, save_state
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import SensorData
from fastapi import FastAPI, Request

settings = ServiceSettings(service_name="ingestion")
setup_logging(settings.log_level)
logger = structlog.get_logger()

app = FastAPI(title="Ingestion Service", version="0.1.0")
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)


@app.post("/ingest")
async def ingest(data: SensorData) -> dict:
    """Receive sensor data and publish to raw-data topic."""
    await publish("raw-data", data.model_dump(mode="json"))
    await save_state(
        f"ingestion:last:{data.sensor_id}",
        {"timestamp": data.timestamp.isoformat(), "values": data.values},
    )
    return {"status": "published", "sensor_id": data.sensor_id}


@app.post("/ingest/batch")
async def ingest_batch(request: Request) -> dict:
    """Receive a batch of sensor data."""
    items = await request.json()
    count = 0
    for item in items:
        data = SensorData(**item)
        await publish("raw-data", data.model_dump(mode="json"))
        count += 1
    return {"status": "published", "count": count}


# --- Dapr Subscription ---
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return []
