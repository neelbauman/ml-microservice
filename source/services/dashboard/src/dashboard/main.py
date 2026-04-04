"""Dashboard service — receives raw data and inference results, serves real-time UI."""

import asyncio
import json
from collections import deque
from pathlib import Path

import structlog
from common.config import ServiceSettings
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import InferenceResult, SensorData
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

settings = ServiceSettings(service_name="dashboard")
setup_logging(settings.log_level)
logger = structlog.get_logger()

app = FastAPI(title="Dashboard Service", version="0.1.0")
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# In-memory buffers
MAX_EVENTS = 200
raw_data_buffer: deque[dict] = deque(maxlen=MAX_EVENTS)
inference_buffer: deque[dict] = deque(maxlen=MAX_EVENTS)
alert_buffer: deque[dict] = deque(maxlen=MAX_EVENTS)

# SSE subscribers
sse_subscribers: list[asyncio.Queue] = []


async def broadcast(event_type: str, data: dict) -> None:
    """Push an event to all connected SSE clients."""
    payload = json.dumps({"type": event_type, "data": data}, default=str)
    dead: list[asyncio.Queue] = []
    for q in sse_subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        sse_subscribers.remove(q)


# --- Dapr Subscriptions ---
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "raw-data", "route": "/events/raw-data"},
        {"pubsubname": "pubsub", "topic": "inference-results", "route": "/events/inference-results"},
        {"pubsubname": "pubsub", "topic": "alerts", "route": "/events/alerts"},
    ]


@app.post("/events/raw-data")
async def on_raw_data(request: Request) -> dict:
    """Receive raw sensor data from Dapr pub/sub."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    sensor = SensorData(**raw)
    event = sensor.model_dump(mode="json")
    raw_data_buffer.appendleft(event)
    await broadcast("raw-data", event)
    return {"status": "SUCCESS"}


@app.post("/events/inference-results")
async def on_inference_result(request: Request) -> dict:
    """Receive inference results from Dapr pub/sub."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    result = InferenceResult(**raw)
    event = result.model_dump(mode="json")
    inference_buffer.appendleft(event)
    await broadcast("inference-result", event)
    return {"status": "SUCCESS"}


@app.post("/events/alerts")
async def on_alert(request: Request) -> dict:
    """Receive alerts from Dapr pub/sub."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    alert_buffer.appendleft(raw)
    await broadcast("alert", raw)
    return {"status": "SUCCESS"}


# --- SSE endpoint ---
@app.get("/sse")
async def sse_stream(request: Request) -> EventSourceResponse:
    """Server-Sent Events stream for real-time updates."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    sse_subscribers.append(queue)

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield {"data": payload}
                except TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            if queue in sse_subscribers:
                sse_subscribers.remove(queue)

    return EventSourceResponse(event_generator())


# --- REST API for initial data load ---
@app.get("/api/raw-data")
async def get_raw_data(limit: int = 50) -> list:
    return list(raw_data_buffer)[:limit]


@app.get("/api/inference-results")
async def get_inference_results(limit: int = 50) -> list:
    return list(inference_buffer)[:limit]


@app.get("/api/alerts")
async def get_alerts(limit: int = 50) -> list:
    return list(alert_buffer)[:limit]


@app.get("/api/stats")
async def get_stats() -> dict:
    inferences = list(inference_buffer)
    anomalies = [r for r in inferences if r.get("is_anomaly")]
    sensors = {r["sensor_id"] for r in inferences}
    return {
        "total_raw": len(raw_data_buffer),
        "total_inferences": len(inferences),
        "total_anomalies": len(anomalies),
        "total_alerts": len(alert_buffer),
        "active_sensors": sorted(sensors),
    }


# --- Serve the dashboard UI ---
@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
