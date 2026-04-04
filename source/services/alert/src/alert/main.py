"""Alert service — receives anomaly alerts and logs/notifies."""

from collections import deque

import structlog
from common.config import ServiceSettings
from common.dapr_helpers import save_state
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import Alert
from fastapi import FastAPI, Request

settings = ServiceSettings(service_name="alert")
setup_logging(settings.log_level)
logger = structlog.get_logger()

app = FastAPI(title="Alert Service", version="0.1.0")
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)

# In-memory alert history (for demo dashboard)
recent_alerts: deque[dict] = deque(maxlen=100)


# --- Dapr Subscription ---
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    return [
        {"pubsubname": "pubsub", "topic": "alerts", "route": "/handle-alert"},
    ]


@app.post("/handle-alert")
async def handle_alert(request: Request) -> dict:
    """Process incoming alert."""
    envelope = await request.json()
    raw = envelope.get("data", envelope)
    alert = Alert(**raw)

    await logger.awarning(
        "ALERT",
        alert_id=alert.alert_id,
        sensor_id=alert.sensor_id,
        level=alert.level.value,
        message=alert.message,
    )

    recent_alerts.appendleft(alert.model_dump(mode="json"))

    await save_state(
        f"alert:last:{alert.sensor_id}",
        alert.model_dump(mode="json"),
    )

    return {"status": "ok", "alert_id": alert.alert_id}


@app.get("/alerts/recent")
async def get_recent_alerts(limit: int = 20) -> list:
    """Get recent alerts (for dashboard)."""
    return list(recent_alerts)[:limit]


@app.get("/alerts/stats")
async def get_alert_stats() -> dict:
    """Get alert statistics."""
    alerts = list(recent_alerts)
    return {
        "total": len(alerts),
        "critical": sum(1 for a in alerts if a.get("level") == "critical"),
        "warning": sum(1 for a in alerts if a.get("level") == "warning"),
        "sensors": list({a["sensor_id"] for a in alerts}),
    }
