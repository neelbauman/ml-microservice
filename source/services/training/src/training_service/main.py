"""Training workflow service — FastAPI + Dapr event triggers for Prefect flows."""

import structlog
from common.health import router as health_router
from common.logging import setup_logging
from common.metrics import setup_metrics
from common.middleware import RequestLoggingMiddleware
from common.models import TrainingTriggerEvent
from fastapi import BackgroundTasks, FastAPI

from training_service.config import TrainingSettings
from training_service.flows.training_flow import retraining_pipeline, training_pipeline

settings = TrainingSettings()
setup_logging(settings.log_level)
logger = structlog.get_logger()

app = FastAPI(title="Training Workflow Service", version="0.1.0")
app.add_middleware(RequestLoggingMiddleware)
app.include_router(health_router)
setup_metrics(app)


# ──────────────────────────────────────────────
# REST API triggers
# ──────────────────────────────────────────────
@app.post("/trigger")
async def trigger_training(
    event: TrainingTriggerEvent,
    background: BackgroundTasks,
) -> dict:
    """Manually trigger a training pipeline run.

    The flow runs in a background thread so the API returns immediately.
    """
    logger.info("trigger_received", source=event.source, s3_key=event.s3_key)
    background.add_task(_run_flow, event)
    return {"status": "accepted", "source": event.source}


@app.post("/trigger/full")
async def trigger_full_pipeline(background: BackgroundTasks) -> dict:
    """Trigger a full training pipeline (preprocess + train + evaluate + register).

    Equivalent to `make train-all` but as a service call.
    """
    event = TrainingTriggerEvent(source=TrainingTriggerEvent.TriggerSource.MANUAL)
    background.add_task(_run_flow, event)
    return {"status": "accepted", "source": "manual"}


# ──────────────────────────────────────────────
# Dapr subscription — listens for training-data-events
# ──────────────────────────────────────────────
@app.get("/dapr/subscribe")
async def subscribe() -> list:
    """Dapr subscription endpoint."""
    return [
        {
            "pubsubname": "pubsub",
            "topic": "training-data-events",
            "route": "/events/training-data",
            "metadata": {
                "rawPayload": "true",
            },
        },
    ]


@app.post("/events/training-data")
async def handle_training_data_event(request: dict, background: BackgroundTasks) -> dict:
    """Handle training data events from S3/MinIO bucket notifications via Dapr.

    MinIO sends S3-compatible event notifications through Kafka (Redpanda).
    The event payload follows the S3 event notification format.
    """
    logger.info("data_event_received", payload=request)

    # Parse S3 event notification format
    data = request.get("data", request)

    # MinIO/S3 event notifications have a "Records" array
    records = data.get("Records", [])
    for record in records:
        s3_info = record.get("s3", {})
        bucket_name = s3_info.get("bucket", {}).get("name")
        object_key = s3_info.get("object", {}).get("key")

        if not object_key:
            continue

        # Only trigger on relevant file types
        if not _is_training_data(object_key):
            logger.debug("skipping_non_training_file", key=object_key)
            continue

        event = TrainingTriggerEvent(
            source=TrainingTriggerEvent.TriggerSource.S3_EVENT,
            s3_bucket=bucket_name,
            s3_key=object_key,
        )
        background.add_task(_run_flow, event)

    # If event is not S3 format, treat as a direct trigger
    if not records:
        s3_key = data.get("s3_key") or data.get("key")
        s3_bucket = data.get("s3_bucket") or data.get("bucket")
        event = TrainingTriggerEvent(
            source=TrainingTriggerEvent.TriggerSource.S3_EVENT,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
        )
        background.add_task(_run_flow, event)

    return {"status": "SUCCESS"}


# ──────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────
def _is_training_data(key: str) -> bool:
    """Check if an S3 object key looks like training data."""
    training_extensions = {".npy", ".csv", ".parquet", ".npz"}
    training_prefixes = {"training/", "data/", "datasets/"}
    return any(key.endswith(ext) for ext in training_extensions) or any(
        key.startswith(prefix) for prefix in training_prefixes
    )


def _run_flow(event: TrainingTriggerEvent) -> None:
    """Execute the appropriate Prefect flow based on the trigger event."""
    try:
        if event.s3_key:
            # Extract the S3 prefix (directory) from the object key
            s3_prefix = "/".join(event.s3_key.split("/")[:-1]) + "/"
            result = retraining_pipeline(
                s3_data_path=s3_prefix,
                model_version=event.model_version,
            )
        else:
            result = training_pipeline(
                model_version=event.model_version,
            )

        logger.info("flow_complete", result=result)
    except Exception:
        logger.exception("flow_failed", source=event.source, s3_key=event.s3_key)
