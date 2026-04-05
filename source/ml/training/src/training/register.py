"""Model registration — registers evaluated model to MLflow Model Registry."""

import json
import os
from pathlib import Path

import httpx
import mlflow
import mlflow.onnx
import onnx
import structlog
from common.logging import setup_logging
from common.models import ModelUpdateEvent

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = structlog.get_logger()

MODEL_DIR = Path(os.getenv("MODEL_OUTPUT_DIR", "/tmp/ml-models"))
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")
REGISTRY_NAME = os.getenv("MODEL_REGISTRY_NAME", "anomaly-detector")

DAPR_HTTP_PORT = int(os.getenv("DAPR_HTTP_PORT", "3501"))


def _publish_model_update(model_name: str, model_version: str, model_uri: str, run_id: str) -> None:
    """Publish model update event via Dapr pub/sub."""
    dapr_url = f"http://localhost:{DAPR_HTTP_PORT}/v1.0/publish/pubsub/model-updates"

    # Read input_dim from training metadata
    input_dim = None
    metadata_path = Path(os.getenv("DATA_OUTPUT_DIR", "/tmp/ml-data")) / "metadata.json"
    if metadata_path.exists():
        meta = json.loads(metadata_path.read_text())
        input_dim = meta.get("num_features")

    # Read anomaly_threshold from evaluation metrics
    anomaly_threshold = None
    metrics_path = MODEL_DIR / f"metrics-{MODEL_VERSION}.json"
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text())
        anomaly_threshold = metrics.get("threshold")

    event = ModelUpdateEvent(
        model_name=model_name,
        model_version=model_version,
        model_uri=model_uri,
        run_id=run_id,
        input_dim=input_dim,
        anomaly_threshold=anomaly_threshold,
    )

    try:
        resp = httpx.post(
            dapr_url,
            json=event.model_dump(mode="json"),
            headers={"Content-Type": "application/json"},
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("model_update_published", model_name=model_name, model_uri=model_uri)
    except Exception as e:
        logger.warning(
            "model_update_publish_failed",
            error=str(e),
            hint="Is Dapr sidecar reachable? For local dev, run 'make up' first.",
        )


def main() -> None:
    logger.info("register_start", model_version=MODEL_VERSION, registry=REGISTRY_NAME)

    onnx_path = MODEL_DIR / f"model-{MODEL_VERSION}.onnx"
    metrics_path = MODEL_DIR / f"metrics-{MODEL_VERSION}.json"

    if not onnx_path.exists():
        logger.error("model_not_found", path=str(onnx_path))
        raise FileNotFoundError(f"ONNX model not found: {onnx_path}")

    # Load metrics for description
    metrics = {}
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text())

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    scaler_path = MODEL_DIR / "scaler.json"

    with mlflow.start_run(run_name=f"register-{MODEL_VERSION}") as run:
        # Log and register the ONNX model in one step
        onnx_model = onnx.load(str(onnx_path))
        result = mlflow.onnx.log_model(
            onnx_model,
            name="model",
            registered_model_name=REGISTRY_NAME,
        )

        # Log scaler as artifact alongside the model
        if scaler_path.exists():
            mlflow.log_artifact(str(scaler_path))
            logger.info("scaler_logged", path=str(scaler_path))

        logger.info(
            "register_done",
            name=REGISTRY_NAME,
            model_uri=result.model_uri,
            run_id=run.info.run_id,
        )

    # Publish model update event to notify inference service
    _publish_model_update(
        model_name=REGISTRY_NAME,
        model_version=MODEL_VERSION,
        model_uri=result.model_uri,
        run_id=run.info.run_id,
    )


if __name__ == "__main__":
    main()
