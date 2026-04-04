"""Model registration task — wraps existing training.register module."""

import json
from pathlib import Path

import httpx
import mlflow
import mlflow.onnx
import onnx
import structlog
from common.models import ModelUpdateEvent
from prefect import task

logger = structlog.get_logger()


@task(name="register-model", retries=2, retry_delay_seconds=5)
def register_model(
    model_output_dir: str,
    model_version: str,
    mlflow_tracking_uri: str,
    experiment_name: str,
    registry_name: str = "anomaly-detector",
    dapr_http_port: int = 3500,
    data_dir: str = "/tmp/ml-data",
) -> dict:
    """Register the ONNX model to MLflow and notify via Dapr pub/sub.

    Returns dict with model_uri and run_id.
    """
    model_dir = Path(model_output_dir)
    onnx_path = model_dir / f"model-{model_version}.onnx"

    if not onnx_path.exists():
        raise FileNotFoundError(f"ONNX model not found: {onnx_path}")

    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"register-{model_version}") as run:
        onnx_model = onnx.load(str(onnx_path))
        result = mlflow.onnx.log_model(
            onnx_model,
            name="model",
            registered_model_name=registry_name,
        )

        logger.info(
            "register_done",
            name=registry_name,
            model_uri=result.model_uri,
            run_id=run.info.run_id,
        )

    # Publish model update event via Dapr
    _publish_model_update(
        model_name=registry_name,
        model_version=model_version,
        model_uri=result.model_uri,
        run_id=run.info.run_id,
        dapr_http_port=dapr_http_port,
        data_dir=data_dir,
        model_output_dir=model_output_dir,
    )

    return {"model_uri": result.model_uri, "run_id": run.info.run_id}


def _publish_model_update(
    model_name: str,
    model_version: str,
    model_uri: str,
    run_id: str,
    dapr_http_port: int,
    data_dir: str,
    model_output_dir: str,
) -> None:
    """Publish model update event via Dapr pub/sub."""
    dapr_url = f"http://localhost:{dapr_http_port}/v1.0/publish/pubsub/model-updates"

    input_dim = None
    metadata_path = Path(data_dir) / "metadata.json"
    if metadata_path.exists():
        meta = json.loads(metadata_path.read_text())
        input_dim = meta.get("num_features")

    anomaly_threshold = None
    metrics_path = Path(model_output_dir) / f"metrics-{model_version}.json"
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
