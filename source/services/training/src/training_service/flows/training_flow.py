"""Prefect flow — full training pipeline from preprocessing to model registration."""

from datetime import datetime

import structlog
from prefect import flow, task

from training_service.config import TrainingSettings
from training_service.tasks.evaluate import evaluate_model
from training_service.tasks.preprocess import preprocess_data
from training_service.tasks.register import register_model
from training_service.tasks.train import train_model
from training_service.tasks.validate import validate_data

logger = structlog.get_logger()


@flow(
    name="training-pipeline",
    retries=0,
    log_prints=True,
    timeout_seconds=7200,
)
def training_pipeline(
    model_version: str | None = None,
    skip_preprocess: bool = False,
    s3_data_path: str | None = None,
) -> dict:
    """Full training pipeline: preprocess -> validate -> train -> evaluate -> register.

    Args:
        model_version: Version tag for the model. Auto-generated if not provided.
        skip_preprocess: If True, skip data generation and use existing data.
        s3_data_path: If set, download data from S3 instead of generating synthetics.
    """
    settings = TrainingSettings()

    if model_version is None:
        model_version = f"v{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    logger.info(
        "pipeline_start",
        model_version=model_version,
        skip_preprocess=skip_preprocess,
        s3_data_path=s3_data_path,
    )

    # Step 1: Preprocess (generate or download data)
    if not skip_preprocess:
        if s3_data_path:
            data_dir = download_s3_data(
                s3_path=s3_data_path,
                local_dir=settings.data_output_dir,
                endpoint_url=settings.s3_endpoint_url,
                bucket=settings.s3_data_bucket,
            )
        else:
            data_dir = preprocess_data(
                data_dir=settings.data_output_dir,
                num_normal=settings.num_normal_samples,
                num_anomaly=settings.num_anomaly_samples,
                num_features=settings.num_features,
            )
    else:
        data_dir = settings.data_output_dir

    # Step 2: Validate
    validate_data(data_dir=data_dir)

    # Step 3: Train
    train_model(
        data_dir=data_dir,
        model_output_dir=settings.model_output_dir,
        model_version=model_version,
        mlflow_tracking_uri=settings.mlflow_tracking_uri,
        experiment_name=settings.mlflow_experiment_name,
        epochs=settings.epochs,
        batch_size=settings.batch_size,
        learning_rate=settings.learning_rate,
        latent_dim=settings.latent_dim,
    )

    # Step 4: Evaluate
    metrics = evaluate_model(
        data_dir=data_dir,
        model_output_dir=settings.model_output_dir,
        model_version=model_version,
        mlflow_tracking_uri=settings.mlflow_tracking_uri,
        experiment_name=settings.mlflow_experiment_name,
        latent_dim=settings.latent_dim,
        anomaly_threshold=settings.anomaly_threshold,
    )

    # Step 5: Register
    register_result = register_model(
        model_output_dir=settings.model_output_dir,
        model_version=model_version,
        mlflow_tracking_uri=settings.mlflow_tracking_uri,
        experiment_name=settings.mlflow_experiment_name,
        registry_name=settings.model_registry_name,
        dapr_http_port=settings.dapr_http_port,
        data_dir=data_dir,
    )

    result = {
        "model_version": model_version,
        "metrics": metrics,
        "model_uri": register_result["model_uri"],
        "run_id": register_result["run_id"],
    }

    logger.info("pipeline_complete", **result)
    return result


@flow(name="training-pipeline-retraining", retries=0, log_prints=True)
def retraining_pipeline(
    s3_data_path: str,
    model_version: str | None = None,
) -> dict:
    """Triggered retraining — downloads new data from S3 and trains.

    This is the flow triggered by S3/MinIO file events via Dapr pub/sub.
    """
    return training_pipeline(
        model_version=model_version,
        skip_preprocess=False,
        s3_data_path=s3_data_path,
    )


@task(name="download-s3-data", retries=2, retry_delay_seconds=10)
def download_s3_data(
    s3_path: str,
    local_dir: str,
    endpoint_url: str | None = None,
    bucket: str = "ml-data",
) -> str:
    """Download training data from S3/MinIO to local directory."""
    from pathlib import Path

    import boto3

    local = Path(local_dir)
    local.mkdir(parents=True, exist_ok=True)

    client_kwargs = {}
    if endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    s3 = boto3.client("s3", **client_kwargs)

    # s3_path is a prefix like "training/2024-01/"
    prefix = s3_path.lstrip("/")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        filename = key.split("/")[-1]
        if filename:
            dest = local / filename
            s3.download_file(bucket, key, str(dest))
            logger.info("s3_download", key=key, dest=str(dest))

    return local_dir
