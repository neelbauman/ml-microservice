"""Training service configuration."""

from pydantic_settings import BaseSettings


class TrainingSettings(BaseSettings):
    """Settings for the training workflow service."""

    service_name: str = "training"
    log_level: str = "INFO"
    dapr_http_port: int = 3500

    # Prefect
    prefect_api_url: str = "http://localhost:4200/api"

    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5001"
    mlflow_experiment_name: str = "anomaly-detection"

    # S3 / MinIO
    s3_data_bucket: str = "ml-data"
    s3_endpoint_url: str | None = None  # Set for MinIO, None for real S3
    aws_access_key_id: str = "minioadmin"
    aws_secret_access_key: str = "minioadmin"

    # Data paths
    data_output_dir: str = "/tmp/ml-data"
    model_output_dir: str = "/tmp/ml-models"

    # Training hyperparameters (defaults)
    epochs: int = 50
    batch_size: int = 64
    learning_rate: float = 1e-3
    latent_dim: int = 3
    num_features: int = 8
    num_normal_samples: int = 100000
    num_anomaly_samples: int = 200
    anomaly_threshold: float = 0.05

    # Model registry
    model_registry_name: str = "anomaly-detector"

    model_config = {"env_prefix": "", "case_sensitive": False}
