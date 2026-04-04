"""Shared Pydantic models for the ML pipeline."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class SensorData(BaseModel):
    """Raw sensor data from ingestion."""

    sensor_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    values: dict[str, float]
    metadata: dict[str, str] = Field(default_factory=dict)


class ProcessedData(BaseModel):
    """Preprocessed and normalized data."""

    sensor_id: str
    timestamp: datetime
    features: list[float]
    feature_names: list[str]
    original_values: dict[str, float]


class InferenceResult(BaseModel):
    """Model inference output."""

    sensor_id: str
    timestamp: datetime
    prediction: float
    confidence: float
    is_anomaly: bool
    model_version: str = "default-v1"
    feature_contributions: dict[str, float] = Field(default_factory=dict)


class AlertLevel(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class Alert(BaseModel):
    """Alert notification."""

    alert_id: str = ""
    sensor_id: str
    timestamp: datetime
    level: AlertLevel
    message: str
    inference_result: InferenceResult | None = None


class ModelUpdateEvent(BaseModel):
    """Notification that a new model version is available in MLflow."""

    model_name: str
    model_version: str
    model_uri: str
    run_id: str
    input_dim: int | None = None
    anomaly_threshold: float | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class TrainingTriggerEvent(BaseModel):
    """Event that triggers a training pipeline run.

    Published when new data files are placed in S3/MinIO (via bucket notifications)
    or when a manual training request is made.
    """

    class TriggerSource(StrEnum):
        S3_EVENT = "s3_event"
        MANUAL = "manual"
        SCHEDULE = "schedule"

    source: TriggerSource = TriggerSource.MANUAL
    s3_bucket: str | None = None
    s3_key: str | None = None
    model_version: str | None = None
    parameters: dict[str, str] = Field(default_factory=dict)
