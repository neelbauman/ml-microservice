"""Data validation task — checks data files before training."""

from pathlib import Path

import numpy as np
import structlog
from prefect import task

logger = structlog.get_logger()


@task(name="validate-data", retries=0)
def validate_data(data_dir: str, min_samples: int = 100) -> dict:
    """Validate training data files exist and have sufficient samples.

    Returns metadata about the validated data.
    """
    data_path = Path(data_dir)
    train_file = data_path / "train.npy"

    if not train_file.exists():
        raise FileNotFoundError(f"Training data not found: {train_file}")

    train_data = np.load(train_file)
    n_samples, n_features = train_data.shape

    if n_samples < min_samples:
        raise ValueError(f"Insufficient training samples: {n_samples} < {min_samples}")

    # Check for NaN/Inf
    if np.any(np.isnan(train_data)) or np.any(np.isinf(train_data)):
        raise ValueError("Training data contains NaN or Inf values")

    logger.info(
        "data_validated",
        samples=n_samples,
        features=n_features,
        data_dir=data_dir,
    )

    return {
        "n_samples": n_samples,
        "n_features": n_features,
        "data_dir": data_dir,
    }
