"""Preprocessing task — wraps existing training.preprocess module."""

import structlog
from prefect import task
from training.preprocess import generate_anomaly, generate_normal

logger = structlog.get_logger()


@task(name="preprocess-data", retries=0)
def preprocess_data(
    data_dir: str,
    num_normal: int = 100_000,
    num_anomaly: int = 200,
    num_features: int = 8,
) -> str:
    """Generate synthetic training data (delegates to existing preprocess module).

    Returns the output directory path.
    """
    import json
    from pathlib import Path

    import numpy as np

    output = Path(data_dir)
    output.mkdir(parents=True, exist_ok=True)

    normal = generate_normal(num_normal, num_features)
    anomaly = generate_anomaly(num_anomaly, num_features)

    # Training data (normal only — autoencoder learns normal patterns)
    np.save(output / "train.npy", normal)

    # Evaluation data (mix of normal and anomaly with labels)
    eval_data = np.concatenate([normal[:500], anomaly])
    eval_labels = np.concatenate([np.zeros(500), np.ones(len(anomaly))])
    np.save(output / "eval_data.npy", eval_data)
    np.save(output / "eval_labels.npy", eval_labels)

    # Metadata
    meta = {
        "num_features": num_features,
        "feature_names": [f"sensor_{i}" for i in range(num_features)],
        "train_samples": num_normal,
        "eval_samples": len(eval_data),
        "anomaly_ratio": len(anomaly) / len(eval_data),
    }
    (output / "metadata.json").write_text(json.dumps(meta, indent=2))

    logger.info("preprocess_done", output_dir=data_dir, train_shape=list(normal.shape))
    return data_dir
