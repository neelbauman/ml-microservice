"""Training data preprocessing — generates synthetic sensor data for demo."""

import json
import os
import random
from pathlib import Path

import numpy as np
import structlog
from common.logging import setup_logging

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = structlog.get_logger()

OUTPUT_DIR = Path(os.getenv("DATA_OUTPUT_DIR", "/tmp/ml-data"))
NUM_NORMAL = int(os.getenv("NUM_NORMAL_SAMPLES", "100000"))
NUM_ANOMALY = int(os.getenv("NUM_ANOMALY_SAMPLES", "200"))
NUM_FEATURES = int(os.getenv("NUM_FEATURES", "8"))


def generate_normal(n: int, n_features: int) -> np.ndarray:
    """Generate normal sensor data (low variance, centered)."""
    return np.random.normal(loc=0.5, scale=0.1, size=(n, n_features)).astype(np.float32)


def generate_anomaly(n: int, n_features: int) -> np.ndarray:
    """Generate anomalous sensor data (high variance, shifted)."""
    data = np.random.normal(loc=0.5, scale=0.1, size=(n, n_features)).astype(np.float32)
    for i in range(n):
        # Inject spikes into 1-3 random features
        spike_count = random.randint(1, min(3, n_features))
        spike_indices = random.sample(range(n_features), spike_count)
        for idx in spike_indices:
            data[i, idx] += random.choice([-1, 1]) * random.uniform(0.5, 1.5)
    return data


def main() -> None:
    logger.info("preprocess_start", num_normal=NUM_NORMAL, num_anomaly=NUM_ANOMALY)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    normal = generate_normal(NUM_NORMAL, NUM_FEATURES)
    anomaly = generate_anomaly(NUM_ANOMALY, NUM_FEATURES)

    # Save training data (normal only — autoencoder learns normal patterns)
    np.save(OUTPUT_DIR / "train.npy", normal)

    # Save evaluation data (mix of normal and anomaly with labels)
    eval_data = np.concatenate([normal[:500], anomaly])
    eval_labels = np.concatenate([np.zeros(500), np.ones(len(anomaly))])
    np.save(OUTPUT_DIR / "eval_data.npy", eval_data)
    np.save(OUTPUT_DIR / "eval_labels.npy", eval_labels)

    # Save metadata
    meta = {
        "num_features": NUM_FEATURES,
        "feature_names": [f"sensor_{i}" for i in range(NUM_FEATURES)],
        "train_samples": NUM_NORMAL,
        "eval_samples": len(eval_data),
        "anomaly_ratio": len(anomaly) / len(eval_data),
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(meta, indent=2))

    logger.info("preprocess_done", output_dir=str(OUTPUT_DIR), train_shape=list(normal.shape))


if __name__ == "__main__":
    main()
