"""Training data preprocessing — generates synthetic sensor data for demo.

Generates multivariate sensor data with realistic inter-feature correlations
and multiple anomaly patterns (spike, drift, stuck, correlation-break, noise).
"""

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

# Sensor definitions: (name, mean, std) representing realistic operating ranges
SENSOR_PROFILES = [
    ("temperature", 25.0, 2.0),  # °C
    ("humidity", 60.0, 5.0),  # %RH
    ("pressure", 1013.0, 3.0),  # hPa
    ("vibration", 0.5, 0.1),  # g
    ("current", 5.0, 0.5),  # A
    ("voltage", 220.0, 2.0),  # V
    ("rpm", 3000.0, 50.0),  # RPM
    ("noise_level", 45.0, 3.0),  # dB
]


def _build_correlation_matrix(n_features: int) -> np.ndarray:
    """Build a realistic correlation matrix with physically motivated relationships.

    - temperature ↔ humidity:    negative correlation  (hot → dry)
    - temperature ↔ vibration:   positive correlation  (heat from friction)
    - current ↔ voltage:         positive correlation  (load)
    - rpm �� vibration:           positive correlation  (mechanical)
    - rpm ↔ noise_level:         positive correlation  (mechanical noise)
    - current ↔ rpm:             positive correlation  (motor load)
    """
    corr = np.eye(n_features)

    # Define pairwise correlations (index, index, rho)
    pairs = [
        (0, 1, -0.5),  # temperature ↔ humidity
        (0, 3, 0.3),  # temperature ↔ vibration
        (4, 5, 0.6),  # current ↔ voltage
        (6, 3, 0.5),  # rpm ↔ vibration
        (6, 7, 0.6),  # rpm ↔ noise_level
        (4, 6, 0.4),  # current ↔ rpm
    ]

    for i, j, rho in pairs:
        if i < n_features and j < n_features:
            corr[i, j] = rho
            corr[j, i] = rho

    # Ensure positive semi-definite via eigenvalue clipping
    eigvals, eigvecs = np.linalg.eigh(corr)
    eigvals = np.maximum(eigvals, 1e-6)
    corr = eigvecs @ np.diag(eigvals) @ eigvecs.T
    # Re-normalize to correlation matrix
    d = np.sqrt(np.diag(corr))
    corr = corr / np.outer(d, d)
    np.fill_diagonal(corr, 1.0)

    return corr


def _build_covariance(n_features: int) -> tuple[np.ndarray, np.ndarray]:
    """Build mean vector and covariance matrix from sensor profiles."""
    profiles = SENSOR_PROFILES[:n_features]
    means = np.array([p[1] for p in profiles], dtype=np.float64)
    stds = np.array([p[2] for p in profiles], dtype=np.float64)
    corr = _build_correlation_matrix(n_features)
    cov = np.outer(stds, stds) * corr
    return means, cov


def generate_normal(n: int, n_features: int) -> np.ndarray:
    """Generate normal sensor data with inter-feature correlations."""
    means, cov = _build_covariance(n_features)
    data = np.random.multivariate_normal(means, cov, size=n).astype(np.float32)
    return data


def generate_anomaly(n: int, n_features: int) -> np.ndarray:
    """Generate anomalous sensor data using multiple realistic anomaly patterns.

    Five anomaly types, roughly equally distributed:
      - spike:             sudden extreme value in 1-3 features
      - drift:             gradual shift away from normal operating range
      - stuck:             sensor frozen at a constant value
      - correlation_break: feature values individually normal but correlations violated
      - noise_burst:       sudden increase in noise / variance
    """
    means, cov = _build_covariance(n_features)
    stds = np.sqrt(np.diag(cov))

    anomaly_types = ["spike", "drift", "stuck", "correlation_break", "noise_burst"]
    type_counts = _distribute(n, len(anomaly_types))

    chunks: list[np.ndarray] = []
    type_labels: list[str] = []

    for atype, count in zip(anomaly_types, type_counts, strict=True):
        if count == 0:
            continue
        chunk = _generate_anomaly_type(atype, count, n_features, means, stds, cov)
        chunks.append(chunk)
        type_labels.extend([atype] * count)

    data = np.concatenate(chunks, axis=0).astype(np.float32)

    # Shuffle so anomaly types are mixed
    indices = np.random.permutation(len(data))
    return data[indices]


def _generate_anomaly_type(
    atype: str,
    count: int,
    n_features: int,
    means: np.ndarray,
    stds: np.ndarray,
    cov: np.ndarray,
) -> np.ndarray:
    """Generate a batch of a specific anomaly type."""
    if atype == "spike":
        return _anomaly_spike(count, n_features, means, stds, cov)
    elif atype == "drift":
        return _anomaly_drift(count, n_features, means, stds, cov)
    elif atype == "stuck":
        return _anomaly_stuck(count, n_features, means, stds, cov)
    elif atype == "correlation_break":
        return _anomaly_correlation_break(count, n_features, means, stds)
    elif atype == "noise_burst":
        return _anomaly_noise_burst(count, n_features, means, stds, cov)
    else:
        raise ValueError(f"Unknown anomaly type: {atype}")


def _anomaly_spike(n: int, n_features: int, means: np.ndarray, stds: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Sudden extreme value in 1-3 features (e.g. sensor malfunction, sudden impact)."""
    data = np.random.multivariate_normal(means, cov, size=n)
    for i in range(n):
        k = random.randint(1, min(3, n_features))
        targets = random.sample(range(n_features), k)
        for idx in targets:
            direction = random.choice([-1, 1])
            magnitude = random.uniform(4, 8)  # 4-8 sigma away
            data[i, idx] = means[idx] + direction * magnitude * stds[idx]
    return data


def _anomaly_drift(n: int, n_features: int, means: np.ndarray, stds: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Gradual shift from normal range (e.g. bearing degradation, calibration drift)."""
    data = np.random.multivariate_normal(means, cov, size=n)
    # Pick 1-2 features to drift
    k = random.randint(1, min(2, n_features))
    drift_features = random.sample(range(n_features), k)
    for idx in drift_features:
        direction = random.choice([-1, 1])
        shift = direction * random.uniform(3, 6) * stds[idx]
        data[:, idx] += shift
    return data


def _anomaly_stuck(n: int, n_features: int, means: np.ndarray, stds: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Sensor frozen at a constant value (e.g. sensor failure, buffer overflow)."""
    data = np.random.multivariate_normal(means, cov, size=n)
    # Pick 1-2 features to freeze
    k = random.randint(1, min(2, n_features))
    stuck_features = random.sample(range(n_features), k)
    for idx in stuck_features:
        # Freeze at an arbitrary point (could be near mean or at boundary)
        frozen_value = means[idx] + random.uniform(-1, 1) * stds[idx]
        data[:, idx] = frozen_value
    return data


def _anomaly_correlation_break(n: int, n_features: int, means: np.ndarray, stds: np.ndarray) -> np.ndarray:
    """Feature values individually plausible but inter-feature correlations violated.

    Generated as independent normals — no correlation structure.
    The autoencoder should detect the broken multivariate pattern.
    """
    data = np.column_stack([np.random.normal(means[j], stds[j], size=n) for j in range(n_features)])
    return data


def _anomaly_noise_burst(n: int, n_features: int, means: np.ndarray, stds: np.ndarray, cov: np.ndarray) -> np.ndarray:
    """Sudden increase in variance (e.g. electrical interference, loose connection)."""
    # Use inflated covariance (3-5x variance)
    scale = random.uniform(3.0, 5.0)
    noisy_cov = cov * (scale**2)
    data = np.random.multivariate_normal(means, noisy_cov, size=n)
    return data


def _distribute(total: int, buckets: int) -> list[int]:
    """Distribute total evenly across buckets, remainder goes to first."""
    base = total // buckets
    remainder = total % buckets
    counts = [base] * buckets
    for i in range(remainder):
        counts[i] += 1
    return counts


def _get_feature_names(n_features: int) -> list[str]:
    """Return feature names, using sensor profile names when available."""
    names = [p[0] for p in SENSOR_PROFILES[:n_features]]
    # Pad with generic names if n_features exceeds profile count
    for i in range(len(names), n_features):
        names.append(f"sensor_{i}")
    return names


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

    # Save scaler (computed from training data for normalization)
    scaler_mean = normal.mean(axis=0).astype(np.float32)
    scaler_std = normal.std(axis=0).astype(np.float32)
    scaler_std[scaler_std < 1e-8] = 1.0
    logger.info("scaler_computed", mean=scaler_mean.tolist(), std=scaler_std.tolist())

    # Save metadata
    meta = {
        "num_features": NUM_FEATURES,
        "feature_names": _get_feature_names(NUM_FEATURES),
        "sensor_profiles": [{"name": p[0], "mean": p[1], "std": p[2]} for p in SENSOR_PROFILES[:NUM_FEATURES]],
        "correlation_pairs": [
            {"features": ["temperature", "humidity"], "rho": -0.5},
            {"features": ["temperature", "vibration"], "rho": 0.3},
            {"features": ["current", "voltage"], "rho": 0.6},
            {"features": ["rpm", "vibration"], "rho": 0.5},
            {"features": ["rpm", "noise_level"], "rho": 0.6},
            {"features": ["current", "rpm"], "rho": 0.4},
        ],
        "anomaly_types": ["spike", "drift", "stuck", "correlation_break", "noise_burst"],
        "train_samples": NUM_NORMAL,
        "eval_samples": len(eval_data),
        "anomaly_ratio": len(anomaly) / len(eval_data),
    }
    (OUTPUT_DIR / "metadata.json").write_text(json.dumps(meta, indent=2))

    logger.info(
        "preprocess_done",
        output_dir=str(OUTPUT_DIR),
        train_shape=list(normal.shape),
        anomaly_types=meta["anomaly_types"],
    )


if __name__ == "__main__":
    main()
