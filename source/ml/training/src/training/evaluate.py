"""Evaluation script — computes metrics on held-out data."""

import json
import os
from pathlib import Path

import mlflow
import numpy as np
import structlog
import torch
from common.logging import setup_logging
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = structlog.get_logger()

DATA_DIR = Path(os.getenv("DATA_OUTPUT_DIR", "/tmp/ml-data"))
MODEL_DIR = Path(os.getenv("MODEL_OUTPUT_DIR", "/tmp/ml-models"))
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")
THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.05"))


def _get_device() -> torch.device:
    """Select the best available device (CUDA > CPU)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def main() -> None:
    device = _get_device()
    logger.info("evaluate_start", model_version=MODEL_VERSION, device=str(device))

    # Load eval data
    eval_data = np.load(DATA_DIR / "eval_data.npy")
    eval_labels = np.load(DATA_DIR / "eval_labels.npy")
    input_dim = eval_data.shape[1]

    # Rebuild model and load weights
    from training.train import _build_model

    model = _build_model(input_dim, int(os.getenv("LATENT_DIM", "3")))
    model.load_state_dict(torch.load(MODEL_DIR / f"model-{MODEL_VERSION}.pt", weights_only=True))
    model.to(device)
    model.eval()

    # Compute anomaly scores (reconstruction error)
    with torch.no_grad():
        x = torch.from_numpy(eval_data).to(device)
        recon = model(x)
        scores = ((x - recon) ** 2).mean(dim=1).cpu().numpy()

    # Binary predictions
    preds = (scores > THRESHOLD).astype(int)

    metrics = {
        "auc_roc": float(roc_auc_score(eval_labels, scores)),
        "precision": float(precision_score(eval_labels, preds, zero_division=0)),
        "recall": float(recall_score(eval_labels, preds, zero_division=0)),
        "f1": float(f1_score(eval_labels, preds, zero_division=0)),
        "threshold": THRESHOLD,
        "mean_normal_score": float(scores[eval_labels == 0].mean()),
        "mean_anomaly_score": float(scores[eval_labels == 1].mean()),
    }

    # Log to MLflow
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run(run_name=f"eval-{MODEL_VERSION}"):
        mlflow.log_metrics(metrics)
        # Save metrics as artifact too
        metrics_path = MODEL_DIR / f"metrics-{MODEL_VERSION}.json"
        metrics_path.write_text(json.dumps(metrics, indent=2))
        mlflow.log_artifact(str(metrics_path))

    logger.info("evaluate_done", **{k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()})


if __name__ == "__main__":
    main()
