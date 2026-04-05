"""Evaluation task — wraps existing training.evaluate module."""

import json
from pathlib import Path

import mlflow
import numpy as np
import structlog
import torch
from prefect import task
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from training.train import _build_model

logger = structlog.get_logger()


def _get_device() -> torch.device:
    """Select the best available device (CUDA > CPU)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


@task(name="evaluate-model", retries=0)
def evaluate_model(
    data_dir: str,
    model_output_dir: str,
    model_version: str,
    mlflow_tracking_uri: str,
    experiment_name: str,
    latent_dim: int = 3,
    anomaly_threshold: float = 0.05,
) -> dict:
    """Evaluate the trained model on held-out data.

    Returns evaluation metrics dict.
    """
    device = _get_device()
    logger.info("evaluate_start", device=str(device))

    data_path = Path(data_dir)
    model_dir = Path(model_output_dir)

    eval_data = np.load(data_path / "eval_data.npy")
    eval_labels = np.load(data_path / "eval_labels.npy")
    input_dim = eval_data.shape[1]

    # Load scaler and normalize eval data
    scaler_path = model_dir / "scaler.json"
    scaler = json.loads(scaler_path.read_text())
    scaler_mean = np.array(scaler["mean"], dtype=np.float32)
    scaler_std = np.array(scaler["std"], dtype=np.float32)
    eval_data_norm = ((eval_data - scaler_mean) / scaler_std).astype(np.float32)

    # Rebuild model and load weights
    model = _build_model(input_dim, latent_dim)
    model.load_state_dict(torch.load(model_dir / f"model-{model_version}.pt", weights_only=True))
    model.to(device)
    model.eval()

    with torch.no_grad():
        x = torch.from_numpy(eval_data_norm).to(device)
        recon = model(x)
        scores = ((x - recon) ** 2).mean(dim=1).cpu().numpy()

    # Auto-compute threshold from normal samples: mean + 3*std
    normal_scores = scores[eval_labels == 0]
    anomaly_threshold = float(normal_scores.mean() + 3.0 * normal_scores.std())
    logger.info(
        "threshold_computed",
        threshold=round(anomaly_threshold, 6),
        normal_mean=round(float(normal_scores.mean()), 6),
        normal_std=round(float(normal_scores.std()), 6),
    )

    preds = (scores > anomaly_threshold).astype(int)

    metrics = {
        "auc_roc": float(roc_auc_score(eval_labels, scores)),
        "precision": float(precision_score(eval_labels, preds, zero_division=0)),
        "recall": float(recall_score(eval_labels, preds, zero_division=0)),
        "f1": float(f1_score(eval_labels, preds, zero_division=0)),
        "threshold": anomaly_threshold,
        "mean_normal_score": float(scores[eval_labels == 0].mean()),
        "mean_anomaly_score": float(scores[eval_labels == 1].mean()),
    }

    # Log to MLflow
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"eval-{model_version}"):
        mlflow.log_metrics(metrics)
        metrics_path = model_dir / f"metrics-{model_version}.json"
        metrics_path.write_text(json.dumps(metrics, indent=2))
        mlflow.log_artifact(str(metrics_path))

    logger.info(
        "evaluate_done",
        **{k: round(v, 4) if isinstance(v, float) else v for k, v in metrics.items()},
    )
    return metrics
