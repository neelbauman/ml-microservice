"""Training task — wraps existing training.train module."""

from pathlib import Path

import mlflow
import numpy as np
import structlog
import torch
from prefect import task
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from training.train import _build_model

logger = structlog.get_logger()


def _get_device() -> torch.device:
    """Select the best available device (CUDA > CPU)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


@task(name="train-model", retries=0, timeout_seconds=3600)
def train_model(
    data_dir: str,
    model_output_dir: str,
    model_version: str,
    mlflow_tracking_uri: str,
    experiment_name: str,
    epochs: int = 50,
    batch_size: int = 64,
    learning_rate: float = 1e-3,
    latent_dim: int = 3,
) -> dict:
    """Train the autoencoder model and log to MLflow.

    Returns dict with run_id, model_path, onnx_path.
    """
    device = _get_device()
    logger.info("train_start", device=str(device))

    data_path = Path(data_dir)
    output_path = Path(model_output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load data
    train_data = np.load(data_path / "train.npy")
    input_dim = train_data.shape[1]

    dataset = TensorDataset(torch.from_numpy(train_data))
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    # Build model
    model = _build_model(input_dim, latent_dim).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)
    criterion = nn.MSELoss()

    # MLflow tracking
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"train-{model_version}") as run:
        mlflow.log_params(
            {
                "input_dim": input_dim,
                "latent_dim": latent_dim,
                "epochs": epochs,
                "batch_size": batch_size,
                "learning_rate": learning_rate,
                "model_version": model_version,
                "device": str(device),
            }
        )

        for epoch in range(epochs):
            total_loss = 0.0
            for (batch,) in loader:
                batch = batch.to(device)
                optimizer.zero_grad()
                output = model(batch)
                loss = criterion(output, batch)
                loss.backward()
                optimizer.step()
                total_loss += loss.item() * len(batch)

            avg_loss = total_loss / len(dataset)
            mlflow.log_metric("train_loss", avg_loss, step=epoch)

            if (epoch + 1) % 10 == 0:
                logger.info("epoch", epoch=epoch + 1, loss=round(avg_loss, 6))

        # Save model artifacts (always on CPU for portability)
        model_path = output_path / f"model-{model_version}.pt"
        torch.save(model.cpu().state_dict(), model_path)

        model.eval()
        onnx_path = output_path / f"model-{model_version}.onnx"
        dummy = torch.randn(1, input_dim)
        torch.onnx.export(
            model,
            dummy,
            str(onnx_path),
            input_names=["input"],
            output_names=["output"],
        )

        mlflow.log_artifact(str(model_path))
        mlflow.log_artifact(str(onnx_path))
        mlflow.log_artifact(str(data_path / "metadata.json"))

        logger.info("train_done", run_id=run.info.run_id, model_path=str(model_path))

        return {
            "run_id": run.info.run_id,
            "model_path": str(model_path),
            "onnx_path": str(onnx_path),
            "input_dim": input_dim,
        }
