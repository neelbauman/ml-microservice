"""Training script — trains autoencoder and logs to MLflow."""

import os
from pathlib import Path

import mlflow
import numpy as np
import structlog
import torch
from common.logging import setup_logging
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

setup_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = structlog.get_logger()

DATA_DIR = Path(os.getenv("DATA_OUTPUT_DIR", "/tmp/ml-data"))
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "anomaly-detection")
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")
MODEL_OUTPUT = Path(os.getenv("MODEL_OUTPUT_DIR", "/tmp/ml-models"))

# Hyperparameters
EPOCHS = int(os.getenv("EPOCHS", "50"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
LR = float(os.getenv("LEARNING_RATE", "1e-3"))
LATENT_DIM = int(os.getenv("LATENT_DIM", "3"))


def load_model(input_dim: int, latent_dim: int):
    # Import from ml/models — available in the training container
    from ml.models import AnomalyAutoencoder

    return AnomalyAutoencoder(input_dim=input_dim, latent_dim=latent_dim)


def _build_model(input_dim: int, latent_dim: int) -> nn.Module:
    """Build autoencoder inline (avoids import issues outside container)."""
    encoder = nn.Sequential(nn.Linear(input_dim, 16), nn.ReLU(), nn.Linear(16, latent_dim), nn.ReLU())
    decoder = nn.Sequential(nn.Linear(latent_dim, 16), nn.ReLU(), nn.Linear(16, input_dim))

    class _AE(nn.Module):
        def __init__(self):
            super().__init__()
            self.encoder = encoder
            self.decoder = decoder

        def forward(self, x):
            return self.decoder(self.encoder(x))

    return _AE()


def _get_device() -> torch.device:
    """Select the best available device (CUDA > CPU)."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    return torch.device("cpu")


def main() -> None:
    device = _get_device()
    logger.info("train_start", model_version=MODEL_VERSION, epochs=EPOCHS, device=str(device))

    # Load data
    train_data = np.load(DATA_DIR / "train.npy")
    input_dim = train_data.shape[1]
    logger.info("data_loaded", shape=list(train_data.shape))

    # Dataset / DataLoader
    dataset = TensorDataset(torch.from_numpy(train_data))
    loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

    # Model
    model = _build_model(input_dim, LATENT_DIM).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    criterion = nn.MSELoss()

    # MLflow
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    with mlflow.start_run(run_name=f"train-{MODEL_VERSION}") as run:
        mlflow.log_params({
            "input_dim": input_dim,
            "latent_dim": LATENT_DIM,
            "epochs": EPOCHS,
            "batch_size": BATCH_SIZE,
            "learning_rate": LR,
            "model_version": MODEL_VERSION,
            "device": str(device),
        })

        # Training loop
        for epoch in range(EPOCHS):
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

        # Save model (always on CPU for portability)
        MODEL_OUTPUT.mkdir(parents=True, exist_ok=True)
        model_path = MODEL_OUTPUT / f"model-{MODEL_VERSION}.pt"
        torch.save(model.cpu().state_dict(), model_path)

        # Export to ONNX (model already on CPU)
        model.eval()
        onnx_path = MODEL_OUTPUT / f"model-{MODEL_VERSION}.onnx"
        dummy = torch.randn(1, input_dim)
        torch.onnx.export(model, dummy, str(onnx_path), input_names=["input"], output_names=["output"])

        # Log artifacts
        mlflow.log_artifact(str(model_path))
        mlflow.log_artifact(str(onnx_path))
        mlflow.log_artifact(str(DATA_DIR / "metadata.json"))

        logger.info("train_done", run_id=run.info.run_id, model_path=str(model_path))


if __name__ == "__main__":
    main()
