"""Anomaly detection model definitions."""

import torch
from torch import nn


class AnomalyAutoencoder(nn.Module):
    """Simple autoencoder for sensor anomaly detection.

    Reconstruction error が大きい入力を異常とみなす。
    """

    def __init__(self, input_dim: int = 8, latent_dim: int = 3):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 16),
            nn.ReLU(),
            nn.Linear(16, latent_dim),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, 16),
            nn.ReLU(),
            nn.Linear(16, input_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.decoder(self.encoder(x))

    def anomaly_score(self, x: torch.Tensor) -> torch.Tensor:
        """Compute per-sample reconstruction error (MSE)."""
        with torch.no_grad():
            recon = self.forward(x)
            return ((x - recon) ** 2).mean(dim=1)
