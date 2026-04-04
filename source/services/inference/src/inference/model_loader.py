"""ONNX model manager — downloads from MLflow, runs via onnxruntime."""

import asyncio
import os
from pathlib import Path

import numpy as np
import onnxruntime as ort
import structlog

logger = structlog.get_logger()

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
MODEL_STORE_PATH = Path(os.getenv("MODEL_STORE_PATH", "/models"))


class ModelManager:
    """Thread-safe ONNX model lifecycle manager.

    Downloads models from MLflow Model Registry and serves predictions
    via onnxruntime. Supports hot-swapping: a new model can be loaded
    while the old one continues serving in-flight requests.
    """

    def __init__(self) -> None:
        self._session: ort.InferenceSession | None = None
        self._model_name: str = ""
        self._model_version: str = ""
        self._input_dim: int = 0
        self._anomaly_threshold: float = 0.05

    @property
    def is_loaded(self) -> bool:
        return self._session is not None

    @property
    def model_version(self) -> str:
        return self._model_version if self._session else "zscore-v1"

    @property
    def input_dim(self) -> int:
        return self._input_dim

    @property
    def info(self) -> dict:
        return {
            "loaded": self.is_loaded,
            "model_name": self._model_name,
            "model_version": self._model_version,
            "input_dim": self._input_dim,
            "anomaly_threshold": self._anomaly_threshold,
        }

    async def load_model(
        self,
        model_name: str,
        model_version: str,
        model_uri: str,
        input_dim: int | None = None,
        anomaly_threshold: float | None = None,
    ) -> None:
        """Download ONNX model from MLflow and create InferenceSession."""
        import mlflow

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        await logger.ainfo(
            "model_download_start",
            model_name=model_name,
            model_version=model_version,
            model_uri=model_uri,
        )

        # Download model artifacts in a thread to avoid blocking the event loop
        local_path = await asyncio.to_thread(
            mlflow.artifacts.download_artifacts, artifact_uri=model_uri,
        )

        # Find the ONNX file in the downloaded directory
        onnx_files = list(Path(local_path).rglob("*.onnx"))
        if not onnx_files:
            raise FileNotFoundError(f"No .onnx file found in {local_path}")

        onnx_path = onnx_files[0]

        # Create new session
        new_session = await asyncio.to_thread(
            ort.InferenceSession, str(onnx_path),
        )

        # Read input dimensions from the model
        model_input_dim = new_session.get_inputs()[0].shape[-1]

        # Atomic swap: replace session reference
        self._session = new_session
        self._model_name = model_name
        self._model_version = model_version
        self._input_dim = input_dim or model_input_dim
        if anomaly_threshold is not None:
            self._anomaly_threshold = anomaly_threshold

        await logger.ainfo(
            "model_loaded",
            model_name=model_name,
            model_version=model_version,
            onnx_path=str(onnx_path),
            input_dim=self._input_dim,
            anomaly_threshold=self._anomaly_threshold,
        )

    async def try_load_latest(self, registry_name: str = "anomaly-detector") -> bool:
        """Try to load the latest model from MLflow Model Registry on startup.

        Returns True if a model was loaded, False otherwise.
        """
        import mlflow
        from mlflow.exceptions import MlflowException

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        try:
            client = mlflow.MlflowClient()
            # Get the latest version of the registered model
            versions = await asyncio.to_thread(
                client.search_model_versions,
                f"name='{registry_name}'",
            )
            if not versions:
                await logger.ainfo("no_registered_model", registry=registry_name)
                return False

            latest = max(versions, key=lambda v: int(v.version))
            model_uri = f"models:/{registry_name}/{latest.version}"

            await logger.ainfo(
                "startup_model_found",
                registry=registry_name,
                version=latest.version,
                model_uri=model_uri,
            )

            await self.load_model(
                model_name=registry_name,
                model_version=latest.version,
                model_uri=model_uri,
            )
            return True
        except MlflowException as e:
            await logger.awarning("startup_model_load_skipped", error=str(e))
            return False
        except Exception as e:
            await logger.awarning("startup_model_load_failed", error=str(e))
            return False

    def predict(self, features: list[float]) -> tuple[float, float, dict[str, float]]:
        """Run ONNX inference. Returns (score, confidence, contributions).

        The autoencoder outputs a reconstruction of the input.
        Anomaly score is the reconstruction error (MSE).
        """
        session = self._session  # capture reference for thread safety
        if session is None:
            raise RuntimeError("No model loaded")

        expected_dim = self._input_dim
        input_features = list(features)

        # Handle dimension mismatch
        if len(input_features) < expected_dim:
            logger.warning(
                "input_dim_mismatch_pad",
                got=len(input_features),
                expected=expected_dim,
            )
            input_features.extend([0.0] * (expected_dim - len(input_features)))
        elif len(input_features) > expected_dim:
            logger.warning(
                "input_dim_mismatch_truncate",
                got=len(input_features),
                expected=expected_dim,
            )
            input_features = input_features[:expected_dim]

        # Prepare input
        input_array = np.array([input_features], dtype=np.float32)
        input_name = session.get_inputs()[0].name

        # Run inference
        output = session.run(None, {input_name: input_array})
        reconstructed = output[0]

        # Anomaly score = reconstruction error (MSE)
        per_feature_error = (input_array[0] - reconstructed[0]) ** 2
        mse = float(np.mean(per_feature_error))

        # Normalize score to 0-1 range based on threshold
        threshold = self._anomaly_threshold
        score = min(mse / (2.0 * threshold), 1.0) if threshold > 0 else 0.0
        score = round(score, 4)

        # Confidence: higher when score is far from the decision boundary (0.5)
        confidence = round(min(1.0, max(0.1, abs(score - 0.5) * 2 + 0.5)), 4)

        # Per-feature contributions (normalized)
        total_error = float(np.sum(per_feature_error)) or 1.0
        contributions = {
            f"f{i}": round(float(e / total_error), 3)
            for i, e in enumerate(per_feature_error[:len(features)])
        }

        return score, confidence, contributions
