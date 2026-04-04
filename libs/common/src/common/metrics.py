"""Prometheus metrics instrumentation for FastAPI services."""

from prometheus_fastapi_instrumentator import Instrumentator


def setup_metrics(app) -> Instrumentator:
    """Instrument a FastAPI app with default HTTP metrics and expose /metrics."""
    instrumentator = Instrumentator(
        excluded_handlers=["/health", "/dapr/subscribe"],
    )
    instrumentator.instrument(app).expose(app, endpoint="/metrics")
    return instrumentator
