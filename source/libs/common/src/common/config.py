"""Service configuration using pydantic-settings."""

from pydantic_settings import BaseSettings


class ServiceSettings(BaseSettings):
    service_name: str = "unknown"
    log_level: str = "INFO"
    dapr_http_port: int = 3500
    dapr_grpc_port: int = 50001

    model_config = {"env_prefix": "", "case_sensitive": False}
