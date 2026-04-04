FROM ghcr.io/astral-sh/uv:0.5-python3.12-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY source/libs/common/pyproject.toml source/libs/common/pyproject.toml
COPY source/services/ingestion/pyproject.toml source/services/ingestion/pyproject.toml
COPY source/services/preprocessing/pyproject.toml source/services/preprocessing/pyproject.toml
COPY source/services/inference/pyproject.toml source/services/inference/pyproject.toml
COPY source/services/alert/pyproject.toml source/services/alert/pyproject.toml
COPY source/services/dashboard/pyproject.toml source/services/dashboard/pyproject.toml
COPY source/ml/training/pyproject.toml source/ml/training/pyproject.toml
COPY source/services/training/pyproject.toml source/services/training/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package training-svc --no-dev --no-install-workspace

COPY source/libs/common/src/ source/libs/common/src/
COPY source/ml/training/src/ source/ml/training/src/
COPY source/services/training/src/ source/services/training/src/

# --- dev: editable install ---
FROM builder AS dev-builder
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package training-svc --no-dev

FROM python:3.12-slim AS dev
WORKDIR /app
COPY --from=dev-builder /app/.venv /app/.venv
COPY --from=dev-builder /app/source/libs/common/src/ /app/source/libs/common/src/
COPY --from=dev-builder /app/source/ml/training/src/ /app/source/ml/training/src/
COPY --from=dev-builder /app/source/services/training/src/ /app/source/services/training/src/
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "training_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# --- prod: non-editable ---
FROM builder AS prod-builder
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package training-svc --no-dev --no-editable

FROM python:3.12-slim AS prod
WORKDIR /app
COPY --from=prod-builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "training_service.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
