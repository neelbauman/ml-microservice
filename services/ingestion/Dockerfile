FROM ghcr.io/astral-sh/uv:0.5-python3.12-bookworm-slim AS builder

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

COPY pyproject.toml uv.lock ./
COPY libs/common/pyproject.toml libs/common/pyproject.toml
COPY services/ingestion/pyproject.toml services/ingestion/pyproject.toml
COPY services/preprocessing/pyproject.toml services/preprocessing/pyproject.toml
COPY services/inference/pyproject.toml services/inference/pyproject.toml
COPY services/alert/pyproject.toml services/alert/pyproject.toml
COPY ml/training/pyproject.toml ml/training/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package ingestion-svc --no-dev --no-install-workspace

COPY libs/common/src/ libs/common/src/
COPY services/ingestion/src/ services/ingestion/src/

# --- dev: editable install — .pth が src/ を参照 ---
FROM builder AS dev-builder
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package ingestion-svc --no-dev

FROM python:3.12-slim AS dev
WORKDIR /app
COPY --from=dev-builder /app/.venv /app/.venv
COPY --from=dev-builder /app/libs/common/src/ /app/libs/common/src/
COPY --from=dev-builder /app/services/ingestion/src/ /app/services/ingestion/src/
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "ingestion.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# --- prod: non-editable — site-packages に直接コピー ---
FROM builder AS prod-builder
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --package ingestion-svc --no-dev --no-editable

FROM python:3.12-slim AS prod
WORKDIR /app
COPY --from=prod-builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "ingestion.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
