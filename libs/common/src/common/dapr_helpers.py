"""Dapr client helpers — thin wrappers for pub/sub, state, and bindings."""

import os

import httpx
import structlog

logger = structlog.get_logger()

DAPR_PORT = int(os.environ.get("DAPR_HTTP_PORT", "3500"))


def _dapr_url(path: str) -> str:
    return f"http://localhost:{DAPR_PORT}{path}"


async def publish(topic: str, data: dict, pubsub: str = "pubsub") -> None:
    """Publish an event to a Dapr pub/sub topic."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _dapr_url(f"/v1.0/publish/{pubsub}/{topic}"),
            json=data,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
    await logger.ainfo("published", topic=topic, pubsub=pubsub)


async def save_state(key: str, value: dict, store: str = "statestore") -> None:
    """Save a key-value pair to Dapr state store."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _dapr_url(f"/v1.0/state/{store}"),
            json=[{"key": key, "value": value}],
        )
        resp.raise_for_status()


async def get_state(key: str, store: str = "statestore") -> dict | None:
    """Retrieve a value from Dapr state store."""
    async with httpx.AsyncClient() as client:
        resp = await client.get(_dapr_url(f"/v1.0/state/{store}/{key}"))
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()


async def invoke_service(app_id: str, method: str, data: dict | None = None) -> dict:
    """Invoke another Dapr service."""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _dapr_url(f"/v1.0/invoke/{app_id}/method/{method}"),
            json=data or {},
        )
        resp.raise_for_status()
        return resp.json()
