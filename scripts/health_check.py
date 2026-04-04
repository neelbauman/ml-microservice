#!/usr/bin/env python3
"""
Health check for all pipeline services and Dapr components.

Usage:
    uv run python scripts/health_check.py
"""

import subprocess
import sys

import httpx

CHECKS = [
    ("Ingestion",     "http://localhost:8001/health"),
    ("Preprocessing", "http://localhost:8002/health"),
    ("Inference",     "http://localhost:8003/health"),
    ("Alert",         "http://localhost:8004/health"),
    ("MLflow",        "http://localhost:5001/health"),
    ("MinIO",         "http://localhost:9000/minio/health/live"),
    ("Redpanda",      "http://localhost:19644/v1/status/ready"),
]

# Dapr sidecars share the app container's network namespace,
# so we check port 3500 from inside the app container.
DAPR_CHECKS = [
    ("Ingestion Dapr",     "ingestion",     "http://localhost:3500/v1.0/healthz"),
    ("Preprocessing Dapr", "preprocessing", "http://localhost:3500/v1.0/healthz"),
    ("Inference Dapr",     "inference",     "http://localhost:3500/v1.0/healthz"),
    ("Alert Dapr",         "alert",         "http://localhost:3500/v1.0/healthz"),
]


def check(name: str, url: str) -> bool:
    try:
        resp = httpx.get(url, timeout=3.0)
        ok = resp.status_code < 400
        print(f"  {'[OK]' if ok else '[NG]':>6}  {name:<22} {url}")
        return ok
    except httpx.ConnectError:
        print(f"  {'[NG]':>6}  {name:<22} Connection refused")
        return False
    except Exception as e:
        print(f"  {'[NG]':>6}  {name:<22} {e}")
        return False


def check_dapr(name: str, service: str, url: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", service,
             "python", "-c",
             f"import urllib.request; r = urllib.request.urlopen('{url}', timeout=3); "
             f"exit(0 if r.status < 400 else 1)"],
            capture_output=True, timeout=10,
        )
        ok = result.returncode == 0
        print(f"  {'[OK]' if ok else '[NG]':>6}  {name:<22} {url}")
        return ok
    except Exception as e:
        print(f"  {'[NG]':>6}  {name:<22} {e}")
        return False


def main():
    print("\n--- Services ---")
    svc_ok = sum(check(n, u) for n, u in CHECKS)

    print("\n--- Dapr Sidecars ---")
    dapr_ok = sum(check_dapr(n, s, u) for n, s, u in DAPR_CHECKS)

    total = len(CHECKS) + len(DAPR_CHECKS)
    passed = svc_ok + dapr_ok
    print(f"\n--- Result: {passed}/{total} healthy ---\n")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()
