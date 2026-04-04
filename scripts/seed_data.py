#!/usr/bin/env python3
"""
Seed the ML pipeline with sample sensor data.

Usage:
    uv run python scripts/seed_data.py                  # 10 normal + 2 anomaly
    uv run python scripts/seed_data.py --count 50       # 50 data points
    uv run python scripts/seed_data.py --continuous      # Continuous stream (1/sec)
"""

import argparse
import json
import math
import random
import sys
import time
from datetime import datetime, timezone

import httpx

INGESTION_URL = "http://localhost:8001"
SENSORS = ["sensor-A", "sensor-B", "sensor-C"]


def generate_normal() -> dict:
    """Generate normal sensor reading."""
    sensor = random.choice(SENSORS)
    return {
        "sensor_id": sensor,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "values": {
            "temperature": round(random.gauss(25.0, 2.0), 2),
            "humidity": round(random.gauss(60.0, 5.0), 2),
            "pressure": round(random.gauss(1013.0, 3.0), 2),
            "vibration": round(random.gauss(0.5, 0.1), 3),
        },
        "metadata": {"location": f"zone-{random.randint(1, 3)}"},
    }


def generate_anomaly() -> dict:
    """Generate anomalous sensor reading."""
    sensor = random.choice(SENSORS)
    return {
        "sensor_id": sensor,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "values": {
            "temperature": round(random.gauss(80.0, 5.0), 2),   # way too hot
            "humidity": round(random.gauss(95.0, 2.0), 2),       # way too humid
            "pressure": round(random.gauss(1013.0, 3.0), 2),     # normal
            "vibration": round(random.gauss(3.0, 0.5), 3),       # high vibration
        },
        "metadata": {"location": f"zone-{random.randint(1, 3)}"},
    }


def send(data: dict) -> bool:
    try:
        resp = httpx.post(f"{INGESTION_URL}/ingest", json=data, timeout=5.0)
        ok = resp.status_code == 200
        tag = "ANOMALY" if data["values"]["temperature"] > 50 else "normal"
        symbol = "+" if ok else "x"
        print(f"  [{symbol}] {data['sensor_id']} | {tag:>7} | temp={data['values']['temperature']:.1f}")
        return ok
    except httpx.ConnectError:
        print(f"  [!] Connection refused — is 'make up' running?", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Seed ML pipeline with sample data")
    parser.add_argument("--count", type=int, default=12, help="Number of data points")
    parser.add_argument("--anomaly-rate", type=float, default=0.15, help="Fraction of anomalies")
    parser.add_argument("--continuous", action="store_true", help="Continuous stream (1/sec)")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval for continuous mode")
    args = parser.parse_args()

    print(f"\n--- ML Pipeline Seed Data ---")
    print(f"Target: {INGESTION_URL}")
    print(f"Mode:   {'continuous' if args.continuous else f'{args.count} data points'}\n")

    sent = 0
    try:
        while True:
            is_anomaly = random.random() < args.anomaly_rate
            data = generate_anomaly() if is_anomaly else generate_normal()
            if not send(data):
                if not args.continuous:
                    break
            sent += 1
            if not args.continuous and sent >= args.count:
                break
            if args.continuous:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass

    print(f"\n--- Done: {sent} data points sent ---\n")


if __name__ == "__main__":
    main()
