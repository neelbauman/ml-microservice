#!/usr/bin/env python3
"""
Watch the pipeline in real-time by polling service states.

Usage:
    uv run python scripts/watch_pipeline.py
"""

import time

import httpx


def poll():
    """Poll all services for their latest state."""
    sensors = ["sensor-A", "sensor-B", "sensor-C"]
    print(f"\033[2J\033[H")  # clear screen
    print("=== ML Pipeline Monitor ===\n")
    print(f"{'Sensor':<12} {'Score':>8} {'Anomaly':>8} {'Confidence':>10} {'Model':>12}")
    print("-" * 56)

    for sid in sensors:
        try:
            resp = httpx.get(f"http://localhost:8003/results/{sid}", timeout=2.0)
            if resp.status_code == 200:
                r = resp.json()
                if "error" not in r:
                    anomaly = "YES" if r.get("is_anomaly") else "no"
                    color = "\033[91m" if r.get("is_anomaly") else "\033[92m"
                    print(f"{sid:<12} {color}{r.get('prediction', 0):>8.4f}\033[0m"
                          f" {anomaly:>8} {r.get('confidence', 0):>10.4f}"
                          f" {r.get('model_version', ''):>12}")
                else:
                    print(f"{sid:<12} {'—':>8} {'—':>8} {'—':>10} {'no data':>12}")
        except Exception:
            print(f"{sid:<12} {'err':>8}")

    # Alert stats
    try:
        resp = httpx.get("http://localhost:8004/alerts/stats", timeout=2.0)
        if resp.status_code == 200:
            s = resp.json()
            print(f"\nAlerts: {s['total']} total, {s['critical']} critical, {s['warning']} warning")
    except Exception:
        pass

    print(f"\nRefreshing every 2s... (Ctrl+C to stop)")


def main():
    try:
        while True:
            poll()
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
