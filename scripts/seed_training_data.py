#!/usr/bin/env python3
"""
Generate synthetic training data and upload to MinIO (S3) ml-data bucket.

This triggers the Training Workflow Service via MinIO bucket notifications.

Usage:
    uv run python scripts/seed_training_data.py                     # defaults
    uv run python scripts/seed_training_data.py --normal 50000      # fewer samples
    uv run python scripts/seed_training_data.py --prefix training/exp-01/
    uv run python scripts/seed_training_data.py --local-only        # save to /tmp, skip upload
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import boto3
import numpy as np

# Reuse existing generators from the training package
from training.preprocess import generate_anomaly, generate_normal

S3_ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_DATA_BUCKET", "ml-data")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


def build_dataset(
    num_normal: int,
    num_anomaly: int,
    num_features: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
    """Generate train/eval arrays and metadata."""
    normal = generate_normal(num_normal, num_features)
    anomaly = generate_anomaly(num_anomaly, num_features)

    train_data = normal  # autoencoder learns normal patterns only

    eval_data = np.concatenate([normal[:500], anomaly])
    eval_labels = np.concatenate([np.zeros(500), np.ones(len(anomaly))])

    metadata = {
        "num_features": num_features,
        "feature_names": [f"sensor_{i}" for i in range(num_features)],
        "train_samples": num_normal,
        "eval_samples": len(eval_data),
        "anomaly_ratio": round(len(anomaly) / len(eval_data), 4),
    }

    return train_data, eval_data, eval_labels, metadata


def save_local(
    out_dir: Path,
    train_data: np.ndarray,
    eval_data: np.ndarray,
    eval_labels: np.ndarray,
    metadata: dict,
) -> list[Path]:
    """Save arrays and metadata to a local directory. Returns list of file paths."""
    out_dir.mkdir(parents=True, exist_ok=True)

    files = []
    for name, arr in [("train.npy", train_data), ("eval_data.npy", eval_data), ("eval_labels.npy", eval_labels)]:
        path = out_dir / name
        np.save(path, arr)
        files.append(path)

    meta_path = out_dir / "metadata.json"
    meta_path.write_text(json.dumps(metadata, indent=2))
    files.append(meta_path)

    return files


def upload_to_s3(files: list[Path], prefix: str, endpoint: str, bucket: str) -> None:
    """Upload files to S3/MinIO."""
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
    )

    for path in files:
        key = f"{prefix}{path.name}"
        s3.upload_file(str(path), bucket, key)
        print(f"  [+] s3://{bucket}/{key}  ({path.stat().st_size:,} bytes)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate training data and upload to MinIO/S3")
    parser.add_argument("--normal", type=int, default=100_000, help="Number of normal samples")
    parser.add_argument("--anomaly", type=int, default=200, help="Number of anomaly samples")
    parser.add_argument("--features", type=int, default=8, help="Number of features")
    parser.add_argument("--prefix", type=str, default="training/", help="S3 key prefix")
    parser.add_argument("--bucket", type=str, default=S3_BUCKET, help="S3 bucket name")
    parser.add_argument("--endpoint", type=str, default=S3_ENDPOINT, help="S3 endpoint URL")
    parser.add_argument("--local-only", action="store_true", help="Save locally only, skip S3 upload")
    parser.add_argument("--out-dir", type=str, default=None, help="Local output directory (default: temp dir)")
    args = parser.parse_args()

    print("\n--- Seed Training Data ---")
    print(f"  Normal samples:  {args.normal:,}")
    print(f"  Anomaly samples: {args.anomaly:,}")
    print(f"  Features:        {args.features}")
    if not args.local_only:
        print(f"  S3 endpoint:     {args.endpoint}")
        print(f"  S3 bucket:       {args.bucket}")
        print(f"  S3 prefix:       {args.prefix}")
    print()

    # Generate
    print("Generating data...")
    train_data, eval_data, eval_labels, metadata = build_dataset(
        args.normal,
        args.anomaly,
        args.features,
    )
    print(f"  train:  {train_data.shape}")
    print(f"  eval:   {eval_data.shape}")
    print()

    # Save locally
    if args.out_dir:
        out_dir = Path(args.out_dir)
    elif args.local_only:
        out_dir = Path("/tmp/ml-data")
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="ml-seed-"))

    files = save_local(out_dir, train_data, eval_data, eval_labels, metadata)
    print(f"Saved to {out_dir}/")
    for f in files:
        print(f"  {f.name}  ({f.stat().st_size:,} bytes)")
    print()

    # Upload to S3
    if not args.local_only:
        print(f"Uploading to s3://{args.bucket}/{args.prefix} ...")
        try:
            upload_to_s3(files, args.prefix, args.endpoint, args.bucket)
            print("\nDone! Files uploaded. Training should trigger automatically")
            print("via MinIO bucket notification → training-data-events topic.\n")
        except Exception as e:
            print(f"\n  [!] Upload failed: {e}", file=sys.stderr)
            print("  Is MinIO running? Try 'make up-infra' first.\n", file=sys.stderr)
            sys.exit(1)
    else:
        print("Done! (local-only mode, no S3 upload)\n")


if __name__ == "__main__":
    main()
