"""Feast feature definitions for sensor anomaly detection."""

from datetime import timedelta

from feast import Entity, Feature, FeatureView, FileSource, ValueType

# Entity: individual sensor
sensor = Entity(
    name="sensor_id",
    value_type=ValueType.STRING,
    description="Unique identifier for a sensor device",
)

# Offline source (Parquet files in S3/MinIO)
sensor_readings_source = FileSource(
    path="s3://ml-data/sensor_readings.parquet",
    timestamp_field="timestamp",
    s3_endpoint_override="http://minio:9000",
)

# Feature view: raw sensor statistics
sensor_stats = FeatureView(
    name="sensor_stats",
    entities=[sensor],
    ttl=timedelta(hours=24),
    schema=[
        Feature(name="mean_value", dtype=ValueType.FLOAT),
        Feature(name="std_value", dtype=ValueType.FLOAT),
        Feature(name="min_value", dtype=ValueType.FLOAT),
        Feature(name="max_value", dtype=ValueType.FLOAT),
        Feature(name="reading_count", dtype=ValueType.INT64),
    ],
    source=sensor_readings_source,
    online=True,
)
