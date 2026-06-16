from __future__ import annotations

from functools import lru_cache

from pyspark.sql import SparkSession

from lemonade_backend.config import get_settings


@lru_cache
def get_spark_session(app_name: str = "lemonade-backend") -> SparkSession:
    settings = get_settings()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    if settings.spark_mode == "local":
        builder = builder.master(settings.spark_master)
    else:
        builder = builder.master("k8s://https://kubernetes.default.svc")

    return builder.getOrCreate()


def stop_spark() -> None:
    try:
        spark = SparkSession.getActiveSession()
        if spark:
            spark.stop()
    except Exception:
        pass
