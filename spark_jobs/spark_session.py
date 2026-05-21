"""Spark session factory.

Locally we run inside a Docker container (apache/spark-py:v3.5.3) where Spark,
Java, and PySpark are pre-installed. The container mounts the project at /app.

Production runs on GKE via Spark Operator (Week 3 Day 5), where session config
comes from the SparkApplication CRD.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "askmydocs-local") -> SparkSession:
    """Return a Spark session for single-container development."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")  # 4 cores inside the container
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        # Critical for Docker: bind to all interfaces so the Spark UI is
        # reachable from the host machine via the published port.
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = get_spark()
    print(f"Spark version: {spark.version}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")
    # Sleep so the UI stays alive long enough for you to open it.
    import time
    print("\nUI is up. Press Ctrl+C when you're done browsing.")
    try:
        time.sleep(300)
    except KeyboardInterrupt:
        pass
    spark.stop()