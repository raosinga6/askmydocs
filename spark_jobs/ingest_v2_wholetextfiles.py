"""Approach 2: sparkContext.wholeTextFiles + RDD.map + explicit schema.

The older, RDD-based approach. Returns (file_path, content) pairs that we map
through pure Python. Originally we used .toDF() with schema inference, but that
crashes when one row has a shape that contradicts inferences from prior rows
(e.g. business_rules sometimes being [str] and sometimes [dict] when a YAML
has malformed content). Fix: pass an explicit schema to createDataFrame.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.spark_session import get_spark

RAW_DIR = Path("/app/data/raw")


# Explicit schema — same defensive trick as Approach 1, just at the toDF boundary.
ROW_SCHEMA = StructType([
    StructField("file_path", StringType(), nullable=True),
    StructField("table_name", StringType(), nullable=True),
    StructField("purpose", StringType(), nullable=True),
    StructField("granularity", StringType(), nullable=True),
    StructField("business_rules", ArrayType(StringType()), nullable=True),
    StructField("input_tables", ArrayType(StringType()), nullable=True),
    StructField("field_count", IntegerType(), nullable=True),
    StructField("parse_error", StringType(), nullable=True),
])


def parse_pair(pair: tuple[str, str]) -> tuple:
    """Parse one (path, content) pair into a tuple matching ROW_SCHEMA."""
    file_path, content = pair
    try:
        doc = yaml.safe_load(content) or {}
        overview = doc.get("overview") or {}
        rules = overview.get("business_rules") or []
        # Coerce business_rules to a list of strings; drop anything else.
        rules = [r for r in rules if isinstance(r, str)]
        input_tables = [t for t in (doc.get("input_tables") or []) if isinstance(t, str)]
        return (
            file_path,
            doc.get("table_name"),
            overview.get("purpose"),
            overview.get("granularity"),
            rules,
            input_tables,
            len(doc.get("fields") or []),
            None,
        )
    except Exception as e:
        return (
            file_path,
            None,
            None,
            None,
            [],
            [],
            0,
            f"{type(e).__name__}: {e}",
        )


def main() -> None:
    spark = get_spark("ingest-v2-wholetextfiles")
    sc = spark.sparkContext

    rdd = sc.wholeTextFiles(str(RAW_DIR), minPartitions=4)
    print(f"Files read: {rdd.count()}")
    print(f"Default partitions: {rdd.getNumPartitions()}")

    parsed = rdd.map(parse_pair)
    df = spark.createDataFrame(parsed, schema=ROW_SCHEMA)

    good_count = df.filter(df.parse_error.isNull()).count()
    bad_count = df.filter(df.parse_error.isNotNull()).count()
    print(f"Parsed OK: {good_count}")
    print(f"Quarantined: {bad_count}")
    if bad_count > 0:
        df.filter(df.parse_error.isNotNull()).select("file_path", "parse_error").show(10, truncate=80)

    print("\nSample:")
    df.filter(df.parse_error.isNull()).select("table_name", "field_count", "input_tables").show(3, truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()