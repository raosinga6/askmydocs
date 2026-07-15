"""Approach 1: spark.read.format("binaryFile") + parsing UDF.

Reads each YAML file as a single row (one row per file, guaranteed), then
parses each one with a Python UDF. Using binaryFile instead of text() because
text()'s `wholetext` option is unreliable across Spark versions.

Tradeoff: Python UDFs serialize each row through Python, which is slower than
pure Spark SQL. For 500 small files this is fine.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.spark_session import get_spark

RAW_DIR = Path("/app/data/raw")


FIELD_SCHEMA = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("technical_description", StringType(), nullable=True),
])

CATALOG_SCHEMA = StructType([
    StructField("table_name", StringType(), nullable=True),
    StructField("purpose", StringType(), nullable=True),
    StructField("granularity", StringType(), nullable=True),
    StructField("business_rules", ArrayType(StringType()), nullable=True),
    StructField("input_tables", ArrayType(StringType()), nullable=True),
    StructField("fields", ArrayType(FIELD_SCHEMA), nullable=True),
    StructField("parse_error", StringType(), nullable=True),
])


def parse_yaml_bytes(content: bytes) -> dict:
    """Parse one YAML file's bytes into a struct matching CATALOG_SCHEMA."""
    try:
        text = content.decode("utf-8")
        doc = yaml.safe_load(text) or {}
        return {
            "table_name": doc.get("table_name"),
            "purpose": (doc.get("overview") or {}).get("purpose"),
            "granularity": (doc.get("overview") or {}).get("granularity"),
            "business_rules": (doc.get("overview") or {}).get("business_rules") or [],
            "input_tables": doc.get("input_tables") or [],
            "fields": [
                {
                    "name": f.get("name"),
                    "description": f.get("description"),
                    "source": f.get("source"),
                    "technical_description": f.get("technical_description"),
                }
                for f in (doc.get("fields") or [])
            ],
            "parse_error": None,
        }
    except Exception as e:
        return {
            "table_name": None,
            "purpose": None,
            "granularity": None,
            "business_rules": [],
            "input_tables": [],
            "fields": [],
            "parse_error": f"{type(e).__name__}: {e}",
        }


def main() -> None:
    spark = get_spark("ingest-v1-text-udf")

    # binaryFile gives one row per file with columns: path, modificationTime, length, content (bytes)
    raw = (
        spark.read
        .format("binaryFile")
        .option("pathGlobFilter", "*.yaml")
        .load(str(RAW_DIR))
    )

    print(f"Files read: {raw.count()}")

    parse_udf = F.udf(parse_yaml_bytes, CATALOG_SCHEMA)
    parsed = raw.withColumn("parsed", parse_udf(F.col("content")))

    good = parsed.filter(F.col("parsed.parse_error").isNull()).select(
        F.col("path").alias("file_path"),
        "parsed.*",
    ).drop("parse_error")

    bad = parsed.filter(F.col("parsed.parse_error").isNotNull()).select(
        F.col("path").alias("file_path"),
        "parsed.parse_error",
    )

    good_count = good.count()
    bad_count = bad.count()
    print(f"Parsed OK: {good_count}")
    print(f"Quarantined: {bad_count}")
    if bad_count > 0:
        bad.show(10, truncate=80)

    print("\nSample row:")
    good.select("table_name", "purpose", "input_tables").show(3, truncate=60)

    print("\nSchema:")
    good.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()