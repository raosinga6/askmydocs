"""Approach 1: spark.read.text + parsing UDF.

Reads all YAMLs as text using Spark's distributed file reader, then parses
each one with a Python UDF. This is the approach we'll keep — it lets Spark
manage partitions and parallelism, which matters when we move to GCS later.

Tradeoff: Python UDFs serialize each row through Python, which is slower than
pure Spark SQL. For 500 small files this is fine. We'll discuss alternatives
in the writeup.
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

REPO = Path(__file__).resolve().parent.parent
RAW_DIR = REPO / "data" / "raw"


# Explicit schema for what one parsed YAML looks like.
# We define this upfront instead of letting Spark infer because:
# 1. Inference is slow and unreliable on nested data
# 2. An explicit schema is the contract — if a YAML doesn't fit, we know
FIELD_SCHEMA = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("description", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("technical_description", StringType(), nullable=True),
])

CATALOG_SCHEMA = StructType([
    StructField("table_name", StringType(), nullable=False),
    StructField("purpose", StringType(), nullable=True),
    StructField("granularity", StringType(), nullable=True),
    StructField("business_rules", ArrayType(StringType()), nullable=True),
    StructField("input_tables", ArrayType(StringType()), nullable=True),
    StructField("fields", ArrayType(FIELD_SCHEMA), nullable=True),
    StructField("parse_error", StringType(), nullable=True),
])


def parse_yaml(content: str) -> dict:
    """Parse one YAML string into a flat dict matching CATALOG_SCHEMA.

    Returns a dict with parse_error populated if parsing fails. We never raise
    from a UDF — failed rows are filtered downstream into quarantine.
    """
    try:
        doc = yaml.safe_load(content)
        return {
            "table_name": doc.get("table_name"),
            "purpose": doc.get("overview", {}).get("purpose"),
            "granularity": doc.get("overview", {}).get("granularity"),
            "business_rules": doc.get("overview", {}).get("business_rules") or [],
            "input_tables": doc.get("input_tables") or [],
            "fields": [
                {
                    "name": f.get("name"),
                    "description": f.get("description"),
                    "source": f.get("source"),
                    "technical_description": f.get("technical_description"),
                }
                for f in doc.get("fields") or []
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

    # spark.read.text reads each line as a row. We need each FILE as a row,
    # so we use wholetext=True (Spark 3.5+).
    raw = (
        spark.read
        .option("wholetext", "true")
        .text(str(RAW_DIR))
        .withColumn("file_path", F.input_file_name())
    )

    print(f"Files read: {raw.count()}")

    # Register the UDF that turns YAML text into a struct.
    parse_udf = F.udf(parse_yaml, CATALOG_SCHEMA)

    parsed = raw.withColumn("parsed", parse_udf(F.col("value")))

    # Split into good rows and quarantine.
    good = parsed.filter(F.col("parsed.parse_error").isNull()).select("file_path", "parsed.*").drop("parse_error")
    bad = parsed.filter(F.col("parsed.parse_error").isNotNull()).select("file_path", "parsed.parse_error")

    good_count = good.count()
    bad_count = bad.count()
    print(f"Parsed OK: {good_count}")
    print(f"Quarantined: {bad_count}")
    if bad_count > 0:
        bad.show(10, truncate=80)

    # Show a sample of the good ones so you can see the shape.
    print("\nSample row:")
    good.select("table_name", "purpose", "input_tables").show(3, truncate=60)

    print("\nSchema:")
    good.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()