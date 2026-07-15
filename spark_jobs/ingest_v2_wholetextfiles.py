"""Approach 2: sparkContext.wholeTextFiles + RDD.map.

This is the older RDD-based approach. It returns an RDD of (file_path, content)
pairs, which we map through pure Python before converting to a DataFrame.

Differences from Approach 1:
- Operates on RDDs, not DataFrames — no schema inference until toDF()
- Each partition holds many files in memory at once (small-file aggregation)
- Lower-level: you control partition count directly
- Less integration with the Catalyst optimizer

We're keeping both around so Day 6 can benchmark them on a 10x dataset.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import Row

from spark_jobs.spark_session import get_spark

REPO = Path(__file__).resolve().parent.parent
RAW_DIR = REPO / "data" / "raw"


def parse_pair(pair: tuple[str, str]) -> Row:
    file_path, content = pair
    try:
        doc = yaml.safe_load(content)
        return Row(
            file_path=file_path,
            table_name=doc.get("table_name"),
            purpose=doc.get("overview", {}).get("purpose"),
            granularity=doc.get("overview", {}).get("granularity"),
            business_rules=doc.get("overview", {}).get("business_rules") or [],
            input_tables=doc.get("input_tables") or [],
            field_count=len(doc.get("fields") or []),
            parse_error=None,
        )
    except Exception as e:
        return Row(
            file_path=file_path,
            table_name=None,
            purpose=None,
            granularity=None,
            business_rules=[],
            input_tables=[],
            field_count=0,
            parse_error=f"{type(e).__name__}: {e}",
        )


def main() -> None:
    spark = get_spark("ingest-v2-wholetextfiles")
    sc = spark.sparkContext

    # wholeTextFiles returns an RDD[(path, content)].
    # minPartitions controls how files are bundled — relevant for the small-file problem.
    rdd = sc.wholeTextFiles(str(RAW_DIR), minPartitions=4)

    print(f"Files read: {rdd.count()}")
    print(f"Default partitions: {rdd.getNumPartitions()}")

    parsed = rdd.map(parse_pair)
    df = parsed.toDF()

    good_count = df.filter(df.parse_error.isNull()).count()
    bad_count = df.filter(df.parse_error.isNotNull()).count()
    print(f"Parsed OK: {good_count}")
    print(f"Quarantined: {bad_count}")

    print("\nSample:")
    df.select("table_name", "field_count", "input_tables").show(3, truncate=60)

    spark.stop()


if __name__ == "__main__":
    main()