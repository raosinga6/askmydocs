"""Run the full ingest → lineage → embedding-input pipeline against the 10x
dataset with NO optimizations. Time every stage. Save results to JSON.

This is the "before" measurement. Don't change defaults here. The point is to
have an honest baseline.
"""
from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

import yaml

from spark_jobs.spark_session import get_spark

RAW_DIR = Path("/app/data/raw_10x")
OUT_DIR = Path("/app/data/parquet_10x_baseline")
RESULTS_PATH = REPO_RESULTS = Path("/app/data/perf_results_baseline.json")


# Reproduce inline the schemas + UDFs so the baseline is self-contained.
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
    try:
        doc = yaml.safe_load(content.decode("utf-8")) or {}
        return {
            "table_name": doc.get("table_name"),
            "purpose": (doc.get("overview") or {}).get("purpose"),
            "granularity": (doc.get("overview") or {}).get("granularity"),
            "business_rules": [
                r for r in ((doc.get("overview") or {}).get("business_rules") or [])
                if isinstance(r, str)
            ],
            "input_tables": [
                t for t in (doc.get("input_tables") or []) if isinstance(t, str)
            ],
            "fields": [
                {
                    "name": f.get("name") if isinstance(f, dict) else None,
                    "description": f.get("description") if isinstance(f, dict) else None,
                    "source": f.get("source") if isinstance(f, dict) else None,
                    "technical_description": f.get("technical_description") if isinstance(f, dict) else None,
                }
                for f in (doc.get("fields") or [])
            ],
            "parse_error": None,
        }
    except Exception as e:
        return {
            "table_name": None, "purpose": None, "granularity": None,
            "business_rules": [], "input_tables": [], "fields": [],
            "parse_error": f"{type(e).__name__}: {e}",
        }


@contextmanager
def timed(name: str, results: dict):
    t0 = time.perf_counter()
    yield
    elapsed = time.perf_counter() - t0
    results[name] = round(elapsed, 2)
    print(f"  [{elapsed:6.2f}s] {name}")


def main() -> None:
    results: dict = {"variant": "baseline", "file_count": None}
    spark = get_spark("perf-baseline")

    print(f"Reading from: {RAW_DIR}")
    print(f"Writing to:   {OUT_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with timed("01_read_files", results):
        raw = (
            spark.read
            .format("binaryFile")
            .option("pathGlobFilter", "*.yaml")
            .load(str(RAW_DIR))
        )
        file_count = raw.count()
        results["file_count"] = file_count
        results["default_partitions_after_read"] = raw.rdd.getNumPartitions()
        print(f"    files={file_count}  partitions={raw.rdd.getNumPartitions()}")

    with timed("02_parse_yaml", results):
        parse_udf = F.udf(parse_yaml_bytes, CATALOG_SCHEMA)
        parsed = (
            raw.withColumn("parsed", parse_udf(F.col("content")))
               .select(F.col("path").alias("file_path"), "parsed.*")
        )
        good_count = parsed.filter(F.col("parse_error").isNull()).count()
        results["good_count"] = good_count
        print(f"    parsed_ok={good_count}")

    with timed("03_write_catalog_parquet", results):
        good = parsed.filter(F.col("parse_error").isNull()).drop("parse_error")
        good.write.mode("overwrite").parquet(str(OUT_DIR / "catalog"))
        # Count output files — this is the small-file problem made visible.
        output_files = list((OUT_DIR / "catalog").glob("part-*.parquet"))
        results["catalog_output_file_count"] = len(output_files)
        if output_files:
            sizes = [p.stat().st_size for p in output_files]
            results["catalog_avg_file_kb"] = round(sum(sizes) / len(sizes) / 1024, 1)
        print(f"    output_files={len(output_files)}")

    with timed("04_extract_table_lineage", results):
        catalog = spark.read.parquet(str(OUT_DIR / "catalog"))
        exploded = (
            catalog.select(
                "table_name",
                F.explode("input_tables").alias("input_table_raw"),
            )
            .filter(F.col("input_table_raw").rlike(r"^[a-z0-9][a-z0-9_]*\.[a-z0-9][a-z0-9_]*$"))
            .withColumn("upstream_namespace", F.split("input_table_raw", "\\.").getItem(0))
            .withColumn("upstream_table", F.split("input_table_raw", "\\.").getItem(1))
            .select(
                F.col("table_name").alias("downstream_table"),
                "upstream_namespace",
                "upstream_table",
            )
            .filter(F.col("downstream_table") != F.col("upstream_table"))
            .dropDuplicates(["downstream_table", "upstream_namespace", "upstream_table"])
        )
        exploded.write.mode("overwrite").parquet(str(OUT_DIR / "table_lineage"))
        results["table_lineage_count"] = exploded.count()
        print(f"    edges={results['table_lineage_count']}")

    results["total_seconds"] = round(sum(v for k, v in results.items() if k.startswith("0")), 2)
    print(f"\nTotal pipeline time: {results['total_seconds']}s")

    RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"Results saved to {RESULTS_PATH}")

    print(f"\nSpark UI: {spark.sparkContext.uiWebUrl}")
    print("UI is up. Ctrl+C when done.")
    import time as t
    try:
        t.sleep(600)
    except KeyboardInterrupt:
        pass
    spark.stop()


if __name__ == "__main__":
    main()