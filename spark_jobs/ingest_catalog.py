"""Production ingestion: read YAMLs, validate, write Parquet + quarantine.

This is the canonical ingestion path. It supersedes Day 2's two experimental
scripts. See dq_contract.md for the data quality rules implemented here.

Outputs:
- /app/data/parquet/catalog/      — clean, validated rows
- /app/data/parquet/quarantine/   — rejected rows with rejection reasons
- /app/data/parquet/dq_report/    — aggregate DQ stats for this run

Raises DQContractViolation if aggregate thresholds breach.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.spark_session import get_spark

import os

RAW_DIR = Path(os.environ.get("ASKMYDOCS_RAW_DIR", "/app/data/raw"))
OUT_DIR = Path(os.environ.get("ASKMYDOCS_OUT_DIR", "/app/data/parquet"))
CATALOG_OUT = OUT_DIR / "catalog"
QUARANTINE_OUT = OUT_DIR / "quarantine"
DQ_REPORT_OUT = OUT_DIR / "dq_report"

def _coerce_to_string_list(value) -> list[str]:
    """Defensively convert a YAML value to a list of strings.
    
    Some YAMLs (from real masking processes) have `input_tables: foo.bar`
    instead of `input_tables: [foo.bar]`. PyYAML returns this as a string,
    not a list. We treat a bare string as a single-element list.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str)]
    return []

class DQContractViolation(RuntimeError):
    """Raised when aggregate DQ thresholds are breached. Halts the job."""


# Per-row schema after parsing — every field nullable so we can quarantine
# anything, not crash.
FIELD_SCHEMA = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("description", StringType(), nullable=True),
    StructField("source", StringType(), nullable=True),
    StructField("technical_description", StringType(), nullable=True),
])

PARSED_SCHEMA = StructType([
    StructField("table_name", StringType(), nullable=True),
    StructField("purpose", StringType(), nullable=True),
    StructField("granularity", StringType(), nullable=True),
    StructField("business_rules", ArrayType(StringType()), nullable=True),
    StructField("input_tables", ArrayType(StringType()), nullable=True),
    StructField("fields", ArrayType(FIELD_SCHEMA), nullable=True),
    StructField("parse_error", StringType(), nullable=True),
])


def parse_yaml_bytes(content: bytes) -> dict:
    """Parse YAML bytes into a struct matching PARSED_SCHEMA."""
    try:
        text = content.decode("utf-8")
        doc = yaml.safe_load(text) or {}
        return {
            "table_name": doc.get("table_name"),
            "purpose": (doc.get("overview") or {}).get("purpose"),
            "granularity": (doc.get("overview") or {}).get("granularity"),
            "business_rules": [
                r for r in ((doc.get("overview") or {}).get("business_rules") or [])
                if isinstance(r, str)
            ],
            "input_tables": _coerce_to_string_list(doc.get("input_tables")),
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
            "table_name": None,
            "purpose": None,
            "granularity": None,
            "business_rules": [],
            "input_tables": [],
            "fields": [],
            "parse_error": f"{type(e).__name__}: {e}",
        }


def validate_row(df: DataFrame) -> DataFrame:
    """Apply per-row DQ rules. Adds a `rejection_reason` column (null = good)."""
    table_name_re = r"^[a-z][a-z0-9_]*$"
    # input_table_re = r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$"
    input_table_re = r"^[a-z0-9][a-z0-9_]*\.[a-z0-9][a-z0-9_]*$"

    # field_names is an array we'll use for duplicate detection.
    df = df.withColumn(
        "field_names",
        F.expr("transform(fields, f -> f.name)"),
    )

    # Build the rejection_reason via CASE WHEN. First matching condition wins.
    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("parse_error").isNotNull(), F.concat(F.lit("parse_error: "), F.col("parse_error")))
        .when(F.col("table_name").isNull(), F.lit("missing table_name"))
        .when(~F.col("table_name").rlike(table_name_re), F.lit("malformed table_name"))
        .when(F.col("purpose").isNull() | (F.length("purpose") < 40), F.lit("purpose null or too short"))
        .when(F.col("granularity").isNull() | (F.length("granularity") < 20), F.lit("granularity null or too short"))
        .when(F.col("input_tables").isNull() | (F.size("input_tables") == 0), F.lit("input_tables empty"))
        .when(F.col("fields").isNull() | (F.size("fields") == 0), F.lit("fields empty"))
        .when(F.expr("exists(field_names, n -> n is null)"), F.lit("field with null name"))
        .when(F.size("field_names") != F.size(F.array_distinct("field_names")), F.lit("duplicate field names"))
        .when(
            F.expr(f"exists(input_tables, t -> not t rlike '{input_table_re}')"),
            F.lit("malformed input_tables entry"),
        )
        .otherwise(F.lit(None).cast("string")),
    )

    return df


def write_dq_report(spark: SparkSession, parsed: DataFrame, file_count: int) -> dict:
    """Compute and write the DQ aggregate report."""
    good_count = parsed.filter(F.col("rejection_reason").isNull()).count()
    bad_count = parsed.filter(F.col("rejection_reason").isNotNull()).count()

    rejection_breakdown = (
        parsed.filter(F.col("rejection_reason").isNotNull())
        .groupBy("rejection_reason")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )

    distinct_domains = parsed.filter(F.col("rejection_reason").isNull()).select(
        F.explode("input_tables").alias("it")
    ).select(
        F.split("it", "\\.").getItem(0).alias("namespace")
    ).distinct().count()

    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "file_count": file_count,
        "good_count": good_count,
        "bad_count": bad_count,
        "rejection_rate": bad_count / max(file_count, 1),
        "rejection_breakdown": [
            {"reason": r["rejection_reason"], "count": r["count"]}
            for r in rejection_breakdown
        ],
        "distinct_input_namespaces": distinct_domains,
    }

    # Write JSON for human inspection.
    DQ_REPORT_OUT.mkdir(parents=True, exist_ok=True)
    (DQ_REPORT_OUT / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def enforce_dq_contract(report: dict) -> None:
    """Apply the aggregate-level halt rules from dq_contract.md."""
    violations: list[str] = []

    if report["file_count"] < 400:
        violations.append(f"file_count {report['file_count']} < 400")

    if report["rejection_rate"] > 0.05:
        violations.append(f"rejection_rate {report['rejection_rate']:.2%} > 5%")

    if report["rejection_breakdown"]:
        top_reason = report["rejection_breakdown"][0]
        if top_reason["count"] > 0.5 * report["bad_count"] and report["bad_count"] >= 5:
            violations.append(
                f"single rejection reason '{top_reason['reason']}' accounts for "
                f"{top_reason['count']}/{report['bad_count']} rejections (>50%)"
            )

    if report["distinct_input_namespaces"] < 2:
        violations.append(
            f"distinct input namespaces {report['distinct_input_namespaces']} < 2 (catalog too narrow)"
        )

    if violations:
        raise DQContractViolation(
            "DQ contract violated:\n  - " + "\n  - ".join(violations)
        )


def main() -> None:
    spark = get_spark("ingest-catalog")

    # 1. Read raw files.
    raw = (
        spark.read
        .format("binaryFile")
        .option("pathGlobFilter", "*.yaml")
        .load(str(RAW_DIR))
    )
    file_count = raw.count()
    print(f"Files read: {file_count}")

    # 2. Parse.
    parse_udf = F.udf(parse_yaml_bytes, PARSED_SCHEMA)
    parsed = (
        raw.withColumn("parsed", parse_udf(F.col("content")))
        .select(F.col("path").alias("file_path"), "parsed.*")
    )

    # 3. Validate.
    validated = validate_row(parsed).cache()  # cache because we read it multiple times

    # 4. Write DQ report (before halting, so we can debug if it halts).
    report = write_dq_report(spark, validated, file_count)
    print(f"\nDQ report: {json.dumps(report, indent=2)}")

    # 5. Enforce aggregate DQ. Raises before writing Parquet if breached.
    enforce_dq_contract(report)

    # 6. Split and write.
    good = validated.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "field_names", "parse_error")
    bad = validated.filter(F.col("rejection_reason").isNotNull()).select(
        "file_path", "rejection_reason", "parse_error"
    )

    good.write.mode("overwrite").parquet(str(CATALOG_OUT))
    bad.write.mode("overwrite").parquet(str(QUARANTINE_OUT))

    print(f"\nWrote {good.count()} good rows to {CATALOG_OUT}")
    print(f"Wrote {bad.count()} quarantined rows to {QUARANTINE_OUT}")
    print(f"DQ report at {DQ_REPORT_OUT / 'report.json'}")

    spark.stop()


if __name__ == "__main__":
    main()