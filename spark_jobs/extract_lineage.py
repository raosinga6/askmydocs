"""Extract table-level and field-level lineage from the validated catalog.

Inputs:
- /app/data/parquet/catalog/  (from ingest_catalog.py)

Outputs:
- /app/data/parquet/table_lineage/
- /app/data/parquet/field_lineage/

See LINEAGE_SCHEMAS.md for output contract.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.lineage_parsers import (
    classify_source_kind,
    parse_input_table,
    parse_source_string,
)
from spark_jobs.spark_session import get_spark

import os
CATALOG_IN = Path(os.environ.get("ASKMYDOCS_CATALOG_IN", "/app/data/parquet/catalog"))
TABLE_LINEAGE_OUT = Path(os.environ.get("ASKMYDOCS_TABLE_LINEAGE_OUT", "/app/data/parquet/table_lineage"))
FIELD_LINEAGE_OUT = Path(os.environ.get("ASKMYDOCS_FIELD_LINEAGE_OUT", "/app/data/parquet/field_lineage"))


# UDF return schemas.
INPUT_TABLE_SCHEMA = StructType([
    StructField("namespace", StringType(), nullable=True),
    StructField("table", StringType(), nullable=True),
])

SOURCE_LIST_SCHEMA = ArrayType(StructType([
    StructField("namespace", StringType(), nullable=False),
    StructField("table", StringType(), nullable=False),
    StructField("column", StringType(), nullable=True),
]))


def parse_input_table_udf(token: str) -> dict | None:
    parsed = parse_input_table(token)
    if parsed is None:
        return None
    return {"namespace": parsed[0], "table": parsed[1]}


def parse_source_list_udf(source: str) -> list[dict]:
    return [
        {"namespace": p.namespace, "table": p.table, "column": p.column}
        for p in parse_source_string(source)
    ]


def extract_table_lineage(catalog: DataFrame) -> DataFrame:
    """Build table_lineage from catalog.input_tables.

    One row per (downstream_table, upstream_table) edge. Drops self-references
    and invalid input_tables entries.
    """
    udf = F.udf(parse_input_table_udf, INPUT_TABLE_SCHEMA)

    exploded = (
        catalog.select(
            "table_name",
            F.explode("input_tables").alias("input_table_raw"),
        )
        .withColumn("parsed", udf(F.col("input_table_raw")))
        .filter(F.col("parsed").isNotNull())
        .select(
            F.col("table_name").alias("downstream_table"),
            F.col("parsed.namespace").alias("upstream_namespace"),
            F.col("parsed.table").alias("upstream_table"),
        )
        .withColumn(
            "upstream_fqn",
            F.concat_ws(".", F.col("upstream_namespace"), F.col("upstream_table")),
        )
        .filter(F.col("downstream_table") != F.col("upstream_table"))  # drop self-refs
        .dropDuplicates(["downstream_table", "upstream_fqn"])
    )

    return exploded


def extract_field_lineage(catalog: DataFrame) -> DataFrame:
    """Build field_lineage from catalog.fields[].source.

    One row per (downstream_table, downstream_field, upstream_namespace,
    upstream_table, upstream_column).
    """
    list_udf = F.udf(parse_source_list_udf, SOURCE_LIST_SCHEMA)
    kind_udf = F.udf(classify_source_kind, StringType())

    fields_exploded = (
        catalog.select(
            "table_name",
            F.explode("fields").alias("field"),
        )
        .select(
            F.col("table_name").alias("downstream_table"),
            F.col("field.name").alias("downstream_field"),
            F.col("field.source").alias("source_string"),
            F.col("field.technical_description").alias("technical_description"),
        )
        .filter(F.col("downstream_field").isNotNull())
    )

    sources_exploded = (
        fields_exploded
        .withColumn("sources", list_udf(F.col("source_string")))
        .withColumn("source_kind", kind_udf(F.col("technical_description")))
        .filter(F.size("sources") > 0)
        .select(
            "downstream_table",
            "downstream_field",
            "source_kind",
            F.explode("sources").alias("src"),
        )
        .select(
            "downstream_table",
            "downstream_field",
            "source_kind",
            F.col("src.namespace").alias("upstream_namespace"),
            F.col("src.table").alias("upstream_table"),
            F.col("src.column").alias("upstream_column"),
        )
        .withColumn(
            "upstream_fqn",
            F.when(
                F.col("upstream_column").isNotNull(),
                F.concat_ws(".", F.col("upstream_namespace"), F.col("upstream_table"), F.col("upstream_column")),
            ).otherwise(
                F.concat_ws(".", F.col("upstream_namespace"), F.col("upstream_table")),
            ),
        )
        .filter(F.col("downstream_table") != F.col("upstream_table"))  # drop self-refs
        .dropDuplicates([
            "downstream_table", "downstream_field",
            "upstream_namespace", "upstream_table", "upstream_column",
        ])
    )

    return sources_exploded


def report_stats(table_lineage: DataFrame, field_lineage: DataFrame, catalog: DataFrame) -> None:
    """Print summary stats so we can sanity-check the extraction."""
    print("\n=== Lineage extraction stats ===")
    print(f"Catalog tables:        {catalog.count()}")
    print(f"Table lineage edges:   {table_lineage.count()}")
    print(f"Field lineage edges:   {field_lineage.count()}")

    print("\nTop 5 downstream tables by upstream count:")
    table_lineage.groupBy("downstream_table").count().orderBy(F.desc("count")).show(5, truncate=60)

    print("Top 5 most-referenced upstream tables:")
    table_lineage.groupBy("upstream_fqn").count().orderBy(F.desc("count")).show(5, truncate=80)

    print("Source kind distribution:")
    field_lineage.groupBy("source_kind").count().orderBy(F.desc("count")).show(truncate=False)

    print("Sample field lineage:")
    field_lineage.select(
        "downstream_table", "downstream_field", "upstream_fqn", "source_kind"
    ).show(10, truncate=60)


def main() -> None:
    spark = get_spark("extract-lineage")

    catalog = spark.read.parquet(str(CATALOG_IN)).cache()
    print(f"Loaded catalog: {catalog.count()} tables")

    table_lineage = extract_table_lineage(catalog).cache()
    field_lineage = extract_field_lineage(catalog).cache()

    table_lineage.write.mode("overwrite").parquet(str(TABLE_LINEAGE_OUT))
    field_lineage.write.mode("overwrite").parquet(str(FIELD_LINEAGE_OUT))

    report_stats(table_lineage, field_lineage, catalog)

    print(f"\nWrote table_lineage to {TABLE_LINEAGE_OUT}")
    print(f"Wrote field_lineage to {FIELD_LINEAGE_OUT}")

    spark.stop()


if __name__ == "__main__":
    main()