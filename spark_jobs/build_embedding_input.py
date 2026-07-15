"""Build the embedding_input dataset from catalog + lineage.

Inputs:
- /app/data/parquet/catalog/         (Day 3 output)
- /app/data/parquet/table_lineage/   (Day 4 output)

Output:
- /app/data/parquet/embedding_input/

See EMBEDDING_INPUT.md for the output schema contract.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.embedding_input import (
    FieldInfo,
    TableInfo,
    build_metadata,
    compose_text_blob,
    infer_business_domain,
)
from spark_jobs.spark_session import get_spark
import os 

CATALOG_IN = Path(os.environ.get("ASKMYDOCS_CATALOG_IN", "/app/data/parquet/catalog"))
LINEAGE_IN = Path(os.environ.get("ASKMYDOCS_TABLE_LINEAGE_IN", "/app/data/parquet/table_lineage"))
OUT_DIR = Path(os.environ.get("ASKMYDOCS_EMBEDDING_OUT", "/app/data/parquet/embedding_input"))


METADATA_SCHEMA = StructType([
    StructField("input_table_count", IntegerType(), nullable=False),
    StructField("field_count", IntegerType(), nullable=False),
    StructField("downstream_count", IntegerType(), nullable=False),
    StructField("pii_field_names", ArrayType(StringType()), nullable=False),
    StructField("primary_upstream_namespace", StringType(), nullable=False),
])


def join_lineage_to_catalog(catalog: DataFrame, table_lineage: DataFrame) -> DataFrame:
    """Add upstream_tables_lineage and downstream_tables columns to catalog.

    `upstream_tables_lineage` is from table_lineage (deduped, resolved FQNs).
    Different from `input_tables` because table_lineage is normalized.
    """
    upstream_agg = (
        table_lineage.groupBy("downstream_table")
        .agg(F.collect_set("upstream_fqn").alias("upstream_fqns_from_lineage"))
        .withColumnRenamed("downstream_table", "_dt_up")
    )

    downstream_agg = (
        table_lineage.groupBy("upstream_table")
        .agg(F.collect_set("downstream_table").alias("downstream_tables"))
        .withColumnRenamed("upstream_table", "_ut_down")
    )

    return (
        catalog
        .join(upstream_agg, catalog.table_name == upstream_agg._dt_up, "left")
        .join(downstream_agg, catalog.table_name == downstream_agg._ut_down, "left")
        .drop("_dt_up", "_ut_down")
        .withColumn("upstream_fqns_from_lineage",
                    F.coalesce(F.col("upstream_fqns_from_lineage"), F.array()))
        .withColumn("downstream_tables",
                    F.coalesce(F.col("downstream_tables"), F.array()))
    )


def build_text_blob_udf(table_name, purpose, granularity, business_rules,
                       fields, upstream_namespaces, upstream_tables, downstream_tables):
    """UDF body — takes Spark columns and returns the composed text_blob."""
    table = TableInfo(
        table_name=table_name,
        purpose=purpose,
        granularity=granularity,
        business_rules=list(business_rules or []),
        fields=[FieldInfo(f["name"], f["description"]) for f in (fields or []) if f],
        upstream_namespaces=list(upstream_namespaces or []),
        upstream_tables=list(upstream_tables or []),
        downstream_tables=list(downstream_tables or []),
    )
    return compose_text_blob(table)


def build_metadata_udf(table_name, purpose, granularity, business_rules,
                      fields, upstream_namespaces, upstream_tables, downstream_tables):
    table = TableInfo(
        table_name=table_name,
        purpose=purpose,
        granularity=granularity,
        business_rules=list(business_rules or []),
        fields=[FieldInfo(f["name"], f["description"]) for f in (fields or []) if f],
        upstream_namespaces=list(upstream_namespaces or []),
        upstream_tables=list(upstream_tables or []),
        downstream_tables=list(downstream_tables or []),
    )
    return build_metadata(table)


def main() -> None:
    spark = get_spark("build-embedding-input")

    catalog = spark.read.parquet(str(CATALOG_IN))
    table_lineage = spark.read.parquet(str(LINEAGE_IN))

    print(f"Loaded catalog: {catalog.count()} tables")
    print(f"Loaded table_lineage: {table_lineage.count()} edges")

    # Join lineage onto catalog so each row has its full neighborhood available.
    enriched = join_lineage_to_catalog(catalog, table_lineage)

    # Extract upstream_namespaces from input_tables (the catalog's declared upstreams).
    # We use input_tables (not lineage) here because input_tables reflects what the
    # YAML author declared, which is what business_domain should reflect.
    namespace_udf = F.udf(
        lambda inputs: [t.split(".", 1)[0] for t in (inputs or []) if "." in t],
        ArrayType(StringType()),
    )
    enriched = enriched.withColumn("upstream_namespaces", namespace_udf(F.col("input_tables")))

    # Compose text_blob and metadata via UDFs.
    text_blob_udf = F.udf(build_text_blob_udf, StringType())
    metadata_udf = F.udf(build_metadata_udf, METADATA_SCHEMA)

    blob_inputs = [
        F.col("table_name"),
        F.col("purpose"),
        F.col("granularity"),
        F.col("business_rules"),
        F.col("fields"),
        F.col("upstream_namespaces"),
        F.col("upstream_fqns_from_lineage").alias("upstream_tables_in"),
        F.col("downstream_tables"),
    ]

    # We can't directly alias inside udf args, so re-fetch the original columns.
    with_blob = enriched.withColumn(
        "text_blob",
        text_blob_udf(
            F.col("table_name"),
            F.col("purpose"),
            F.col("granularity"),
            F.col("business_rules"),
            F.col("fields"),
            F.col("upstream_namespaces"),
            F.col("upstream_fqns_from_lineage"),
            F.col("downstream_tables"),
        ),
    ).withColumn(
        "metadata",
        metadata_udf(
            F.col("table_name"),
            F.col("purpose"),
            F.col("granularity"),
            F.col("business_rules"),
            F.col("fields"),
            F.col("upstream_namespaces"),
            F.col("upstream_fqns_from_lineage"),
            F.col("downstream_tables"),
        ),
    )

    # Infer business_domain.
    domain_udf = F.udf(infer_business_domain, StringType())
    with_domain = with_blob.withColumn(
        "business_domain", domain_udf(F.col("upstream_namespaces")),
    )

    # Final projection — only the columns the embedding pipeline needs.
    final = with_domain.select(
        "table_name",
        "business_domain",
        "text_blob",
        "metadata",
    )

    final.write.mode("overwrite").parquet(str(OUT_DIR))

    # Stats so you can sanity-check.
    print(f"\nWrote {final.count()} embedding-input rows to {OUT_DIR}")
    print("\nBusiness domain distribution:")
    final.groupBy("business_domain").count().orderBy(F.desc("count")).show(20, truncate=False)
    print("\ntext_blob length distribution:")
    final.select(F.length("text_blob").alias("blob_len")).describe().show()
    print("\nSample row:")
    sample = final.filter(F.col("table_name") == "action_after_ticket_closure_base").limit(1)
    if sample.count() == 0:
        sample = final.limit(1)
    row = sample.collect()[0]
    print(f"\ntable_name: {row.table_name}")
    print(f"business_domain: {row.business_domain}")
    print(f"metadata: {row.metadata}")
    print(f"\ntext_blob:\n{'-'*60}\n{row.text_blob}\n{'-'*60}")

    spark.stop()


if __name__ == "__main__":
    main()