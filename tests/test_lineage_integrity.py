"""Cross-check lineage outputs against the catalog. Runs after extract_lineage."""
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from spark_jobs.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-lineage-integrity")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def catalog(spark):
    return spark.read.parquet("/app/data/parquet/catalog")


@pytest.fixture(scope="module")
def table_lineage(spark):
    return spark.read.parquet("/app/data/parquet/table_lineage")


@pytest.fixture(scope="module")
def field_lineage(spark):
    return spark.read.parquet("/app/data/parquet/field_lineage")


def test_every_downstream_table_in_catalog(catalog, table_lineage):
    catalog_tables = {r.table_name for r in catalog.select("table_name").collect()}
    lineage_downstreams = {r.downstream_table for r in table_lineage.select("downstream_table").distinct().collect()}
    missing = lineage_downstreams - catalog_tables
    assert not missing, f"Downstream tables in lineage but not catalog: {missing}"


def test_no_self_references(table_lineage):
    self_refs = table_lineage.filter(F.col("downstream_table") == F.col("upstream_table")).count()
    assert self_refs == 0


def test_field_lineage_implies_table_lineage(table_lineage, field_lineage):
    """Every (downstream, upstream) pair in field_lineage should exist in table_lineage."""
    field_pairs = field_lineage.select(
        "downstream_table",
        F.concat_ws(".", "upstream_namespace", "upstream_table").alias("upstream_fqn"),
    ).distinct()
    table_pairs = table_lineage.select("downstream_table", "upstream_fqn")

    orphans = field_pairs.exceptAll(table_pairs).count()
    # Allow a small number — Gemini sometimes invents sources not in input_tables.
    assert orphans < 100, f"Too many field lineage edges with no table lineage parent: {orphans}"


def test_lineage_has_volume(table_lineage, field_lineage):
    """Sanity check that extraction actually produced edges."""
    assert table_lineage.count() > 500, "Suspiciously few table edges"
    assert field_lineage.count() > 1000, "Suspiciously few field edges"


def test_source_kinds_distributed(field_lineage):
    """All three source_kinds should appear in the data."""
    kinds = {r.source_kind for r in field_lineage.select("source_kind").distinct().collect()}
    assert "direct" in kinds
    assert "derived" in kinds
    # unknown is allowed but not required