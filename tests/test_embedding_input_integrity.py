"""Cross-check the embedding_input dataset for completeness and quality."""
import pytest
from pyspark.sql import functions as F

from spark_jobs.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-embedding-input")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def catalog(spark):
    return spark.read.parquet("/app/data/parquet/catalog")


@pytest.fixture(scope="module")
def embedding_input(spark):
    return spark.read.parquet("/app/data/parquet/embedding_input")


def test_one_row_per_catalog_table(catalog, embedding_input):
    assert catalog.count() == embedding_input.count()


def test_all_text_blobs_nonempty(embedding_input):
    empty = embedding_input.filter(F.length("text_blob") < 100).count()
    assert empty == 0, "Some text_blobs are suspiciously short"


def test_text_blob_within_hard_cap(embedding_input):
    too_long = embedding_input.filter(F.length("text_blob") > 4000).count()
    assert too_long == 0


def test_no_null_business_domain(embedding_input):
    nulls = embedding_input.filter(F.col("business_domain").isNull()).count()
    assert nulls == 0


def test_table_name_unique(embedding_input):
    total = embedding_input.count()
    distinct = embedding_input.select("table_name").distinct().count()
    assert total == distinct


def test_purpose_appears_in_every_blob(embedding_input):
    """Purpose is a required section in our template; every blob should have it."""
    missing = embedding_input.filter(~F.col("text_blob").contains("Purpose:")).count()
    assert missing == 0


def test_fields_section_in_every_blob(embedding_input):
    missing = embedding_input.filter(~F.col("text_blob").contains("Fields:")).count()
    assert missing == 0


def test_real_masked_table_has_rich_blob(embedding_input):
    """The action_after_ticket_closure_base masked YAML should produce a rich blob."""
    row = embedding_input.filter(
        F.col("table_name") == "action_after_ticket_closure_base"
    ).collect()
    if not row:
        pytest.skip("action_after_ticket_closure_base not in catalog")
    assert len(row[0].text_blob) > 1000
    assert "ticket" in row[0].text_blob.lower()
    assert "scan" in row[0].text_blob.lower()