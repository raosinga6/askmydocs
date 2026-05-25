"""Tests for the embedding input composer."""
import pytest

from spark_jobs.embedding_input import (
    FieldInfo,
    TableInfo,
    build_metadata,
    compose_text_blob,
    infer_business_domain,
    is_pii_field,
    primary_upstream_namespace,
)


def make_table(**overrides) -> TableInfo:
    defaults = {
        "table_name": "test_table",
        "purpose": "Test purpose long enough to satisfy schema requirements minimum.",
        "granularity": "One row per test entity per day.",
        "business_rules": ["Rule one.", "Rule two."],
        "fields": [
            FieldInfo("id", "Primary key identifier."),
            FieldInfo("name", "Person's full name."),
        ],
        "upstream_namespaces": ["data_warehouse"],
        "upstream_tables": ["data_warehouse.foo", "core_prod_gl.bar"],
        "downstream_tables": ["downstream_one"],
    }
    defaults.update(overrides)
    return TableInfo(**defaults)


class TestIsPiiField:
    @pytest.mark.parametrize("name", ["email", "phone_number", "full_name", "home_address", "user_ssn"])
    def test_pii_names_detected(self, name):
        assert is_pii_field(name)

    @pytest.mark.parametrize("name", ["order_id", "amount", "status", "created_at", "hub_id"])
    def test_non_pii_names_pass(self, name):
        assert not is_pii_field(name)

    def test_empty_safe(self):
        assert not is_pii_field("")
        assert not is_pii_field(None)


class TestInferBusinessDomain:
    def test_most_common_namespace_wins(self):
        assert infer_business_domain(["data_warehouse", "data_warehouse", "core_prod_gl"]) == "data_warehouse"

    def test_empty_returns_unknown(self):
        assert infer_business_domain([]) == "unknown"


class TestPrimaryUpstreamNamespace:
    def test_basic(self):
        result = primary_upstream_namespace([
            "data_warehouse.foo",
            "data_warehouse.bar",
            "core_prod_gl.baz",
        ])
        assert result == "data_warehouse"

    def test_empty_returns_unknown(self):
        assert primary_upstream_namespace([]) == "unknown"


class TestComposeTextBlob:
    def test_includes_all_sections(self):
        table = make_table()
        blob = compose_text_blob(table)
        assert "Table: test_table" in blob
        assert "Purpose:" in blob
        assert "Granularity:" in blob
        assert "Business rules:" in blob
        assert "Fields:" in blob
        assert "Joins with:" in blob
        assert "Used by:" in blob

    def test_section_ordering(self):
        """Order matters for embedding attention — Purpose should come early."""
        table = make_table()
        blob = compose_text_blob(table)
        assert blob.index("Purpose:") < blob.index("Fields:")
        assert blob.index("Fields:") < blob.index("Joins with:")

    def test_business_rules_become_bulleted(self):
        table = make_table(business_rules=["First rule.", "Second rule."])
        blob = compose_text_blob(table)
        assert "- First rule." in blob
        assert "- Second rule." in blob

    def test_fields_sorted_by_description_length(self):
        """Richer descriptions should appear first (they're more informative)."""
        table = make_table(fields=[
            FieldInfo("short_field", "x"),
            FieldInfo("rich_field", "x" * 200),
        ])
        blob = compose_text_blob(table)
        assert blob.index("rich_field") < blob.index("short_field")

    def test_hard_cap_enforced(self):
        # Construct a deliberately huge fields list.
        table = make_table(fields=[FieldInfo(f"field_{i}", "x" * 100) for i in range(500)])
        blob = compose_text_blob(table)
        assert len(blob) <= 4000

    def test_handles_missing_optional_sections(self):
        table = make_table(business_rules=[], upstream_tables=[], downstream_tables=[])
        blob = compose_text_blob(table)
        # Should not crash; should still include the core sections.
        assert "Table: test_table" in blob
        assert "Purpose:" in blob


class TestBuildMetadata:
    def test_field_count(self):
        meta = build_metadata(make_table())
        assert meta["field_count"] == 2

    def test_pii_field_names_detected(self):
        meta = build_metadata(make_table())
        assert "name" in meta["pii_field_names"]
        assert "id" not in meta["pii_field_names"]

    def test_primary_namespace(self):
        table = make_table(upstream_tables=[
            "data_warehouse.foo",
            "data_warehouse.bar",
            "core_prod_gl.baz",
        ])
        meta = build_metadata(table)
        assert meta["primary_upstream_namespace"] == "data_warehouse"