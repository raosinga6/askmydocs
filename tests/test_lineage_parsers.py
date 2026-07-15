"""Unit tests for lineage parsers. Run with: uv run pytest tests/test_lineage_parsers.py -v"""
import pytest

from spark_jobs.lineage_parsers import (
    ParsedSource,
    classify_source_kind,
    parse_input_table,
    parse_source_string,
)


class TestParseSourceString:
    def test_empty_returns_empty(self):
        assert parse_source_string(None) == []
        assert parse_source_string("") == []
        assert parse_source_string("   ") == []

    def test_single_column_fqn(self):
        result = parse_source_string("core_prod_gl.inbound_scans.hub_id")
        assert result == [ParsedSource("core_prod_gl", "inbound_scans", "hub_id")]

    def test_single_table_fqn(self):
        result = parse_source_string("data_warehouse.hubs_enriched")
        assert result == [ParsedSource("data_warehouse", "hubs_enriched", None)]

    def test_real_multi_source_from_masked_yaml(self):
        # Straight from action_after_ticket_closure_base_masked.yaml
        source = (
            "core_prod_gl.inbound_scans.hub_id, "
            "core_prod_gl.warehouse_sweeps.hub_id, "
            "data_warehouse.add_to_shipment_events.hub_id, "
            "data_warehouse.hubs_enriched"
        )
        result = parse_source_string(source)
        assert len(result) == 4
        assert result[0].column == "hub_id"
        assert result[3].column is None  # the bare table reference

    def test_digit_leading_namespace(self):
        result = parse_source_string("3pl_prod_gl.parcels.id")
        assert result == [ParsedSource("3pl_prod_gl", "parcels", "id")]

    def test_noise_tokens_dropped(self):
        # Real-world: someone wrote prose in the source field
        source = "Direct field from source"
        assert parse_source_string(source) == []

    def test_trailing_comma_doesnt_break(self):
        result = parse_source_string("a.b.c, d.e.f,")
        assert len(result) == 2

    def test_trailing_dot_stripped(self):
        result = parse_source_string("a.b.c.")
        assert result == [ParsedSource("a", "b", "c")]


class TestClassifySourceKind:
    def test_direct(self):
        assert classify_source_kind("Direct field from source") == "direct"
        assert classify_source_kind("Direct field, lowercased") == "direct"

    def test_derived(self):
        assert classify_source_kind("Derived field: IF(x > 0, 1, 0)") == "derived"
        assert classify_source_kind("Derived field: ROW_NUMBER() OVER (...)") == "derived"
        assert classify_source_kind("COALESCE(a, b)") == "derived"

    def test_unknown(self):
        assert classify_source_kind(None) == "unknown"
        assert classify_source_kind("") == "unknown"
        assert classify_source_kind("some unrelated text") == "unknown"


class TestParseInputTable:
    def test_valid(self):
        assert parse_input_table("data_warehouse.hubs") == ("data_warehouse", "hubs")

    def test_digit_leading(self):
        assert parse_input_table("3pl_prod_gl.parcels") == ("3pl_prod_gl", "parcels")

    def test_invalid_returns_none(self):
        assert parse_input_table("just_a_name") is None
        assert parse_input_table("a.b.c.d") is None  # too many dots
        assert parse_input_table(None) is None
        assert parse_input_table("") is None