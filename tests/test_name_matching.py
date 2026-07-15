"""Unit tests for serve/matching.py — pure functions, no corpus needed."""
from __future__ import annotations

from serve.matching import (
    did_you_mean,
    exact_mentions,
    fuzzy_name_matches,
    identifier_tokens,
    query_tokens,
)

NAMES = [
    "orders_base_007",
    "orders_hourly_snapshot_055",
    "delivery_attempts_events_026",
    "calendar_base_007",
    "cod_collections_base_019",
]
BY_NAME = {n: i for i, n in enumerate(NAMES)}


def test_query_tokens_lowercases_and_splits():
    assert "orders_base_007" in query_tokens("Explain Orders_Base_007 please")


def test_exact_mentions_finds_named_table():
    assert exact_mentions("explain orders_base_007", BY_NAME) == [0]


def test_exact_mentions_dedupes_and_preserves_order():
    q = "join calendar_base_007 with orders_base_007 and calendar_base_007"
    assert exact_mentions(q, BY_NAME) == [3, 0]


def test_exact_mentions_empty_for_prose():
    assert exact_mentions("how do failed deliveries work?", BY_NAME) == []


def test_identifier_tokens_requires_underscore():
    assert identifier_tokens("explain active_orders table now") == ["active_orders"]


def test_fuzzy_matches_by_shared_parts():
    hits = fuzzy_name_matches("active_orders", NAMES)
    assert hits and all("orders" in h for h in hits)


def test_fuzzy_matches_catches_typo():
    assert fuzzy_name_matches("orders_bsae_007", NAMES)[0] == "orders_base_007"


def test_fuzzy_no_match_for_unrelated_token():
    assert fuzzy_name_matches("employee_payroll", NAMES) == []


def test_did_you_mean_skips_exact_names():
    assert did_you_mean("explain orders_base_007", NAMES, BY_NAME) == []


def test_did_you_mean_suggests_for_missing_name():
    out = did_you_mean("explain active_orders table", NAMES, BY_NAME)
    assert "orders_base_007" in out or "orders_hourly_snapshot_055" in out
