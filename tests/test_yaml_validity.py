"""Verify every YAML (real + synthetic) satisfies the schema."""
import json
import re
from pathlib import Path

import pytest
import yaml
from jsonschema import validate

REPO = Path(__file__).resolve().parent.parent
SCHEMA = json.loads((REPO / "schemas" / "data_dictionary_schema.json").read_text())
YAML_DIR = REPO / "data" / "raw"

SOURCE_PATH_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,3}$")


@pytest.fixture(scope="session")
def all_yamls() -> list[tuple[Path, dict]]:
    return [(p, yaml.safe_load(p.read_text())) for p in sorted(YAML_DIR.glob("*.yaml"))]


def test_count_is_500(all_yamls):
    assert len(all_yamls) == 500


@pytest.mark.parametrize("yaml_path", sorted(YAML_DIR.glob("*.yaml")))
def test_yaml_matches_schema(yaml_path: Path) -> None:
    doc = yaml.safe_load(yaml_path.read_text())
    validate(doc, SCHEMA)


def test_source_paths_well_formed(all_yamls):
    """Every comma-separated source token should look like a.b[.c[.d]] identifier path."""
    bad: list[str] = []
    for path, doc in all_yamls:
        for field in doc["fields"]:
            tokens = [t.strip() for t in field["source"].split(",")]
            for tok in tokens:
                if not SOURCE_PATH_RE.match(tok):
                    bad.append(f"{path.name}::{field['name']} -> '{tok}'")
    assert not bad, f"{len(bad)} bad source tokens. First 5:\n" + "\n".join(bad[:5])


def test_input_tables_referenced_in_sources(all_yamls):
    """At least 60% of input_tables should appear in at least one field source.

    If an input table never shows up in any source, it's probably unused.
    The threshold is loose because some input_tables are used for filtering
    only and never appear in select projections (as is true in the real sample).
    """
    weak: list[str] = []
    for path, doc in all_yamls:
        input_set = set(doc["input_tables"])
        referenced: set[str] = set()
        for field in doc["fields"]:
            for tok in field["source"].split(","):
                tok = tok.strip()
                parts = tok.split(".")
                if len(parts) >= 2:
                    referenced.add(".".join(parts[:2]))
        coverage = len(input_set & referenced) / max(len(input_set), 1)
        if coverage < 0.6:
            weak.append(f"{path.name}: {coverage:.0%} input table coverage")
    # Allow up to 5% of tables to have weak coverage (mimics real-world data).
    assert len(weak) <= len(all_yamls) * 0.05, (
        f"{len(weak)} tables have low input_table coverage. First 5:\n"
        + "\n".join(weak[:5])
    )


def test_no_duplicate_table_names(all_yamls):
    names = [doc["table_name"] for _, doc in all_yamls]
    dupes = [n for n in set(names) if names.count(n) > 1]
    assert not dupes, f"Duplicate table names: {dupes}"


def test_real_yamls_present(all_yamls):
    """At least one real masked YAML should be present in the catalog."""
    names = [doc["table_name"] for _, doc in all_yamls]
    real_present = any("masked" in n or "action_after_ticket_closure" in n for n in names)
    assert real_present, "Expected at least one real masked YAML in data/raw/"