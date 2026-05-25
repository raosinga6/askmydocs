"""Compose the embedding input text_blob and metadata per table.

Pure functions kept separate from the Spark job so they're unit-testable.
The Spark job wraps these as UDFs.
"""
from __future__ import annotations

from dataclasses import dataclass

# Length budget for the text_blob — see EMBEDDING_INPUT.md.
TARGET_LENGTH = 2000
HARD_CAP = 4000
MAX_FIELDS_IN_TEXT = 50
MAX_DOWNSTREAMS_IN_TEXT = 10

PII_NAME_HINTS = {"email", "phone", "ssn", "passport", "name", "address",
                  "dob", "ip_address", "credit_card"}


@dataclass(frozen=True)
class FieldInfo:
    name: str
    description: str | None


@dataclass(frozen=True)
class TableInfo:
    table_name: str
    purpose: str | None
    granularity: str | None
    business_rules: list[str]
    fields: list[FieldInfo]
    upstream_namespaces: list[str]  # from input_tables
    upstream_tables: list[str]       # from table_lineage (deduped)
    downstream_tables: list[str]     # from table_lineage (deduped)


def is_pii_field(field_name: str) -> bool:
    """Heuristic: does this field name suggest PII?"""
    if not field_name:
        return False
    lower = field_name.lower()
    return any(hint in lower for hint in PII_NAME_HINTS)


def infer_business_domain(upstream_namespaces: list[str]) -> str:
    """Pick the most-referenced namespace as the table's domain proxy.

    Falls back to 'unknown' if no upstream tables exist.
    """
    if not upstream_namespaces:
        return "unknown"
    counts: dict[str, int] = {}
    for ns in upstream_namespaces:
        counts[ns] = counts.get(ns, 0) + 1
    return max(counts.items(), key=lambda x: x[1])[0]


def primary_upstream_namespace(upstream_tables: list[str]) -> str:
    """Most-referenced namespace among upstream_tables (full FQN list)."""
    counts: dict[str, int] = {}
    for fqn in upstream_tables:
        if "." in fqn:
            ns = fqn.split(".", 1)[0]
            counts[ns] = counts.get(ns, 0) + 1
    if not counts:
        return "unknown"
    return max(counts.items(), key=lambda x: x[1])[0]


def _format_fields_section(fields: list[FieldInfo]) -> str:
    """Render fields into a compact 'name: description; name: description' string."""
    if not fields:
        return "(no fields documented)"
    # Sort by description length desc (richer descriptions are more informative);
    # cap at MAX_FIELDS_IN_TEXT.
    sorted_fields = sorted(
        fields,
        key=lambda f: len(f.description or ""),
        reverse=True,
    )[:MAX_FIELDS_IN_TEXT]
    parts = []
    for f in sorted_fields:
        desc = (f.description or "").strip()
        if desc:
            parts.append(f"{f.name}: {desc}")
        else:
            parts.append(f.name)
    return "; ".join(parts)


def _format_list(items: list[str], cap: int) -> str:
    if not items:
        return "(none)"
    return ", ".join(items[:cap])


def compose_text_blob(table: TableInfo) -> str:
    """Build the full text_blob string for one table."""
    sections: list[str] = []

    sections.append(f"Table: {table.table_name}")

    if table.purpose:
        sections.append(f"Purpose: {table.purpose.strip()}")

    if table.granularity:
        sections.append(f"Granularity: {table.granularity.strip()}")

    if table.business_rules:
        rules_str = "\n".join(f"- {r.strip()}" for r in table.business_rules if r.strip())
        if rules_str:
            sections.append(f"Business rules:\n{rules_str}")

    fields_str = _format_fields_section(table.fields)
    sections.append(f"Fields: {fields_str}")

    if table.upstream_tables:
        sections.append(f"Joins with: {_format_list(table.upstream_tables, 20)}")

    if table.downstream_tables:
        sections.append(f"Used by: {_format_list(table.downstream_tables, MAX_DOWNSTREAMS_IN_TEXT)}")

    blob = "\n\n".join(sections)

    # Hard truncation guard.
    if len(blob) > HARD_CAP:
        blob = blob[:HARD_CAP].rsplit(" ", 1)[0] + "..."

    return blob


def build_metadata(table: TableInfo) -> dict:
    """Build the metadata struct for one table."""
    return {
        "input_table_count": len(table.upstream_tables),
        "field_count": len(table.fields),
        "downstream_count": len(table.downstream_tables),
        "pii_field_names": [f.name for f in table.fields if is_pii_field(f.name)],
        "primary_upstream_namespace": primary_upstream_namespace(table.upstream_tables),
    }