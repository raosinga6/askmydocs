"""Pure-Python parsers for lineage extraction.

Kept separate from the Spark job so they're unit-testable without a Spark
session. The Spark job wraps these as UDFs.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

# An FQN is namespace.table[.column], lowercase alphanum + underscores.
# Allow digit-leading identifiers because real namespaces use them (3pl_prod_gl).
FQN_RE = re.compile(
    r"^([a-z0-9][a-z0-9_]*)\.([a-z0-9][a-z0-9_]*)(?:\.([a-z0-9][a-z0-9_]*))?$"
)


@dataclass(frozen=True)
class ParsedSource:
    namespace: str
    table: str
    column: str | None  # None when the source references the whole table

    @property
    def table_fqn(self) -> str:
        return f"{self.namespace}.{self.table}"

    @property
    def column_fqn(self) -> str:
        return f"{self.namespace}.{self.table}.{self.column}" if self.column else self.table_fqn


def parse_source_string(source: str | None) -> list[ParsedSource]:
    """Parse a comma-separated source string into a list of ParsedSource.

    Returns an empty list if `source` is None, blank, or contains no valid FQNs.
    Tokens that don't match FQN_RE are silently dropped — they're not lineage,
    they're noise (e.g. "Derived field" inline notes that ended up in source).
    """
    if not source:
        return []
    out: list[ParsedSource] = []
    for raw in source.split(","):
        token = raw.strip().rstrip(".")
        if not token:
            continue
        m = FQN_RE.match(token)
        if not m:
            continue
        ns, tbl, col = m.group(1), m.group(2), m.group(3)
        out.append(ParsedSource(namespace=ns, table=tbl, column=col))
    return out


def classify_source_kind(technical_description: str | None) -> str:
    """Classify a field as 'direct', 'derived', or 'unknown' based on its tech description."""
    if not technical_description:
        return "unknown"
    td = technical_description.lower()
    if td.startswith("direct field"):
        return "direct"
    derived_markers = ("derived field", "if(", "case when", "row_number", "lag(",
                       "lead(", "sum(", "count(", "coalesce", "concat", "substr",
                       "regexp_extract", "from_utc_timestamp", "datediff")
    if any(marker in td for marker in derived_markers):
        return "derived"
    return "unknown"


def parse_input_table(token: str | None) -> tuple[str, str] | None:
    """Parse a single input_tables entry into (namespace, table). Returns None if invalid."""
    if not token or not isinstance(token, str):
        return None
    m = re.match(r"^([a-z0-9][a-z0-9_]*)\.([a-z0-9][a-z0-9_]*)$", token.strip())
    if not m:
        return None
    return m.group(1), m.group(2)