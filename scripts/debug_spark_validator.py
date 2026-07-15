"""Replicate the Spark validator's regex check, identify which files fail."""
import re
from pathlib import Path

import yaml

RAW_DIR = Path("/app/data/raw")
PATTERN = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")

results: list[tuple[str, list, list[str]]] = []  # (file, input_tables, failing_entries)

for path in sorted(RAW_DIR.glob("*.yaml")):
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        continue

    raw_input_tables = doc.get("input_tables") or []
    # Mimic the UDF's filter:
    filtered = [t for t in raw_input_tables if isinstance(t, str)]

    failing = [t for t in filtered if not PATTERN.match(t)]
    if failing:
        results.append((path.name, filtered, failing))

print(f"Files with at least one failing input_table: {len(results)}")
for name, all_, failing in results[:20]:
    print(f"\n{name}")
    print(f"  all input_tables: {all_}")
    print(f"  failing: {failing}")