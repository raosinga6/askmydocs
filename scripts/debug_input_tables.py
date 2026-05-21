"""Debug why so many input_tables entries are failing validation."""
import re
from pathlib import Path

import yaml

RAW_DIR = Path("/app/data/raw")  # If running in container
# Use this if running on host:
# RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

PATTERN = re.compile(r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")

bad_entries: list[tuple[str, str]] = []

for path in sorted(RAW_DIR.glob("*.yaml")):
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        continue
    for entry in (doc.get("input_tables") or []):
        if not isinstance(entry, str):
            bad_entries.append((path.name, f"non-string: {entry!r}"))
            continue
        if not PATTERN.match(entry):
            bad_entries.append((path.name, entry))

print(f"Total bad input_tables entries: {len(bad_entries)}")
print(f"Files with bad entries: {len({p for p, _ in bad_entries})}")
print("\nSample bad entries:")
for name, entry in bad_entries[:20]:
    print(f"  {name}: {entry}")