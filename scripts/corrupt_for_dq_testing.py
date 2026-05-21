"""Corrupt 9 files in data/raw/ to test DQ checks."""
from __future__ import annotations

import random
import sys
from pathlib import Path

import yaml

random.seed(123)

REPO = Path(__file__).resolve().parent.parent
RAW_DIR = REPO / "data" / "raw"

CORRUPTION_MARKER = "# CORRUPTED_FOR_DQ_TESTING"

print(f"REPO: {REPO}")
print(f"RAW_DIR: {RAW_DIR}")
print(f"RAW_DIR exists: {RAW_DIR.exists()}")
print(f"YAML files found: {len(list(RAW_DIR.glob('*.yaml')))}")


def list_candidates() -> list[Path]:
    """Files we can corrupt — excludes real masked YAMLs and already-broken ones."""
    files = sorted(RAW_DIR.glob("*.yaml"))
    candidates = []
    for f in files:
        try:
            content = f.read_text(encoding="utf-8")
        except Exception as e:
            print(f"  Skipping unreadable file {f.name}: {e}")
            continue
        if "masked" in f.name:
            continue
        if CORRUPTION_MARKER in content:
            continue
        candidates.append(f)
    return candidates


def corrupt_missing_table_name(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc.pop("table_name", None)
    write_with_marker(path, doc, "missing required field: table_name")


def corrupt_missing_input_tables(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc.pop("input_tables", None)
    write_with_marker(path, doc, "missing required field: input_tables")


def corrupt_empty_fields(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["fields"] = []
    write_with_marker(path, doc, "empty fields list")


def corrupt_bad_input_table_format(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    doc["input_tables"] = ["just_a_name", "another.bad.format.too.long"]
    write_with_marker(path, doc, "malformed input_tables entries")


def corrupt_bad_field_source(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if doc.get("fields"):
        doc["fields"][0]["source"] = "this isn't a real source path"
    write_with_marker(path, doc, "malformed field source path")


def corrupt_purpose_too_short(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if "overview" in doc:
        doc["overview"]["purpose"] = "TBD"
    write_with_marker(path, doc, "purpose under 40 chars")


def corrupt_duplicate_field_names(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if len(doc.get("fields") or []) >= 2:
        doc["fields"][1]["name"] = doc["fields"][0]["name"]
    write_with_marker(path, doc, "duplicate field names")


def corrupt_yaml_syntax(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    broken = text.replace("description:", 'description: "unclosed "string', 1)
    path.write_text(broken + f"\n{CORRUPTION_MARKER}: yaml syntax error\n", encoding="utf-8")


def corrupt_input_table_self_reference(path: Path) -> None:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    table_name = doc.get("table_name")
    if table_name:
        doc["input_tables"] = doc.get("input_tables", []) + [f"data_warehouse.{table_name}"]
    write_with_marker(path, doc, "self-referential input table")


def write_with_marker(path: Path, doc: dict, reason: str) -> None:
    content = yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, width=120, allow_unicode=True)
    content += f"\n{CORRUPTION_MARKER}: {reason}\n"
    path.write_text(content, encoding="utf-8")


CORRUPTIONS = [
    corrupt_missing_table_name,
    corrupt_missing_input_tables,
    corrupt_empty_fields,
    corrupt_bad_input_table_format,
    corrupt_bad_field_source,
    corrupt_purpose_too_short,
    corrupt_duplicate_field_names,
    corrupt_yaml_syntax,
    corrupt_input_table_self_reference,
]


def main() -> None:
    candidates = list_candidates()
    print(f"\nCandidates available for corruption: {len(candidates)}")

    if len(candidates) < len(CORRUPTIONS):
        print(f"ERROR: Not enough clean files: have {len(candidates)}, need {len(CORRUPTIONS)}", file=sys.stderr)
        sys.exit(1)

    chosen = random.sample(candidates, len(CORRUPTIONS))
    for path, corrupt_fn in zip(chosen, CORRUPTIONS):
        print(f"Corrupting {path.name} ({corrupt_fn.__name__})")
        try:
            corrupt_fn(path)
        except Exception as e:
            print(f"  FAILED: {e}", file=sys.stderr)

    print(f"\nDone. Corrupted {len(chosen)} files.")


if __name__ == "__main__":
    main()