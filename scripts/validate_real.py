"""Validate real masked YAMLs against the schema."""
import json
import sys
from pathlib import Path

import yaml
from jsonschema import validate, ValidationError

REPO = Path(__file__).resolve().parent.parent
#SCHEMA = json.loads((REPO / "schemas" / "data_dictionary_schema.json").read_text())
SCHEMA = json.loads((REPO / "schemas" / "data_dictionary_schema.json").read_text(encoding="utf-8"))
REAL_DIR = REPO / "data" / "real"

if not REAL_DIR.exists() or not any(REAL_DIR.glob("*.yaml")):
    print(f"No YAML files found in {REAL_DIR}")
    sys.exit(1)

errors = 0
for path in sorted(REAL_DIR.glob("*.yaml")):
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        validate(doc, SCHEMA)
        print(f"OK: {path.name}")
    except ValidationError as e:
        print(f"INVALID: {path.name}")
        print(f"  -> {e.message}")
        errors += 1
    except yaml.YAMLError as e:
        print(f"YAML PARSE ERROR: {path.name}")
        print(f"  -> {e}")
        errors += 1

sys.exit(1 if errors else 0)