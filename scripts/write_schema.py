"""Write the data dictionary JSON Schema to disk.

Running this is more reliable than pasting JSON into a text editor on Windows,
which often produces empty files or BOM-encoded files that json.loads can't read.
"""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCHEMA_PATH = REPO / "schemas" / "data_dictionary_schema.json"

SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "DataDictionaryEntry",
    "type": "object",
    "required": ["table_name", "overview", "input_tables", "fields"],
    "additionalProperties": False,
    "properties": {
        "table_name": {
            "type": "string",
            "pattern": "^[a-z][a-z0-9_]*$",
            "minLength": 3,
            "maxLength": 80,
        },
        "overview": {
            "type": "object",
            "required": ["purpose", "granularity", "business_rules"],
            "additionalProperties": False,
            "properties": {
                "purpose": {"type": "string", "minLength": 40, "maxLength": 1000},
                "granularity": {"type": "string", "minLength": 20, "maxLength": 500},
                "business_rules": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 20,
                    "items": {"type": "string", "minLength": 10, "maxLength": 400},
                },
            },
        },
        "input_tables": {
            "type": "array",
            "minItems": 1,
            "maxItems": 25,
            "items": {
                "type": "string",
                "pattern": "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*$",
            },
        },
        "fields": {
            "type": "array",
            "minItems": 1,
            "maxItems": 60,
            "items": {
                "type": "object",
                "required": ["name", "description", "source", "technical_description"],
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string", "pattern": "^[a-z][a-z0-9_]*$"},
                    "description": {"type": "string", "minLength": 5, "maxLength": 500},
                    "source": {"type": "string", "minLength": 5, "maxLength": 2000},
                    "technical_description": {"type": "string", "minLength": 5, "maxLength": 2000},
                },
            },
        },
    },
}

SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
SCHEMA_PATH.write_text(json.dumps(SCHEMA, indent=2), encoding="utf-8")
print(f"Wrote {SCHEMA_PATH} ({SCHEMA_PATH.stat().st_size} bytes)")