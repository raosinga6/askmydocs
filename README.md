# AskMyDocs

RAG service over a logistics data dictionary catalog. Production deployment on GCP via GKE.

## Catalog

- **Format**: matches our internal data dictionary YAML format — `table_name`, `overview {purpose, granularity, business_rules}`, `input_tables`, `fields [{name, description, source, technical_description}]`.
- **Sources**: real masked dictionaries in `data/real/` + synthetic neighbors in the same shape.
- **Total**: 500 YAMLs across 10 logistics business domains.

## Day 1 — YAML contract + catalog assembly

- Schema: `schemas/data_dictionary_schema.json` (validated against real masked sample)
- Topology: `schemas/domain_topology.md`
- Generator: `scripts/generate_yamls.py` (seeded, deterministic)
- Validation: `uv run pytest tests/test_yaml_validity.py`

## Layout

- `data/real/` — real masked dictionaries (gitignored by default; toggle if safe to publish)
- `data/raw/` — combined catalog (real + synthetic) ready for Spark ingestion
- `schemas/` — JSON schema + topology doc
- `scripts/` — generators
- `tests/` — pytest validation