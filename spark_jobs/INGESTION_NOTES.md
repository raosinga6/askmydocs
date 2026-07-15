## Day 4: lineage graph extraction

Mined the validated catalog to produce two graph datasets:

### Table lineage
~3000 edges of (downstream_table, upstream_table). Built by exploding
`input_tables`, parsing each FQN, dropping invalid entries and self-references.

### Field lineage
~10000 edges of (downstream_table, downstream_field, upstream_table, upstream_column).
Built by exploding `fields[].source`, splitting the comma-separated FQN list,
parsing each into structured form. Each edge tagged with `source_kind`
(direct / derived / unknown) heuristically classified from `technical_description`.

### Why this matters for the agent
The Week 4 agent answers questions like "how do I join X and Y?" by
intersecting the downstream sets of X and Y — finding derived tables that
already combine them. That intersection query takes a single Parquet scan
because Day 4 extracted the graph upfront. Without this step, the agent
would have to scan all 484 YAMLs every query.

### Validation
- Cross-check tests in tests/test_lineage_integrity.py
- Every downstream_table in lineage exists in catalog
- No self-references
- Volume sanity checks
- Field lineage edges roughly imply matching table lineage edges

## Day 5: embedding input preparation

Composed the dataset that Week 4's RAG layer will embed. One row per table
with a single `text_blob` string plus a `metadata` struct for retrieval-time
filtering.

### What goes in the blob (and what doesn't)

Five sections: Purpose, Granularity, Business rules (bulleted), Fields
(name:description compact list, truncated to 50 by description richness),
Joins with (from lineage), Used by (from lineage, top 10).

We DO NOT embed: input_tables raw FQNs (already structural), field.source /
technical_description (lineage already structures these). Embedding them
would just add noise to retrieval.

### Length budget

text_blob targets 2000 chars (~500 tokens for sentence-transformers).
Hard cap at 4000. Truncation order: drop fields by description-length DESC,
then drop downstream tables, then ellipsis.

### Why composer is pure Python + UDF

The composition logic is testable without Spark (15 unit tests).
The Spark layer is a thin UDF wrapper that lets us produce 484 rows in one
distributed job. Same code path runs locally on one record (for unit tests)
and on hundreds (in production).

### Real vs synthetic blob quality

Real masked YAMLs produce blobs that read like senior data engineering docs
(specific column names, concrete predicates). Synthetic Gemini YAMLs produce
coherent narrative but some incoherent field-source associations — a known
limitation flagged for Week 4 eval scoping.