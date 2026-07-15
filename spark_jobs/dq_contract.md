# Data Quality Contract — Data Dictionary Ingestion

## Schema-level checks (per-row, rejection criteria)

A row is **rejected** (sent to quarantine) if any of:

1. YAML failed to parse (caught by the UDF)
2. `table_name` is null or doesn't match `^[a-z][a-z0-9_]*$`
3. `input_tables` is null, empty, or any element doesn't match `^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`
4. `fields` is null or empty
5. Any field has a null `name`
6. Duplicate field names within the same table
7. `purpose` is null or under 40 characters
8. `granularity` is null or under 20 characters

## Aggregate-level checks (job-level, halt criteria)

The job **fails** (raises, no Parquet written) if:

1. Total file count < 400 (we expect ~500, allow 20% loss)
2. Rejection rate > 5% (more than 25 of 500)
3. Any rejection reason accounts for > 50% of rejections (indicates systemic issue)
4. Domain coverage < 8 distinct business domains in good rows

## Why these specific thresholds

- **5% rejection rate** is generous for a one-off masking exercise but would be alarming in production. We pick it as a learning threshold — corrupting 9 files leaves us comfortably under it.
- **Single-cause >50%** catches scenarios like "upstream changed timestamp format" where one root cause produces hundreds of rejections.
- **Domain coverage** catches "the upstream pipeline only delivered one domain by mistake."

## What this contract does NOT check

- Field-level lineage validity (does `data_warehouse.foo.bar` actually exist?)
  Reason: cross-file integrity is Day 4's job, not Day 3's.
- Semantic correctness (does `purpose` make sense?)
  Reason: requires LLM, not deterministic DQ.
- Schema evolution (is this YAML compatible with last week's?)
  Reason: we don't have history yet. Week 2's dbt snapshot will handle this.