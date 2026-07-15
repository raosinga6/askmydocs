# Ingestion approaches: notes for Day 6 perf comparison

## Approach 1: `spark.read.text` + UDF (`ingest_v1_text_udf.py`)

- **What**: DataFrame API, one row per file via `wholetext=true`, parsing inside a Python UDF
- **Pros**:
  - Returns a DataFrame directly — plays well with Catalyst, joins, partitioning
  - Schema-on-read via `CATALOG_SCHEMA`, fails closed on shape changes
  - Natural fit for downstream Parquet write
- **Cons**:
  - Python UDF serializes every row through the JVM↔Python bridge
  - 500 files = 500 tasks by default with `wholetext=true`

## Approach 2: `wholeTextFiles` RDD (`ingest_v2_wholetextfiles.py`)

- **What**: RDD API, `(path, content)` pairs, parsing in pure Python `map`
- **Pros**:
  - Spark automatically bundles small files into larger partitions
  - `minPartitions` gives explicit control
  - Useful pattern for legacy codebases
- **Cons**:
  - RDD API is essentially deprecated for new code
  - No Catalyst optimization
  - Schema only emerges after `toDF()`, which is slower and less safe

## The small-file problem (preview for Day 6)

500 YAMLs are tiny (~3-5 KB each). Spark's per-task overhead dwarfs the actual work.
The fix is to coalesce reads:
- Approach 1: read into many partitions, `coalesce(8)` before write
- Approach 2: tune `minPartitions` on the RDD

We'll benchmark both on a 10x dataset on Day 6 and pick a winner.

## Decision: keep Approach 1 as the production path

DataFrame API is the future. The Python UDF overhead is fine at our scale and
we'll address it on Day 6 if it actually matters.