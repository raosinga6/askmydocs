# AskMyDocs — Project Guide

## Executive summary

AskMyDocs is a **RAG (Retrieval-Augmented Generation) service over a logistics data-dictionary catalog**, built as a production-shaped data platform destined for GCP/GKE. The core asset is a catalog of **500 data-dictionary YAMLs** (a handful of real masked dictionaries plus synthetic neighbors generated in the same format) spanning **10 logistics business domains**. A Spark pipeline validates the catalog against a data-quality contract, extracts table- and field-level **lineage**, and assembles per-table **text blobs ready for embedding** — the retrieval corpus for the RAG layer. Everything runs identically on a laptop (Docker Compose) and is packaged for production (multi-stage, non-root image driven by env vars for GKE SparkApplication CRDs).

**Why it matters:** the pipeline demonstrates end-to-end data-platform discipline — schema contracts, fail-closed DQ gates with quarantine, deterministic synthetic data, lineage as a first-class dataset, performance tuning at 10× scale, and a dev/prod Docker split — while producing a real retrieval corpus for grounded Q&A over enterprise metadata.

## Data flow

```
 data/real/*.yaml            scripts/generate_yamls.py
 (masked real dicts)         scripts/generate_yamls_gemini.py   (Gemini-authored
        │                           │                            narratives, cached)
        └────────────┬──────────────┘
                     ▼
              data/raw/  (~500 YAMLs, one per table)
                     │
                     ▼
   ┌─ 1. ingest_catalog ────────────────────────────────────┐
   │  parse YAML → validate vs schema + DQ contract          │
   │  ├── data/parquet/catalog/      (clean rows)            │
   │  ├── data/parquet/quarantine/   (rejects + reasons)     │
   │  └── data/parquet/dq_report/    (aggregate stats)       │
   │  raises DQContractViolation if thresholds breach        │
   └──────────────────────────────────────────────────────────┘
                     │
                     ▼
   ┌─ 2. extract_lineage ───────────────────────────────────┐
   │  parse field sources → classify source kinds            │
   │  ├── data/parquet/table_lineage/                         │
   │  └── data/parquet/field_lineage/                         │
   └──────────────────────────────────────────────────────────┘
                     │
                     ▼
   ┌─ 3. build_embedding_input ─────────────────────────────┐
   │  catalog + table_lineage → one text_blob per table       │
   │  └── data/parquet/embedding_input/                       │
   └──────────────────────────────────────────────────────────┘
                     │
                     ▼
   ┌─ 4. build_embeddings ──────────────────────────────────┐
   │  text_blob → gemini-embedding-001 (768d, normalized,     │
   │  JSON-cached API calls, fail-closed contract)            │
   │  └── data/parquet/embeddings/                            │
   └──────────────────────────────────────────────────────────┘
                     │
                     ▼
   ┌─ 5. serve (web UI + API) ──────────────────────────────┐
   │  FastAPI · python -m serve · http://localhost:8008        │
   │  /api/search — cosine top-k retrieval                     │
   │  /api/ask    — Gemini answer grounded ONLY in retrieved   │
   │                entries, with cited sources                │
   └──────────────────────────────────────────────────────────┘
   (CLI alternative: scripts/search_catalog.py)
```

Each stage reads only the previous stage's Parquet, so any stage can be re-run in isolation. Contracts for each hop are documented in-repo: [`spark_jobs/dq_contract.md`](spark_jobs/dq_contract.md), [`spark_jobs/LINEAGE_SCHEMAS.md`](spark_jobs/LINEAGE_SCHEMAS.md), [`spark_jobs/EMBEDDING_INPUT.md`](spark_jobs/EMBEDDING_INPUT.md).

## How to use

### Prerequisites

- **uv** (Python 3.12) for host-side work: generators, schema tests
- **Docker Desktop** for the Spark pipeline (`docker compose` v2)
- Optional: a `GEMINI_API_KEY` in `.env` for the Gemini narrative generator

### 1. Set up

```bash
uv sync                 # installs deps incl. pyspark 3.5.3, google-genai
```

### 2. Build the catalog (only if data/raw is empty)

```bash
uv run python scripts/generate_yamls.py          # template-based, seeded/deterministic
# or richer narratives (needs GEMINI_API_KEY; caches to scripts/.gemini_cache.json):
uv run python scripts/generate_yamls_gemini.py
```

Real masked dictionaries live in `data/real/` (gitignored) and are copied in alongside the synthetic ones.

### 3. Run the Spark pipeline (Docker)

`./run` wraps `docker compose run` against the dev image (bind-mounts the repo at `/app`, Spark UI on `localhost:4040`):

```bash
./run python -m spark_jobs ingest_catalog          # → catalog + quarantine + dq_report
./run python -m spark_jobs extract_lineage         # → table_lineage + field_lineage
./run python -m spark_jobs build_embedding_input   # → embedding_input
./run python -m spark_jobs build_embeddings        # → embeddings (needs GEMINI_API_KEY)
```

Then query the catalog semantically:

```bash
uv run python scripts/search_catalog.py "which table tracks COD collections?" --top-k 5
```

Embedding contract, model config, and the no-vector-DB rationale are in [`spark_jobs/EMBEDDINGS.md`](spark_jobs/EMBEDDINGS.md). API responses are cached in `data/.embedding_cache.json`, so re-runs only embed changed blobs.

Or use the web UI — search + grounded Q&A over the catalog:

```bash
uv run python -m serve        # → http://localhost:8008
```

To strip the synthetic catalog and keep only real dictionaries (e.g. before
an internal production run):

```bash
uv run python scripts/clean_synthetic.py           # dry-run
uv run python scripts/clean_synthetic.py --apply   # delete synthetic YAMLs
export ASKMYDOCS_DQ_MIN_FILES=1 ASKMYDOCS_DQ_MIN_NAMESPACES=1   # resize DQ gates
# then re-run the pipeline stages
```

The first `./run` builds the `askmydocs-spark:dev` image (~1 min warm, longer cold — pyspark is a 300 MB wheel).

### 4. Run the tests

```bash
uv run pytest                                # host: schema + lineage-parser tests (500+ cases)
./run python -m pytest tests/               # container: adds the parquet integrity tests
```

Note: `test_lineage_integrity.py` and `test_embedding_input_integrity.py` hardcode `/app/data/parquet/...`, so they only pass **inside the container after the pipeline has run**. On the host they error with `PATH_NOT_FOUND` — expected, not a regression.

### 5. Performance experiments (optional)

```bash
./run python -m spark_jobs.perf_baseline     # naive: no repartition/cache/coalesce
./run python -m spark_jobs.perf_optimized    # repartition(8) + cache + coalesce + AQE
uv run python scripts/compare_perf.py        # before/after table
```

See [`spark_jobs/PERF_NOTES.md`](spark_jobs/PERF_NOTES.md) for the 10×-scale (5,000-file) methodology.

### 6. Production image

```bash
docker build -f docker/Dockerfile.spark.prod -t askmydocs-spark:prod .
docker run --rm askmydocs-spark:prod extract_lineage    # entrypoint dispatches jobs
```

Paths are env-driven (`ASKMYDOCS_RAW_DIR`, `ASKMYDOCS_OUT_DIR`) so the same image points at GCS in GKE. Full details in [`docker/DEPLOYMENT.md`](docker/DEPLOYMENT.md).

## Key points

1. **Fail-closed data quality.** Ingestion is a contract, not a best effort: per-row rejects go to quarantine with reasons; the whole job aborts if rejection rate > 5%, file count < 400, one cause dominates rejects (> 50%), or fewer than 8 domains survive. No partial/dirty Parquet is ever written.

2. **Deterministic, realistic synthetic data.** Generators are seeded (`seed=42`) so the 500-file catalog is reproducible. The Gemini variant only LLM-authors the narrative fields (purpose/granularity/business rules) — the part retrieval quality depends on — while ~7,500 field entries stay template-based for cost; responses are cached so re-runs are free.

3. **Lineage is a first-class dataset.** Field `source` strings are parsed and classified into table-level and field-level lineage Parquet, with cross-file integrity tests (every referenced upstream table must exist in the catalog, no self-references).

4. **One image, many jobs.** `python -m spark_jobs <job>` is the single entry point; in GKE each SparkApplication CRD just sets `args: ["<job>"]`. Dev image bind-mounts code; prod image is multi-stage (~40% smaller), non-root (UID 1001), and pip-free at runtime for a reduced supply-chain surface.

5. **Two ingestion styles kept for comparison.** `ingest_v1_text_udf.py` (DataFrame + UDF) and `ingest_v2_wholetextfiles.py` (RDD) are retained as Day-2 experiments; `ingest_catalog.py` is the canonical path. The small-file problem (500 × ~4 KB files vs Spark task overhead) motivated the perf work — see [`spark_jobs/INGESTION_NOTES.md`](spark_jobs/INGESTION_NOTES.md).

6. **Adjacent workstreams.** `dbt_project/` holds the dbt/BigQuery models (Week 2 schema-evolution snapshots planned); `data-platform-lab/` is a self-contained Docker → Kafka (Redpanda) → dbt → Kubernetes practice lab, independent of the main pipeline.

7. **Multi-machine development.** The repo is developed from both a Mac and a Windows machine (`scripts/*.ps1`, `*.bat` are the Windows helpers). Always `git fetch` and check for divergence before pushing.

## Repository map

| Path | What it is |
|---|---|
| `data/real/` → `data/raw/` | masked real dicts → combined 500-YAML catalog (both gitignored) |
| `schemas/` | JSON Schema for the YAML contract + domain topology doc |
| `scripts/` | catalog generators (template + Gemini), perf comparison, DQ corruption tool |
| `spark_jobs/` | the 3-stage pipeline, perf variants, and per-stage contract docs |
| `tests/` | schema validation (500 cases), lineage parsers, container-only integrity tests |
| `docker/` | dev + prod Spark images, compose file, deployment guide |
| `dbt_project/` | dbt models for BigQuery |
| `data-platform-lab/` | standalone Kafka/dbt/k8s learning lab |
| `run` | wrapper: `./run <cmd>` runs `<cmd>` in the dev Spark container |
