# Local Testing Runbook

Step-by-step guide to run the full AskMyDocs pipeline on your machine and
verify every stage. All commands run from the repo root. Every expected
output below was verified on macOS (Apple Silicon) on 2026-07-15.

There are two ways to run the Spark stages — pick one:

- **Path A — host (uv)**: fastest iteration, needs Java 17+ on your machine
- **Path B — Docker (`./run`)**: no host Java needed, needs Docker Desktop

---

## 0. Prerequisites

| Tool | Check | Notes |
|---|---|---|
| uv | `uv --version` | manages Python 3.12 venv |
| Java 17+ | `java -version` | Path A only (pyspark driver) |
| Docker Desktop | `docker compose version` | Path B only |
| Gemini API key | — | only for `build_embeddings` + search |

```bash
git clone https://github.com/raosinga6/askmydocs && cd askmydocs
uv sync                        # installs pyspark 3.5.3, google-genai, pyarrow, numpy, pytest
```

Create `.env` in the repo root (needed for embedding steps only):

```
GEMINI_API_KEY=<your key>
```

## 1. Get the catalog data (`data/raw/`)

`data/raw/` and `data/real/` are **gitignored**, so a fresh clone has no data.

- If you have the 5 real masked YAMLs, place them in `data/real/` first.
- Generate the 500-file catalog (deterministic, seeded):

```bash
uv run python scripts/generate_yamls.py            # template narratives, offline
# — or, richer LLM-written narratives (uses GEMINI_API_KEY, cached):
uv run python scripts/generate_yamls_gemini.py
```

**Verify:** `ls data/raw | wc -l` → `500`

## 2. Validate the YAML contract (no Spark needed)

```bash
uv run pytest tests/test_yaml_validity.py tests/test_lineage_parsers.py -q
```

**Expect:** all pass (~519 tests, ~30 s). If `test_real_yamls_present` fails,
`data/real/` is empty — see step 1.

## 3. Run the Spark pipeline

### Path A — on the host

The jobs default to container paths (`/app/...`), so point them at the repo:

```bash
export ASKMYDOCS_RAW_DIR=$PWD/data/raw \
       ASKMYDOCS_OUT_DIR=$PWD/data/parquet \
       ASKMYDOCS_CATALOG_IN=$PWD/data/parquet/catalog \
       ASKMYDOCS_TABLE_LINEAGE_OUT=$PWD/data/parquet/table_lineage \
       ASKMYDOCS_FIELD_LINEAGE_OUT=$PWD/data/parquet/field_lineage \
       ASKMYDOCS_TABLE_LINEAGE_IN=$PWD/data/parquet/table_lineage \
       ASKMYDOCS_EMBEDDING_IN=$PWD/data/parquet/embedding_input \
       ASKMYDOCS_EMBEDDING_OUT=$PWD/data/parquet/embedding_input \
       ASKMYDOCS_EMBEDDINGS_OUT=$PWD/data/parquet/embeddings \
       ASKMYDOCS_EMBED_CACHE=$PWD/data/.embedding_cache.json

uv run python -m spark_jobs ingest_catalog
uv run python -m spark_jobs extract_lineage
uv run python -m spark_jobs build_embedding_input
```

### Path B — in Docker

```bash
./run python -m spark_jobs ingest_catalog
./run python -m spark_jobs extract_lineage
./run python -m spark_jobs build_embedding_input
```

(First `./run` builds the `askmydocs-spark:dev` image; Spark UI at
`localhost:4040` while a job runs.)

**Verify after `ingest_catalog`** — the console prints the DQ report; expect:

```
"rejection_rate": 0.0
Wrote 500 good rows to .../data/parquet/catalog
Wrote 0 quarantined rows ...
```

A non-zero quarantine count is data trouble, not a crash — inspect
`data/parquet/quarantine/`. The job aborts entirely if DQ thresholds breach
(see `spark_jobs/dq_contract.md`).

**Verify after all three:** `ls data/parquet` →
`catalog dq_report embedding_input field_lineage quarantine table_lineage`

## 4. Build the embeddings (needs GEMINI_API_KEY)

```bash
uv run python -m spark_jobs build_embeddings     # Path A (env from step 3 still set)
# or: ./run python -m spark_jobs build_embeddings
```

**Expect:**

```
500 blobs | 0 cached | 500 to embed via gemini-embedding-001
  embedded 50/500 ... embedded 500/500
Wrote 500 embeddings (gemini-embedding-001, 768 dims) to .../data/parquet/embeddings
```

Takes ~1–2 min on first run. Re-runs print `500 cached | 0 to embed` and
finish instantly — results are cached in `data/.embedding_cache.json`.

## 5. Test semantic search

```bash
uv run python scripts/search_catalog.py "which table tracks cash on delivery collections?" --top-k 3
```

**Expect:** the `cod_collections_*` tables ranked first with scores ≈ 0.76,
each with a purpose snippet. Try your own logistics questions (PETS tickets,
recovery facilities, delivery attempts, hub sweeps).

## 6. Run the full test suite

```bash
uv run pytest -q
```

**Expect on the host:** ~561 passed and **13 errors** — the errors are
`test_lineage_integrity.py` / `test_embedding_input_integrity.py`, which
hardcode `/app/...` container paths. They are not regressions. To run those
too, use the container after the pipeline has produced parquet inside it:

```bash
./run python -m pytest tests/ -q
```

The newer suites (`test_embeddings_core.py`, `test_embeddings_integrity.py`)
run anywhere — integrity tests skip with a clear message if the parquet
hasn't been built.

## 7. Optional extras

```bash
# Performance comparison at 10x scale (see spark_jobs/PERF_NOTES.md)
uv run python -m spark_jobs.perf_baseline
uv run python -m spark_jobs.perf_optimized
uv run python scripts/compare_perf.py

# Production image smoke test
docker build -f docker/Dockerfile.spark.prod -t askmydocs-spark:prod .
docker run --rm askmydocs-spark:prod ingest_catalog   # entrypoint dispatches jobs
```

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `PATH_NOT_FOUND: /app/data/...` | Job or test ran on host without the step-3 env exports |
| `JAVA_GATEWAY_EXITED` / no `java` | Install Java 17+ (Path A) or use `./run` (Path B) |
| `KeyError: 'GEMINI_API_KEY'` | Create `.env` (step 0) or export the var |
| `no rows in .../embedding_input` | Run the three step-3 stages first, in order |
| `test_real_yamls_present` fails | `data/real/` empty — masked real YAMLs not on this machine |
| Docker build slow (~5 min) | One-time: pyspark wheel is ~330 MB; later builds hit cache |
| Port 4040 busy | Another Spark UI is up; harmless, Spark picks 4041 |
