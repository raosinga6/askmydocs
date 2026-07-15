# Embeddings Contract

Produced by `python -m spark_jobs build_embeddings` from `embedding_input/`.
Consumed by `scripts/search_catalog.py` (and later the RAG serving layer).

## Output schema (one row per table, `data/parquet/embeddings/`)

| column | type | notes |
|---|---|---|
| table_name | string | unique, matches embedding_input |
| business_domain | string | carried through for filtering |
| text_blob | string | exact text that was embedded (pre-truncation) |
| embedding | array\<float\> | unit-normalized, `embedding_dims` long |
| embedding_model | string | e.g. `gemini-embedding-001` |
| embedding_dims | int | e.g. 768 |
| content_sha256 | string | cache key: sha256(model\|dims\|task\|text_blob) |
| embedded_at | string | UTC ISO timestamp of the build |

## Model configuration

- **Model**: `gemini-embedding-001` (override: `ASKMYDOCS_EMBED_MODEL`)
- **Dims**: 768 via `output_dimensionality` (override: `ASKMYDOCS_EMBED_DIMS`).
  Truncated vectors are **not** pre-normalized by the API, so the job
  re-normalizes every vector — retrieval assumes cosine == dot product.
- **Task types**: documents are embedded with `RETRIEVAL_DOCUMENT`, queries
  with `RETRIEVAL_QUERY`. Never mix them — the model embeds the two sides
  asymmetrically on purpose.
- **Truncation**: blobs are cut at 8,000 chars before the API call (model
  limit is 2,048 tokens). Blobs front-load purpose/granularity/rules, so
  only the tail of long field lists is lost.

## Invariants (fail closed — job raises, nothing written)

1. Output row count == embedding_input row count
2. `table_name` unique
3. Every vector exactly `embedding_dims` long
4. `GEMINI_API_KEY` present (env or `.env`)

`tests/test_embeddings_integrity.py` re-checks 1–3 plus unit norms against
the written Parquet (skips when the dataset hasn't been built).

## Caching

Every (model, dims, task, text) → vector result is cached in
`data/.embedding_cache.json` (gitignored, checkpointed every 50 calls).
Unchanged blobs cost zero API calls on re-runs; editing one YAML re-embeds
only that table.

## Why brute-force cosine instead of a vector database

500 vectors × 768 dims is ~1.5 MB — a numpy dot product scans it in
microseconds. An ANN index (FAISS/pgvector/Vertex Vector Search) buys nothing
below ~100k vectors and adds an infra dependency. The Parquet layout is the
interface; a vector DB can be swapped in behind `search_catalog.py` when scale
demands it.
