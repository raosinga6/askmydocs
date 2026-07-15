"""Embed the catalog: embedding_input text blobs -> vectors in Parquet.

Inputs:
- /app/data/parquet/embedding_input/   (Day 5 output)

Output:
- /app/data/parquet/embeddings/

Model: gemini-embedding-001 (task_type=RETRIEVAL_DOCUMENT), truncated to 768
dims and re-normalized. Queries must use the same model/dims with
task_type=RETRIEVAL_QUERY — see scripts/search_catalog.py.

API calls fan out over a thread pool with retry + JSON cache, the same
pattern as scripts/generate_yamls_gemini.py, so re-runs are free.

See EMBEDDINGS.md for the output contract. Raises EmbeddingContractViolation
if the output would break it (fail closed, like ingest_catalog).
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from spark_jobs.embeddings_core import (
    content_key,
    l2_normalize,
    load_cache,
    resolve_gemini_api_key,
    save_cache,
    truncate_for_embedding,
)
from spark_jobs.spark_session import get_spark

EMBEDDING_INPUT_IN = Path(os.environ.get(
    "ASKMYDOCS_EMBEDDING_IN", "/app/data/parquet/embedding_input"))
EMBEDDINGS_OUT = Path(os.environ.get(
    "ASKMYDOCS_EMBEDDINGS_OUT", "/app/data/parquet/embeddings"))
CACHE_PATH = Path(os.environ.get(
    "ASKMYDOCS_EMBED_CACHE", "/app/data/.embedding_cache.json"))

MODEL = os.environ.get("ASKMYDOCS_EMBED_MODEL", "gemini-embedding-001")
DIMS = int(os.environ.get("ASKMYDOCS_EMBED_DIMS", "768"))
TASK_TYPE = "RETRIEVAL_DOCUMENT"
WORKERS = 8
RETRY_LIMIT = 3
RETRY_BACKOFF_S = 4.0

EMBEDDINGS_SCHEMA = StructType([
    StructField("table_name", StringType(), nullable=False),
    StructField("business_domain", StringType(), nullable=False),
    StructField("text_blob", StringType(), nullable=False),
    StructField("embedding", ArrayType(FloatType()), nullable=False),
    StructField("embedding_model", StringType(), nullable=False),
    StructField("embedding_dims", IntegerType(), nullable=False),
    StructField("content_sha256", StringType(), nullable=False),
    StructField("embedded_at", StringType(), nullable=False),
])


class EmbeddingContractViolation(Exception):
    """Output would violate EMBEDDINGS.md — nothing is written."""


def make_client():
    from google import genai

    api_key = resolve_gemini_api_key()
    if not api_key:
        raise EmbeddingContractViolation(
            "GEMINI_API_KEY not set (checked env and .env)")
    return genai.Client(api_key=api_key)


def embed_one(client, text: str) -> list[float]:
    """Embed a single document with retry. Returns a unit-normalized vector."""
    from google.genai import types

    last_err: Exception | None = None
    for attempt in range(RETRY_LIMIT):
        try:
            resp = client.models.embed_content(
                model=MODEL,
                contents=truncate_for_embedding(text),
                config=types.EmbedContentConfig(
                    task_type=TASK_TYPE,
                    output_dimensionality=DIMS,
                ),
            )
            vec = list(resp.embeddings[0].values)
            if len(vec) != DIMS:
                raise ValueError(f"expected {DIMS} dims, got {len(vec)}")
            return l2_normalize(vec)
        except Exception as e:  # noqa: BLE001 — retry any transient API error
            last_err = e
            time.sleep(RETRY_BACKOFF_S * (2 ** attempt))
    raise EmbeddingContractViolation(
        f"embedding failed after {RETRY_LIMIT} attempts: {last_err}")


def embed_all(rows: list[dict]) -> list[dict]:
    """Embed every row, using the cache; returns rows + embedding columns."""
    client = None
    cache = load_cache(CACHE_PATH)
    now = datetime.now(timezone.utc).isoformat()

    to_fetch: dict[str, str] = {}  # key -> text
    for row in rows:
        key = content_key(MODEL, DIMS, TASK_TYPE, row["text_blob"])
        row["_key"] = key
        if key not in cache:
            to_fetch[key] = row["text_blob"]

    print(f"{len(rows)} blobs | {len(rows) - len(to_fetch)} cached | "
          f"{len(to_fetch)} to embed via {MODEL}", flush=True)

    if to_fetch:
        client = make_client()
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {
                pool.submit(embed_one, client, text): key
                for key, text in to_fetch.items()
            }
            done = 0
            for fut in as_completed(futures):
                cache[futures[fut]] = fut.result()  # raises on hard failure
                done += 1
                if done % 50 == 0:
                    print(f"  embedded {done}/{len(to_fetch)}", flush=True)
                    save_cache(CACHE_PATH, cache)  # checkpoint
        save_cache(CACHE_PATH, cache)

    out = []
    for row in rows:
        vec = cache[row["_key"]]
        out.append({
            "table_name": row["table_name"],
            "business_domain": row["business_domain"],
            "text_blob": row["text_blob"],
            "embedding": vec,
            "embedding_model": MODEL,
            "embedding_dims": DIMS,
            "content_sha256": row["_key"],
            "embedded_at": now,
        })
    return out


def check_contract(input_count: int, out_rows: list[dict]) -> None:
    if len(out_rows) != input_count:
        raise EmbeddingContractViolation(
            f"row count mismatch: {input_count} inputs, {len(out_rows)} embeddings")
    names = {r["table_name"] for r in out_rows}
    if len(names) != len(out_rows):
        raise EmbeddingContractViolation("duplicate table_name in output")
    for r in out_rows:
        if len(r["embedding"]) != DIMS:
            raise EmbeddingContractViolation(
                f"{r['table_name']}: {len(r['embedding'])} dims, expected {DIMS}")


def main() -> None:
    spark = get_spark("build_embeddings")

    embedding_input = spark.read.parquet(str(EMBEDDING_INPUT_IN))
    rows = [
        {"table_name": r.table_name,
         "business_domain": r.business_domain,
         "text_blob": r.text_blob}
        for r in embedding_input
        .select("table_name", "business_domain", "text_blob")
        .collect()
    ]
    if not rows:
        raise EmbeddingContractViolation(f"no rows in {EMBEDDING_INPUT_IN}")

    out_rows = embed_all(rows)
    check_contract(len(rows), out_rows)

    df = spark.createDataFrame(out_rows, schema=EMBEDDINGS_SCHEMA)
    df.coalesce(1).write.mode("overwrite").parquet(str(EMBEDDINGS_OUT))
    print(f"Wrote {len(out_rows)} embeddings ({MODEL}, {DIMS} dims) "
          f"to {EMBEDDINGS_OUT}", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
