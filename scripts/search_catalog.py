"""Semantic search over the embedded catalog.

Usage:
    uv run python scripts/search_catalog.py "which table tracks COD remittance?"
    uv run python scripts/search_catalog.py "driver route attempts" --top-k 3

Loads data/parquet/embeddings/ with pyarrow (no Spark needed — searching is
interactive, indexing is the batch job), embeds the query with the same
model/dims recorded in the Parquet (task_type=RETRIEVAL_QUERY), and ranks by
cosine similarity.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from spark_jobs.embeddings_core import cosine_top_k, l2_normalize  # noqa: E402

DEFAULT_EMBEDDINGS_DIR = REPO / "data" / "parquet" / "embeddings"


def embed_query(text: str, model: str, dims: int) -> list[float]:
    from dotenv import load_dotenv
    from google import genai
    from google.genai import types

    load_dotenv()
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    resp = client.models.embed_content(
        model=model,
        contents=text,
        config=types.EmbedContentConfig(
            task_type="RETRIEVAL_QUERY",
            output_dimensionality=dims,
        ),
    )
    return l2_normalize(list(resp.embeddings[0].values))


def purpose_snippet(text_blob: str, max_len: int = 220) -> str:
    """The blob's 'Purpose:' section, trimmed for display."""
    for section in text_blob.split("\n\n"):
        if section.startswith("Purpose:"):
            text = section[len("Purpose:"):].strip().replace("\n", " ")
            return text if len(text) <= max_len else text[: max_len - 1] + "…"
    return text_blob[:max_len]


def main() -> int:
    parser = argparse.ArgumentParser(description="Semantic search over the catalog")
    parser.add_argument("query", help="natural-language question")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--embeddings-dir", type=Path,
                        default=Path(os.environ.get(
                            "ASKMYDOCS_EMBEDDINGS_OUT", DEFAULT_EMBEDDINGS_DIR)))
    args = parser.parse_args()

    import pyarrow.parquet as pq

    table = pq.read_table(args.embeddings_dir)
    if table.num_rows == 0:
        print(f"No embeddings at {args.embeddings_dir} — "
              "run: python -m spark_jobs build_embeddings", file=sys.stderr)
        return 1

    model = table["embedding_model"][0].as_py()
    dims = table["embedding_dims"][0].as_py()
    doc_vecs = table["embedding"].to_pylist()

    query_vec = embed_query(args.query, model, dims)
    hits = cosine_top_k(query_vec, doc_vecs, args.top_k)

    names = table["table_name"].to_pylist()
    domains = table["business_domain"].to_pylist()
    blobs = table["text_blob"].to_pylist()

    print(f"\nTop {len(hits)} of {table.num_rows} tables for: {args.query!r}\n")
    for rank, (i, score) in enumerate(hits, 1):
        print(f"{rank}. [{score:.3f}] {names[i]}  ({domains[i]})")
        print(f"   {purpose_snippet(blobs[i])}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
