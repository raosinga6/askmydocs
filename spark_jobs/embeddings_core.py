"""Pure helpers for the embedding layer: cache keys, normalization, search.

No Spark, no network — everything here is unit-testable on the host.
build_embeddings.py (indexing) and scripts/search_catalog.py (querying)
both import from this module so document- and query-side math never drifts.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from pathlib import Path

# Hard cap on characters sent to the embedding model. gemini-embedding-001
# accepts 2048 tokens; ~4 chars/token puts 8000 chars safely inside the limit.
# The text_blob front-loads purpose/granularity/business_rules, so truncation
# only ever trims the tail of the field list.
MAX_EMBED_CHARS = 8000


def content_key(model: str, dims: int, task_type: str, text: str) -> str:
    """Stable cache key: same (model, dims, task, text) -> same embedding."""
    h = hashlib.sha256()
    h.update(f"{model}|{dims}|{task_type}|".encode("utf-8"))
    h.update(text.encode("utf-8"))
    return h.hexdigest()


def truncate_for_embedding(text: str, max_chars: int = MAX_EMBED_CHARS) -> str:
    return text if len(text) <= max_chars else text[:max_chars]


def l2_normalize(vec: list[float]) -> list[float]:
    """Unit-normalize. Required: gemini-embedding-001 vectors are only
    pre-normalized at the full 3072 dims; truncated dims (768) are not."""
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        raise ValueError("zero-norm embedding vector")
    return [v / norm for v in vec]


def cosine_top_k(
    query_vec: list[float],
    doc_vecs: list[list[float]],
    k: int,
) -> list[tuple[int, float]]:
    """Return [(doc_index, score)] for the k best matches, best first.

    Assumes all vectors are unit-normalized, so cosine == dot product.
    Brute force is the right call at catalog scale (500 rows; fine to ~100k).
    """
    import numpy as np

    q = np.asarray(query_vec, dtype=np.float32)
    m = np.asarray(doc_vecs, dtype=np.float32)
    scores = m @ q
    k = min(k, len(doc_vecs))
    top = np.argpartition(-scores, k - 1)[:k]
    top = top[np.argsort(-scores[top])]
    return [(int(i), float(scores[i])) for i in top]


def load_cache(path: Path) -> dict[str, list[float]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_cache(path: Path, cache: dict[str, list[float]]) -> None:
    """Atomic write so a crash mid-save can't corrupt the cache."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(cache, f)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
