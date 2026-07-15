"""Unit tests for the pure embedding helpers. No Spark, no network."""
from __future__ import annotations

import math

import pytest

from spark_jobs.embeddings_core import (
    MAX_EMBED_CHARS,
    content_key,
    cosine_top_k,
    l2_normalize,
    load_cache,
    save_cache,
    truncate_for_embedding,
)


# --- content_key -----------------------------------------------------------

def test_content_key_is_stable():
    assert content_key("m", 768, "RETRIEVAL_DOCUMENT", "hello") == \
           content_key("m", 768, "RETRIEVAL_DOCUMENT", "hello")


@pytest.mark.parametrize("a,b", [
    (("m", 768, "RETRIEVAL_DOCUMENT", "hello"), ("m", 768, "RETRIEVAL_DOCUMENT", "world")),
    (("m", 768, "RETRIEVAL_DOCUMENT", "hello"), ("m", 512, "RETRIEVAL_DOCUMENT", "hello")),
    (("m", 768, "RETRIEVAL_DOCUMENT", "hello"), ("m", 768, "RETRIEVAL_QUERY", "hello")),
    (("m", 768, "RETRIEVAL_DOCUMENT", "hello"), ("m2", 768, "RETRIEVAL_DOCUMENT", "hello")),
])
def test_content_key_differs(a, b):
    assert content_key(*a) != content_key(*b)


# --- l2_normalize ----------------------------------------------------------

def test_l2_normalize_unit_norm():
    vec = l2_normalize([3.0, 4.0])
    assert vec == pytest.approx([0.6, 0.8])
    assert math.hypot(*vec) == pytest.approx(1.0)


def test_l2_normalize_rejects_zero_vector():
    with pytest.raises(ValueError):
        l2_normalize([0.0, 0.0, 0.0])


# --- truncate_for_embedding -------------------------------------------------

def test_truncate_short_text_untouched():
    assert truncate_for_embedding("abc") == "abc"


def test_truncate_long_text_capped():
    long = "x" * (MAX_EMBED_CHARS + 500)
    assert len(truncate_for_embedding(long)) == MAX_EMBED_CHARS


# --- cosine_top_k -----------------------------------------------------------

def test_cosine_top_k_ranks_by_similarity():
    docs = [
        [1.0, 0.0],   # 0: identical to query
        [0.0, 1.0],   # 1: orthogonal
        [0.6, 0.8],   # 2: partial match
    ]
    hits = cosine_top_k([1.0, 0.0], docs, k=3)
    assert [i for i, _ in hits] == [0, 2, 1]
    assert hits[0][1] == pytest.approx(1.0)
    assert hits[2][1] == pytest.approx(0.0)


def test_cosine_top_k_caps_at_corpus_size():
    hits = cosine_top_k([1.0, 0.0], [[1.0, 0.0]], k=10)
    assert len(hits) == 1


# --- cache round-trip --------------------------------------------------------

def test_cache_roundtrip(tmp_path):
    path = tmp_path / "cache.json"
    assert load_cache(path) == {}
    save_cache(path, {"k1": [0.1, 0.2]})
    assert load_cache(path) == {"k1": [0.1, 0.2]}
