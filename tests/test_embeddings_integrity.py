"""Integrity checks for the embeddings Parquet against EMBEDDINGS.md.

Reads with pyarrow (no Spark session needed) and skips cleanly when the
dataset hasn't been built yet, instead of erroring like the older
integrity suites. Point at non-default paths with:

    ASKMYDOCS_EMBEDDINGS_OUT=... ASKMYDOCS_EMBEDDING_IN=... pytest
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

REPO = Path(__file__).resolve().parent.parent

EMBEDDINGS_DIR = Path(os.environ.get(
    "ASKMYDOCS_EMBEDDINGS_OUT", REPO / "data" / "parquet" / "embeddings"))
EMBEDDING_INPUT_DIR = Path(os.environ.get(
    "ASKMYDOCS_EMBEDDING_IN", REPO / "data" / "parquet" / "embedding_input"))


def _read(path: Path):
    import pyarrow.parquet as pq

    if not path.exists():
        pytest.skip(f"{path} not built yet — run the pipeline first")
    table = pq.read_table(path)
    if table.num_rows == 0:
        pytest.skip(f"{path} is empty (parquet data files are gitignored)")
    return table


@pytest.fixture(scope="module")
def embeddings():
    return _read(EMBEDDINGS_DIR)


@pytest.fixture(scope="module")
def embedding_input():
    return _read(EMBEDDING_INPUT_DIR)


def test_row_count_matches_embedding_input(embeddings, embedding_input):
    assert embeddings.num_rows == embedding_input.num_rows


def test_table_names_unique_and_match_input(embeddings, embedding_input):
    emb_names = set(embeddings["table_name"].to_pylist())
    in_names = set(embedding_input["table_name"].to_pylist())
    assert len(emb_names) == embeddings.num_rows, "duplicate table_name"
    assert emb_names == in_names


def test_vectors_have_declared_dims(embeddings):
    dims = set(embeddings["embedding_dims"].to_pylist())
    assert len(dims) == 1, f"mixed dims in one dataset: {dims}"
    (declared,) = dims
    lengths = {len(v) for v in embeddings["embedding"].to_pylist()}
    assert lengths == {declared}


def test_vectors_unit_normalized(embeddings):
    m = np.asarray(embeddings["embedding"].to_pylist(), dtype=np.float32)
    norms = np.linalg.norm(m, axis=1)
    assert np.allclose(norms, 1.0, atol=1e-3), \
        f"norms outside [1±1e-3]: min={norms.min()}, max={norms.max()}"


def test_vectors_finite_and_nonconstant(embeddings):
    m = np.asarray(embeddings["embedding"].to_pylist(), dtype=np.float32)
    assert np.isfinite(m).all(), "NaN/Inf in embeddings"
    # 500 distinct tables must not all embed to the same point
    assert len(np.unique(m.round(4), axis=0)) > 1


def test_single_model_recorded(embeddings):
    assert len(set(embeddings["embedding_model"].to_pylist())) == 1
