"""Table-name matching helpers: exact mentions and fuzzy "did you mean".

Pure functions, no corpus/network side effects — unit-tested in
tests/test_name_matching.py. serve.app wires them to the loaded corpus.
"""
from __future__ import annotations

import difflib
import re

_TOKEN_RE = re.compile(r"[a-z][a-z0-9_]{2,}")


def query_tokens(query: str) -> list[str]:
    return _TOKEN_RE.findall(query.lower())


def exact_mentions(query: str, by_name: dict[str, int]) -> list[int]:
    """Indices of tables whose exact name appears as a token in the query."""
    seen = set()
    out = []
    for tok in query_tokens(query):
        i = by_name.get(tok)
        if i is not None and i not in seen:
            seen.add(i)
            out.append(i)
    return out


def identifier_tokens(query: str) -> list[str]:
    """Identifier-looking tokens (contain an underscore), e.g. 'active_orders'."""
    return [t for t in query_tokens(query) if "_" in t]


def fuzzy_name_matches(
    token: str,
    names: list[str],
    limit: int = 5,
    min_score: float = 0.35,
) -> list[str]:
    """Closest table names to a token that isn't an exact match.

    Score blends word-part overlap (dominant — 'active_orders' should surface
    orders_* tables) with sequence similarity (tie-breaker for typos like
    'orders_bsae_007').
    """
    parts = {p for p in token.split("_") if p}
    if not parts:
        return []
    scored: list[tuple[float, str]] = []
    for name in names:
        name_parts = {p for p in name.split("_") if p}
        overlap = len(parts & name_parts) / len(parts)
        ratio = difflib.SequenceMatcher(None, token, name).ratio()
        score = 0.7 * overlap + 0.3 * ratio
        if score >= min_score:
            scored.append((score, name))
    scored.sort(key=lambda t: (-t[0], t[1]))
    return [name for _, name in scored[:limit]]


def did_you_mean(query: str, names: list[str], by_name: dict[str, int],
                 limit: int = 5) -> list[str]:
    """Fuzzy suggestions for identifier tokens that match no exact table name."""
    out: list[str] = []
    for tok in identifier_tokens(query):
        if tok in by_name:
            continue
        for name in fuzzy_name_matches(tok, names, limit=limit):
            if name not in out:
                out.append(name)
    return out[:limit]
