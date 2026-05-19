"""Generate 499 synthetic data dictionary YAMLs with Gemini-authored narratives.

Strategy:
- Field-level content (name, description, source, technical_description) is template-based,
  same as the original generator. We have ~7500 fields across 499 tables, doing that with
  Gemini would be slow and expensive.
- Narrative content (overview.purpose, overview.granularity, overview.business_rules) is
  Gemini-authored per table. This is where templated text reads as obviously fake, and
  where retrieval quality in Week 4 depends most on data realism.

Concurrency: uses ThreadPoolExecutor to fan out Gemini calls. Default 8 workers.
Caching: results are cached to scripts/.gemini_cache.json so re-runs don't re-spend money.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv
from faker import Faker
from google import genai
from google.genai import types
from jsonschema import validate, ValidationError

# Reuse field/source generation from the original generator.
from generate_yamls import (
    DOMAINS, NAMESPACES, COMMON_FIELD_NAMES,
    FIELD_TECH_PATTERNS_DIRECT, FIELD_TECH_PATTERNS_DERIVED,
    ARCHETYPES, make_table_name, make_source_path, make_field,
    make_input_tables, make_fields, plan_tables, TableSpec,
)

load_dotenv()
random.seed(42)
Faker.seed(42)

REPO = Path(__file__).resolve().parent.parent
SCHEMA = json.loads((REPO / "schemas" / "data_dictionary_schema.json").read_text(encoding="utf-8"))
REAL_DIR = REPO / "data" / "real"
OUT_DIR = REPO / "data" / "raw"
CACHE_PATH = Path(__file__).resolve().parent / ".gemini_cache.json"

MODEL = "gemini-2.5-pro"
WORKERS = 8
RETRY_LIMIT = 3
RETRY_BACKOFF_S = 4.0

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

NARRATIVE_PROMPT = """You are writing a data dictionary entry for an internal logistics data warehouse \
at a Southeast Asian e-commerce company. The company operates last-mile delivery across SG, MY, ID, PH, TH, VN.

Write narrative content for a table with these properties:

- table_name: {table_name}
- business_domain: {domain}
- archetype: {archetype}
- upstream_tables: {input_tables}

Output STRICT JSON matching this shape, no markdown fences, no commentary outside the JSON:

{{
  "purpose": "<2-3 sentences explaining what this table tracks and why it exists. Mention the audience and the analytical question it answers. Refer to upstream tables naturally where it makes sense.>",
  "granularity": "<1-2 sentences describing what one row represents. Be specific about the grain.>",
  "business_rules": [
    "<rule 1 — specific filter, derivation, or constraint that shapes the data>",
    "<rule 2>",
    "<rule 3>",
    "<rule 4>",
    "<rule 5>"
  ]
}}

Style requirements:
- Write like a senior data engineer documenting their own work, not a marketing brief.
- Be concrete: name actual columns, statuses, or thresholds where plausible.
- Business rules should describe filters, joins, derivation logic, or recovery handling — \
not generic statements like "this table is refreshed daily".
- Reference logistics concepts realistically: hubs, scans, sortation, last-mile, recovery facilities, \
PETS tickets, push-off cutoffs, COD, sweeps, route attempts.
- Granularity sentence MUST be at least 20 characters and at most 500 characters.
- Purpose MUST be at least 40 characters and at most 1000 characters.
- Each business rule MUST be at least 10 characters and at most 400 characters.
- Output 3 to 7 business rules.
"""


@dataclass
class NarrativeRequest:
    cache_key: str
    table_name: str
    domain: str
    archetype: str
    input_tables: list[str]


def cache_key_for(table_name: str, domain: str, archetype: str, input_tables: list[str]) -> str:
    """Stable hash so retries hit cache."""
    payload = json.dumps({
        "model": MODEL,
        "table_name": table_name,
        "domain": domain,
        "archetype": archetype,
        "input_tables": sorted(input_tables),
    }, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def load_cache() -> dict:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def call_gemini(req: NarrativeRequest) -> dict:
    """Call Gemini, parse JSON, validate field constraints."""
    prompt = NARRATIVE_PROMPT.format(
        table_name=req.table_name,
        domain=req.domain,
        archetype=req.archetype,
        input_tables=", ".join(req.input_tables),
    )
    last_err: Exception | None = None
    for attempt in range(RETRY_LIMIT):
        try:
            resp = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.9,
                ),
            )
            data = json.loads(resp.text)
            # Light validation — full schema validation happens at write time.
            assert "purpose" in data and len(data["purpose"]) >= 40
            assert "granularity" in data and len(data["granularity"]) >= 20
            assert isinstance(data.get("business_rules"), list) and 3 <= len(data["business_rules"]) <= 7
            return data
        except Exception as e:
            last_err = e
            time.sleep(RETRY_BACKOFF_S * (attempt + 1))
    raise RuntimeError(f"Gemini failed after {RETRY_LIMIT} retries for {req.table_name}: {last_err}")


def fetch_narratives(requests: list[NarrativeRequest], cache: dict) -> dict[str, dict]:
    """Fan out Gemini calls in parallel, return {cache_key: narrative_json}."""
    pending = [r for r in requests if r.cache_key not in cache]
    print(f"Cache hits: {len(requests) - len(pending)} / {len(requests)}")
    if not pending:
        return {r.cache_key: cache[r.cache_key] for r in requests}

    results: dict[str, dict] = {}
    completed = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(call_gemini, r): r for r in pending}
        for fut in as_completed(futures):
            req = futures[fut]
            try:
                narrative = fut.result()
                cache[req.cache_key] = narrative
                results[req.cache_key] = narrative
            except Exception as e:
                print(f"  FAIL {req.table_name}: {e}")
                continue
            completed += 1
            if completed % 25 == 0:
                save_cache(cache)
                print(f"  ...{completed}/{len(pending)} done, cache saved")

    save_cache(cache)
    # Include cache hits.
    for r in requests:
        if r.cache_key in cache and r.cache_key not in results:
            results[r.cache_key] = cache[r.cache_key]
    return results


def build_table(spec: TableSpec, input_tables: list[str], narrative: dict, fields: list[dict]) -> dict:
    return {
        "table_name": spec.name,
        "overview": {
            "purpose": narrative["purpose"],
            "granularity": narrative["granularity"],
            "business_rules": narrative["business_rules"],
        },
        "input_tables": input_tables,
        "fields": fields,
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for p in OUT_DIR.glob("*.yaml"):
        p.unlink()

    # 1. Copy real YAMLs.
    real_files = list(REAL_DIR.glob("*.yaml")) if REAL_DIR.exists() else []
    for src in real_files:
        shutil.copy(src, OUT_DIR / src.name)
    print(f"Copied {len(real_files)} real YAMLs")

    target = 500 - len(real_files)
    specs = plan_tables(target)

    # 2. Dedupe names against real YAMLs.
    seen = {p.stem for p in real_files}
    for spec in specs:
        name = spec.name
        suffix = 0
        while name in seen:
            suffix += 1
            name = f"{spec.name}_v{suffix}"
        spec.name = name
        seen.add(name)

    # 3. Pre-compute input_tables per spec (Gemini needs them in the prompt).
    spec_inputs: dict[str, list[str]] = {}
    for spec in specs:
        spec_inputs[spec.name] = make_input_tables(spec.domain, spec.upstream_count)

    # 4. Build the list of Gemini requests + use cache.
    cache = load_cache()
    requests = [
        NarrativeRequest(
            cache_key=cache_key_for(spec.name, spec.domain, spec.archetype, spec_inputs[spec.name]),
            table_name=spec.name,
            domain=spec.domain,
            archetype=spec.archetype,
            input_tables=spec_inputs[spec.name],
        )
        for spec in specs
    ]
    narratives = fetch_narratives(requests, cache)

    # 5. Build and write each YAML.
    failures: list[tuple[str, str]] = []
    written = 0
    for spec, req in zip(specs, requests):
        if req.cache_key not in narratives:
            failures.append((spec.name, "no narrative from Gemini"))
            continue
        input_tables = spec_inputs[spec.name]
        fields = make_fields(input_tables, spec.field_count)
        doc = build_table(spec, input_tables, narratives[req.cache_key], fields)
        try:
            validate(doc, SCHEMA)
        except ValidationError as e:
            failures.append((spec.name, str(e.message)))
            continue
        (OUT_DIR / f"{spec.name}.yaml").write_text(
            yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, width=120, allow_unicode=True),
            encoding="utf-8",
        )
        written += 1

    total = len(list(OUT_DIR.glob("*.yaml")))
    print(f"\nWrote {written} synthetic + copied {len(real_files)} real = {total} total")
    if failures:
        print(f"\n{len(failures)} failures:")
        for name, msg in failures[:10]:
            print(f"  - {name}: {msg}")


if __name__ == "__main__":
    main()