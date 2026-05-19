"""Generate 495 synthetic data dictionary YAMLs matching the real format.

Real masked YAMLs go in data/real/ and are validated separately.
This script produces synthetic neighbors that mimic the same shape and
narrative style closely enough that downstream Spark/dbt/RAG code works
identically on real and synthetic.
"""
from __future__ import annotations

import json
import random
import shutil
from dataclasses import dataclass
from pathlib import Path

import yaml
from faker import Faker
from jsonschema import validate, ValidationError

fake = Faker()
random.seed(42)
Faker.seed(42)

REPO = Path(__file__).resolve().parent.parent
SCHEMA = json.loads((REPO / "schemas" / "data_dictionary_schema.json").read_text())
REAL_DIR = REPO / "data" / "real"
OUT_DIR = REPO / "data" / "raw"

# ---------------------------------------------------------------------------
# Vocabulary — kept simple but plausible for a logistics warehouse.
# ---------------------------------------------------------------------------

NAMESPACES = ["data_warehouse", "core_prod_gl", "metadata"]

DOMAINS = {
    "orders": ["orders", "order_status_events", "order_line_items", "order_returns",
               "order_cancellations", "order_attempts"],
    "shipments": ["shipments", "add_to_shipment_events", "shipment_status_events",
                  "inbound_scans", "outbound_scans", "warehouse_sweeps",
                  "shipment_routes", "delivery_attempts"],
    "hubs": ["hubs", "hubs_enriched", "hub_capacity", "sortation_events",
             "facility_zones", "hub_operating_hours"],
    "fleet": ["drivers", "vehicles", "routes_planned", "routes_actual",
              "last_mile_push_off_cutoffs", "driver_assignments"],
    "customers": ["customers", "shippers", "consignees", "customer_addresses",
                  "customer_contracts"],
    "support": ["pets_tickets", "pets_tickets_enriched", "ticket_escalations",
                "ticket_resolutions", "ticket_categories"],
    "billing": ["invoices", "cod_collections", "settlements", "billing_adjustments"],
    "inventory": ["sku_master", "stock_levels", "warehouse_locations", "stock_movements"],
    "calendar": ["calendar", "operating_calendar", "holiday_calendar"],
    "audit": ["audit_log", "change_events", "data_quality_events"],
}

ARCHETYPES = ["enriched", "base", "events", "snapshot_daily", "snapshot_hourly", "agg"]

GRANULARITY_PATTERNS = [
    "Each row represents a single {entity}.",
    "Each row represents a single {entity} with {attribute} information.",
    "One row per {entity} per day.",
    "One row per {entity} per {attribute}.",
    "Each row corresponds to one {entity} event.",
]

PURPOSE_OPENERS = [
    "Tracks {what} to enable {why}.",
    "Captures {what} for {why}.",
    "Stores {what} used in {why}.",
    "Provides {what} supporting {why}.",
    "Aggregates {what} for analysis of {why}.",
]

PURPOSE_WHAT = [
    "scan-level events across the network",
    "hub-level capacity and utilization",
    "ticket lifecycle and resolution timing",
    "order status transitions",
    "delivery attempt outcomes",
    "shipment routing decisions",
    "billing adjustments and exceptions",
    "driver assignments and route execution",
    "customer-level shipping volumes",
    "warehouse sortation activity",
]

PURPOSE_WHY = [
    "operational reporting",
    "downstream lineage analysis",
    "exception monitoring",
    "SLA tracking",
    "capacity planning",
    "cost allocation",
    "customer-facing dashboards",
    "ad-hoc investigation by ops teams",
    "recovery process auditing",
    "weekly leadership reviews",
]

RULE_PATTERNS = [
    "Only includes records where {predicate}.",
    "Excludes {entity} marked as {flag}.",
    "Joins on {predicate} to enrich with {attribute}.",
    "Filters to {predicate} for the relevant time window.",
    "Flags records where {predicate} for downstream alerting.",
    "Deduplicates on {attribute} keeping the latest {entity}.",
    "Partitioned by {attribute} for performance.",
    "Refreshed {cadence} from upstream sources.",
]

RULE_PREDICATES = [
    "the ticket is resolved or cancelled",
    "scan timestamp is after resolution datetime",
    "facility_type is RECOVERY or name matches recovery patterns",
    "order_status is in ('shipped', 'delivered', 'returned')",
    "shipment is associated with an active route",
    "the calendar date falls within operating hours",
    "the record is not soft-deleted",
    "the event occurred within the last 30 days",
]

CADENCES = ["hourly", "daily", "weekly"]

FIELD_TECH_PATTERNS_DIRECT = [
    "Direct field from source",
    "Direct field from source, used as partition key",
    "Direct field, lowercased",
    "Direct field with null-handling: COALESCE(field, default)",
]

FIELD_TECH_PATTERNS_DERIVED = [
    "Derived field: ROW_NUMBER() OVER (PARTITION BY {pcol} ORDER BY {ocol} DESC) used to pick latest",
    "Derived field: IF({pred}, 1, 0)",
    "Derived field: CASE WHEN {pred} THEN '{a}' ELSE '{b}' END",
    "Derived field: SUM({col}) OVER (PARTITION BY {pcol})",
    "Derived field: from_utc_timestamp({col}, local_timezone)",
    "Derived field: DATEDIFF(day, {col_a}, {col_b})",
    "Derived field: First {col} ranked by {ocol} ASC over partition of {pcol}",
]

COMMON_FIELD_NAMES = [
    "id", "order_id", "shipment_id", "hub_id", "ticket_id", "customer_id",
    "driver_id", "vehicle_id", "sku_id", "invoice_id", "status", "created_at",
    "updated_at", "resolution_datetime", "scan_type_name", "facility_type",
    "hub_name", "address_city", "country_code", "total_amount", "weight_kg",
    "is_active", "is_deleted", "created_month", "created_date",
]


@dataclass
class TableSpec:
    name: str
    domain: str
    archetype: str
    upstream_count: int
    field_count: int


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def make_table_name(domain: str, archetype: str, base: str) -> str:
    """Combine a domain root + archetype into a realistic table name."""
    if archetype == "enriched":
        return f"{base}_enriched"
    if archetype == "base":
        return f"{base}_base"
    if archetype == "events":
        return f"{base}_events" if not base.endswith("events") else base
    if archetype == "snapshot_daily":
        return f"{base}_daily_snapshot"
    if archetype == "snapshot_hourly":
        return f"{base}_hourly_snapshot"
    if archetype == "agg":
        return f"{base}_agg_{random.choice(['daily', 'weekly', 'monthly'])}"
    return base


def make_input_tables(domain: str, k: int) -> list[str]:
    """Pick k upstream tables, weighted toward the same domain but with cross-domain leaks."""
    candidates: list[str] = []
    # Same-domain candidates.
    for t in DOMAINS[domain]:
        candidates.append(f"data_warehouse.{t}")
    # Cross-domain — sample 2-3 other domains.
    for other in random.sample([d for d in DOMAINS if d != domain], k=3):
        for t in DOMAINS[other][:2]:
            candidates.append(f"{random.choice(NAMESPACES)}.{t}")
    return sorted(set(random.sample(candidates, min(k, len(candidates)))))


def make_purpose() -> str:
    opener = random.choice(PURPOSE_OPENERS).format(
        what=random.choice(PURPOSE_WHAT),
        why=random.choice(PURPOSE_WHY),
    )
    detail = random.choice([
        "This table helps {audience} understand {topic}.",
        "Used by {audience} for {topic} analysis.",
        "Supports {audience} in identifying {topic}.",
    ]).format(
        audience=random.choice(["ops teams", "data analysts", "the recovery team",
                                "leadership", "finance", "the on-call engineer"]),
        topic=random.choice(["root causes", "performance trends", "anomalies",
                             "cost drivers", "SLA breaches", "exception patterns"]),
    )
    return f"{opener} {detail}"


def make_granularity(table_name: str) -> str:
    entity = table_name.replace("_", " ").replace("enriched", "").replace("base", "").strip()
    if not entity:
        entity = "record"
    return random.choice(GRANULARITY_PATTERNS).format(
        entity=entity,
        attribute=random.choice(["status", "timestamp", "hub", "facility", "driver", "carrier"]),
    )


def make_business_rules() -> list[str]:
    count = random.randint(3, 7)
    rules = []
    for _ in range(count):
        rules.append(random.choice(RULE_PATTERNS).format(
            predicate=random.choice(RULE_PREDICATES),
            entity=random.choice(["tickets", "shipments", "orders", "scans", "records"]),
            flag=random.choice(["is_deleted", "test", "internal", "voided"]),
            attribute=random.choice(["order_id", "created_at", "hub_id", "ticket_id"]),
            cadence=random.choice(CADENCES),
        ))
    return rules


def make_source_path(input_tables: list[str], n_sources: int = 1) -> str:
    """Generate a realistic source string. Sometimes multi-valued (comma-separated)."""
    chosen = random.sample(input_tables, min(n_sources, len(input_tables)))
    parts = []
    for table in chosen:
        col = random.choice(COMMON_FIELD_NAMES)
        parts.append(f"{table}.{col}")
    return ", ".join(parts)


def make_field(name: str, input_tables: list[str], force_derived: bool = False) -> dict:
    is_derived = force_derived or random.random() < 0.35
    if is_derived:
        n_sources = random.randint(2, min(4, max(2, len(input_tables))))
        source = make_source_path(input_tables, n_sources)
        tech = random.choice(FIELD_TECH_PATTERNS_DERIVED).format(
            pcol=random.choice(COMMON_FIELD_NAMES),
            ocol=random.choice(["created_at", "updated_at", "resolution_datetime"]),
            pred=random.choice(RULE_PREDICATES),
            a=random.choice(["yes", "active", "open", "recovery"]),
            b=random.choice(["no", "inactive", "closed", "non_recovery"]),
            col=random.choice(COMMON_FIELD_NAMES),
            col_a="created_at",
            col_b="resolution_datetime",
        )
    else:
        source = make_source_path(input_tables, 1)
        tech = random.choice(FIELD_TECH_PATTERNS_DIRECT)

    description = fake.sentence(nb_words=random.randint(8, 16)).rstrip(".")
    # Make descriptions feel like real DD entries.
    description = description.replace(" the ", " ").lower().capitalize()
    return {
        "name": name,
        "description": description,
        "source": source,
        "technical_description": tech,
    }


def make_fields(input_tables: list[str], k: int) -> list[dict]:
    fields = [
        # Always include a primary id + a partition-ish key + a timestamp.
        make_field("id", input_tables),
        make_field("created_at", input_tables),
        make_field("created_month", input_tables),
    ]
    used = {"id", "created_at", "created_month"}
    pool = [n for n in COMMON_FIELD_NAMES if n not in used]
    extras = random.sample(pool, min(k - 3, len(pool)))
    for n in extras:
        fields.append(make_field(n, input_tables))
    # Add some narrative-rich derived fields (these are what the agent will love retrieving).
    for _ in range(random.randint(1, 4)):
        synthetic_name = f"{random.choice(['recovery', 'non_recovery', 'last', 'first', 'next'])}_{random.choice(['scan', 'hub', 'event', 'flag', 'date', 'count'])}"
        if synthetic_name not in used:
            fields.append(make_field(synthetic_name, input_tables, force_derived=True))
            used.add(synthetic_name)
    return fields


def build_table(spec: TableSpec) -> dict:
    input_tables = make_input_tables(spec.domain, spec.upstream_count)
    return {
        "table_name": spec.name,
        "overview": {
            "purpose": make_purpose(),
            "granularity": make_granularity(spec.name),
            "business_rules": make_business_rules(),
        },
        "input_tables": input_tables,
        "fields": make_fields(input_tables, spec.field_count),
    }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def plan_tables(target_synthetic: int) -> list[TableSpec]:
    """Plan ~495 synthetic tables distributed across domains and archetypes."""
    specs: list[TableSpec] = []
    weights = {"orders": 14, "shipments": 16, "hubs": 9, "fleet": 10,
               "customers": 8, "support": 12, "billing": 7, "inventory": 8,
               "calendar": 4, "audit": 12}
    weight_sum = sum(weights.values())
    plan = {d: round(target_synthetic * w / weight_sum) for d, w in weights.items()}
    while sum(plan.values()) < target_synthetic:
        plan[random.choice(list(plan))] += 1
    while sum(plan.values()) > target_synthetic:
        plan[random.choice(list(plan))] -= 1

    for domain, count in plan.items():
        for i in range(count):
            base = random.choice(DOMAINS[domain])
            archetype = random.choice(ARCHETYPES)
            name = make_table_name(domain, archetype, base)
            # Dedupe by suffixing the index when needed.
            name = f"{name}_{i:03d}" if i > 0 else name
            specs.append(TableSpec(
                name=name,
                domain=domain,
                archetype=archetype,
                upstream_count=random.randint(2, 8),
                field_count=random.randint(8, 25),
            ))
    return specs


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Clear any prior synthetic outputs but keep the directory.
    for p in OUT_DIR.glob("*.yaml"):
        p.unlink()

    # 1. Copy real YAMLs into the unified raw directory.
    real_files = list(REAL_DIR.glob("*.yaml")) if REAL_DIR.exists() else []
    for src in real_files:
        shutil.copy(src, OUT_DIR / src.name)
    print(f"Copied {len(real_files)} real YAMLs from {REAL_DIR}")

    target = 500 - len(real_files)
    print(f"Generating {target} synthetic YAMLs to reach 500 total")

    specs = plan_tables(target)
    failures: list[tuple[str, str]] = []
    seen: set[str] = {p.stem for p in real_files}

    for spec in specs:
        # Avoid colliding with real table names.
        name = spec.name
        suffix = 0
        while name in seen:
            suffix += 1
            name = f"{spec.name}_v{suffix}"
        spec.name = name
        seen.add(name)

        doc = build_table(spec)
        try:
            validate(doc, SCHEMA)
        except ValidationError as e:
            failures.append((spec.name, str(e.message)))
            continue
        (OUT_DIR / f"{spec.name}.yaml").write_text(
            yaml.safe_dump(doc, sort_keys=False, default_flow_style=False, width=120)
        )

    total = len(list(OUT_DIR.glob("*.yaml")))
    print(f"\nFinal count in {OUT_DIR}: {total}")
    if failures:
        print(f"\n{len(failures)} failures:")
        for name, msg in failures[:10]:
            print(f"  - {name}: {msg}")


if __name__ == "__main__":
    main()