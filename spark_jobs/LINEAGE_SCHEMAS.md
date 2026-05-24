# Lineage Schemas

## table_lineage.parquet

One row per (downstream_table, upstream_table) pair.

| column | type | example |
|---|---|---|
| downstream_table | string | action_after_ticket_closure_base |
| upstream_namespace | string | data_warehouse |
| upstream_table | string | pets_tickets_enriched |
| upstream_fqn | string | data_warehouse.pets_tickets_enriched |

Partition: none (small dataset, < 5k rows expected)

## field_lineage.parquet

One row per (downstream_table, downstream_field, upstream_table, upstream_column).

| column | type | example |
|---|---|---|
| downstream_table | string | action_after_ticket_closure_base |
| downstream_field | string | recovery_scan_hub |
| upstream_namespace | string | core_prod_gl |
| upstream_table | string | inbound_scans |
| upstream_column | string | hub_id |
| upstream_fqn | string | core_prod_gl.inbound_scans.hub_id |
| source_kind | string | direct \| derived \| unknown |

`source_kind` is heuristic: if `technical_description` starts with "Direct field", it's "direct". If it starts with "Derived field" or contains "IF(", "CASE", "ROW_NUMBER", etc., it's "derived". Otherwise "unknown".

Partition: none.

## Quality expectations

- Every downstream_table in lineage tables exists in catalog.parquet
- No self-references (downstream != upstream after FQN normalization)
- Field lineage row count ≥ table lineage row count (each table contributes multiple field edges)