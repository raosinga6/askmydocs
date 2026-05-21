# Domain Topology

## Business domains (10)
1. **orders** — order lifecycle, status transitions
2. **shipments** — physical movement, scans, hubs
3. **hubs** — facility metadata, sortation, routing
4. **fleet** — drivers, vehicles, routes, last-mile
5. **customers** — shipper and consignee profiles
6. **support** — PETS tickets, escalations, resolutions
7. **billing** — invoicing, COD, settlements
8. **inventory** — warehousing, SKU, stock levels
9. **calendar** — operational dates, cutoffs, holidays
10. **audit** — change logs, event streams

## Table archetypes
- `*_enriched` — joined/derived dimension-like tables (e.g. hubs_enriched, pets_tickets_enriched)
- `*_base` — first-level derived facts (e.g. action_after_ticket_closure_base)
- `*_events` — append-only event streams (e.g. add_to_shipment_events)
- `*_snapshot` — daily/hourly snapshots
- `dim_*` / `fct_*` — dimensional layer
- `stg_*` — staging cleanups

## Naming sources
- `data_warehouse.<table>` — internal warehouse
- `core_prod_gl.<table>` — production OLTP replicas
- `metadata.<table>` — config/lookups