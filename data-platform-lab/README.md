# Data Platform Lab

Minimal hands-on lab for **Docker → Kafka → dbt → Kubernetes**. No app tests, no CI — only smoke checks.

## What’s in the box

| Piece | Purpose |
|-------|---------|
| `docker/docker-compose.yml` | Postgres + Redpanda (Kafka API) |
| `kafka/` | Tiny producer + consumer (Python) |
| `dbt/` | 2 models + 1 seed (works without Kafka) |
| `k8s/` | Manifests for later weeks (Kind/minikube) |

## Prerequisites

- Docker Desktop (Windows)
- Python 3.11+ (`pip install -r kafka/requirements.txt`)
- [dbt-core](https://docs.getdbt.com/docs/core/installation) + adapter: `pip install dbt-core dbt-postgres`
- Later: [Kind](https://kind.sigs.k8s.io/) or minikube for `k8s/`

## Quick start (5 minutes)

```powershell
cd C:\BI\DWH\proj\askmydocs\data-platform-lab

# 1) Start infra
.\scripts\up.ps1

# 2) Smoke check
.\scripts\smoke.ps1

# 3) dbt only path (no Kafka required)
cd dbt
copy profiles.yml.example profiles.yml   # first time only
dbt seed
dbt run
dbt test   # optional; 2 generic tests only

# 4) Kafka path
cd ..
pip install -r kafka/requirements.txt
python kafka/producer.py --count 20
python kafka/consumer.py --max-messages 20

cd dbt
dbt run
```

## Daily learning map (this repo)

| Day | Focus | Commands |
|-----|--------|----------|
| 1 | Docker, limits, networks | `docker compose`, `docker stats` |
| 2 | Second service, DNS | add `admin` profile in compose |
| 3 | Kafka produce/consume | `producer.py`, `rpk` / consumer |
| 4 | dbt staging → mart | `dbt seed`, `dbt run` |
| 5 | Break/fix broker | stop Redpanda, observe lag |
| 6+ | Kubernetes | `k8s/README.md` |

## Ports

| Service | Port |
|---------|------|
| Postgres | 5432 |
| Kafka (external) | 19092 |
| Redpanda Console | 8080 |

## Credentials (lab only)

- Postgres: `lab` / `lab`, database `lab`
- Never use these outside your machine.

## Stop

```powershell
.\scripts\down.ps1
```
