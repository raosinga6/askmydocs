# Learning checklist (Docker → Kafka → dbt → K8s)

Use this repo only. No askmydocs validation.

## Day 1 — Docker

- [ ] `.\scripts\up.ps1` and `.\scripts\smoke.ps1`
- [ ] `docker stats` while Redpanda is running
- [ ] Add `deploy.resources.limits` override (copy snippet from README in parent chat)
- [ ] `docker network inspect` for compose network

## Day 2 — Networking

- [ ] `docker compose --profile admin up -d` → open http://localhost:8080
- [ ] `docker exec lab-postgres ping -c 2 lab-redpanda` (or getent hosts)

## Day 3 — Kafka

- [ ] `pip install -r kafka/requirements.txt`
- [ ] `python kafka/producer.py --count 20`
- [ ] `python kafka/consumer.py --from-beginning --max-messages 20`
- [ ] `docker exec lab-redpanda rpk topic describe lab.events`

## Day 4 — dbt

- [ ] `copy dbt\profiles.yml.example dbt\profiles.yml`
- [ ] `cd dbt && dbt seed && dbt run`
- [ ] Query mart: `SELECT * FROM analytics_marts.fct_daily_events;` (schema may be `marts` — check `dbt run` output)

## Day 5 — Failure drills

- [ ] `docker stop lab-redpanda`, produce messages (fails), start broker, retry
- [ ] Re-run consumer with `--from-beginning`

## Week 6+ — Kubernetes

- [ ] Follow `k8s/README.md`
