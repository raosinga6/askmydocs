# Kubernetes phase (Week 6+)

Use this **after** you’re comfortable with `docker/docker-compose.yml`.

## 1. Create a local cluster (Kind)

```powershell
kind create cluster --name data-platform-lab
kubectl cluster-info --context kind-data-platform-lab
```

## 2. Deploy lab namespace + Postgres

```powershell
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/postgres.yaml
kubectl wait --for=condition=ready pod -l app=lab-postgres -n data-platform-lab --timeout=120s
```

Port-forward to use dbt from your laptop:

```powershell
kubectl port-forward -n data-platform-lab svc/lab-postgres 5432:5432
```

Update `dbt/profiles.yml` host to `localhost` (same as Docker path).

## 3. Kafka on Kubernetes (later)

Options when ready:

- [Strimzi](https://strimzi.io/quickstarts/) Kafka operator
- Helm install Redpanda
- Managed Kafka (Confluent Cloud) — skip local broker ops

Do **not** deploy `k8s/redpanda.yaml` until you finish CKA basics; it’s a stub placeholder.

## Mapping: Docker Compose → Kubernetes

| Compose | Kubernetes |
|---------|------------|
| `service: postgres` | `Deployment` + `Service` + `PVC` |
| `service: redpanda` | `StatefulSet` or operator CRD |
| `ports: "5432:5432"` | `Service` + `port-forward` or `Ingress` |
| `volumes` | `PersistentVolumeClaim` |
| `healthcheck` | `livenessProbe` / `readinessProbe` |
| `environment` | `ConfigMap` / `Secret` |
