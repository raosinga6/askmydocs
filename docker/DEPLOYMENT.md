# AskMyDocs Spark Job Deployment

## Two images, two purposes

We maintain parallel Docker images for the Spark jobs:

| Image | File | Used for |
|---|---|---|
| `askmydocs-spark:dev` | `docker/Dockerfile.spark` | Local development with bind-mounted code |
| `askmydocs-spark:prod` | `docker/Dockerfile.spark.prod` | Production deployment to GKE |

Both are built from `python:3.11-slim-bookworm` with OpenJDK 17, but they
differ in critical ways — see below.

## Production image design

### Multi-stage build

Stage 1 (`build`): installs pip dependencies into `/opt/venv`. Includes
pip itself, build tools, and any wheel artifacts.

Stage 2 (`runtime`): copies only `/opt/venv` from stage 1. Final image
contains only the venv, system Java, and our source code. No pip caches,
no build tools, no temporary files.

This pattern reduces image size by ~40% and removes a large class of
supply-chain attack surface (no `pip` in the runtime image means a
compromised job can't `pip install` malicious packages at runtime).

### Non-root user

UID 1001 (`sparkuser`). Matches Kubernetes Pod Security Standards
`runAsUser: 1001` policy. Code and venv are owned by this user; the
filesystem under `/data` is expected to be writable by UID 1001 (via
fsGroup setting in the Pod spec).

### Configuration via environment

All input/output paths are env vars. Defaults are `/data/...` for prod.
Override in the Pod spec to point at GCS:

```yaml
env:
  - name: ASKMYDOCS_RAW_DIR
    value: "gs://askmydocs-prod/raw"
  - name: ASKMYDOCS_OUT_DIR
    value: "gs://askmydocs-prod/parquet"
```

Spark reads GCS paths natively via the GCS connector (added in Week 3).

### ENTRYPOINT + CMD split

Fixed entrypoint is the dispatcher. CMD is the default job. From Kubernetes:

```yaml
args: ["extract_lineage"]
```

A single image runs all three jobs. Three SparkApplication CRDs (one per job)
all reference the same image with different `args`.

## Build and push

```bash
scripts/build_and_push.sh 0.1.0  # or .bat on Windows
```

This tags and pushes to the local registry at `localhost:5000`. In Week 3
this becomes `gcloud artifacts docker push` to GCP Artifact Registry.

## Versioning policy

- `:dev` for whatever is checked in locally
- `:X.Y.Z` for releases — bump on every push to a shared registry
- No `:latest` — ambiguous, deploy-time race conditions
- In real production, also tag by git SHA for auditability (`:sha-abc123`)

## Image inventory

To verify a deployed image's contents without running it:

```bash
docker inspect <image>
docker history <image>  # see layers
docker run --rm --entrypoint ls <image> -la /app/spark_jobs
```

The image is immutable — what's in it at build time is what runs in production.

## What's NOT in the prod image (deliberately)

- pytest, pytest-* (use `:dev` for tests)
- google-genai (only the YAML generator needs it; runs on dev)
- Gemini API keys (loaded from Secret Manager in production)
- The 500-YAML dataset (mounted from GCS at runtime, not baked in)
- Tests under `tests/`
- Scripts under `scripts/`
- This documentation file