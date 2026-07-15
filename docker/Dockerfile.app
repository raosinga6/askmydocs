# All-in-one product image: pipeline + embeddings + web UI in one container.
#
#   docker run -p 8008:8008 \
#     -e GEMINI_API_KEY=... \
#     -v /path/to/your/yamls:/data/raw:ro \
#     -v askmydocs_state:/data/parquet \
#     ghcr.io/raosinga6/askmydocs:latest
#
# On start it runs any pipeline stage whose output is missing (ingest ->
# lineage -> embedding_input -> embeddings), then serves the UI on :8008.
# Same multi-stage / non-root conventions as Dockerfile.spark.prod.

# Python 3.12 to match pyproject's requires-python (numpy 2.5 needs >=3.12);
# pyspark 3.5.3 is verified working on 3.12 in local dev.
# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS build

COPY requirements.app.txt /tmp/requirements.app.txt
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.app.txt

# ---------------------------------------------------------------------------
FROM python:3.12-slim-bookworm AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/* && \
    apt-get clean

COPY --from=build /opt/venv /opt/venv

ARG TARGETARCH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-${TARGETARCH}
ENV PATH=/opt/venv/bin:$JAVA_HOME/bin:$PATH

RUN groupadd --system --gid 1001 appuser && \
    useradd --system --uid 1001 --gid appuser --shell /bin/bash \
            --create-home appuser && \
    mkdir -p /data/raw /data/parquet && chown -R appuser:appuser /data

WORKDIR /app

COPY --chown=appuser:appuser schemas/ /app/schemas/
COPY --chown=appuser:appuser spark_jobs/ /app/spark_jobs/
COPY --chown=appuser:appuser serve/ /app/serve/
COPY --chown=appuser:appuser docker/entrypoint.app.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

USER appuser

# Everything lives under /data so one volume persists all state.
ENV ASKMYDOCS_RAW_DIR=/data/raw \
    ASKMYDOCS_OUT_DIR=/data/parquet \
    ASKMYDOCS_CATALOG_IN=/data/parquet/catalog \
    ASKMYDOCS_TABLE_LINEAGE_OUT=/data/parquet/table_lineage \
    ASKMYDOCS_FIELD_LINEAGE_OUT=/data/parquet/field_lineage \
    ASKMYDOCS_TABLE_LINEAGE_IN=/data/parquet/table_lineage \
    ASKMYDOCS_EMBEDDING_OUT=/data/parquet/embedding_input \
    ASKMYDOCS_EMBEDDING_IN=/data/parquet/embedding_input \
    ASKMYDOCS_EMBEDDINGS_OUT=/data/parquet/embeddings \
    ASKMYDOCS_EMBED_CACHE=/data/parquet/.embedding_cache.json \
    ASKMYDOCS_HOST=0.0.0.0 \
    PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8008

LABEL org.opencontainers.image.title="askmydocs" \
      org.opencontainers.image.description="RAG search + grounded Q&A over data dictionary catalogs — self-bootstrapping all-in-one image" \
      org.opencontainers.image.source="https://github.com/raosinga6/askmydocs"

ENTRYPOINT ["/app/entrypoint.sh"]
