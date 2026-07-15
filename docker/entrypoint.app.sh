#!/usr/bin/env bash
# All-in-one bootstrap: run the pipeline stages that haven't produced output
# yet, then start the web UI. Idempotent — restarting the container re-embeds
# nothing (Spark's _SUCCESS markers + the content-keyed embedding cache).
#
# Force a full rebuild (e.g. after replacing the YAMLs): FORCE_REBUILD=1
set -euo pipefail

RAW_DIR="${ASKMYDOCS_RAW_DIR:-/data/raw}"
PARQUET_DIR="${ASKMYDOCS_OUT_DIR:-/data/parquet}"

yaml_count=$(find "$RAW_DIR" -maxdepth 1 -name '*.yaml' 2>/dev/null | wc -l)
if [ "$yaml_count" -eq 0 ]; then
  echo "ERROR: no .yaml files in $RAW_DIR." >&2
  echo "Mount your data dictionary YAMLs there, e.g.:" >&2
  echo "  docker run -v /path/to/your/yamls:/data/raw:ro ..." >&2
  exit 1
fi
echo "Catalog: $yaml_count YAMLs in $RAW_DIR"

if [ -z "${GEMINI_API_KEY:-}" ]; then
  echo "ERROR: GEMINI_API_KEY is not set (needed for embeddings and Q&A)." >&2
  exit 1
fi

stage() {  # stage <job-name> <output-dir>
  local job="$1" out="$2"
  if [ -f "$out/_SUCCESS" ] && [ -z "${FORCE_REBUILD:-}" ]; then
    echo "── $job: output exists at $out, skipping (FORCE_REBUILD=1 to redo)"
  else
    echo "── $job: running…"
    python -m spark_jobs "$job"
  fi
}

stage ingest_catalog        "$PARQUET_DIR/catalog"
stage extract_lineage       "$PARQUET_DIR/table_lineage"
stage build_embedding_input "$PARQUET_DIR/embedding_input"
stage build_embeddings      "$PARQUET_DIR/embeddings"

echo "── starting AskMyDocs UI on :${ASKMYDOCS_PORT:-8008}"
exec python -m serve
