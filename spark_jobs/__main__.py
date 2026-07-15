"""Production entry point for AskMyDocs Spark jobs.

Single image, multiple jobs. Choose which job to run via subcommand:

    python -m spark_jobs ingest_catalog
    python -m spark_jobs extract_lineage
    python -m spark_jobs build_embedding_input

Optional --raw-dir / --out-dir flags override the production defaults.
Used in Week 3 when SparkApplication CRDs point at GCS paths.
"""
from __future__ import annotations

import argparse
import importlib
import sys

JOBS = {
    "ingest_catalog": "spark_jobs.ingest_catalog",
    "extract_lineage": "spark_jobs.extract_lineage",
    "build_embedding_input": "spark_jobs.build_embedding_input",
    "build_embeddings": "spark_jobs.build_embeddings",
}


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="python -m spark_jobs",
        description="AskMyDocs Spark jobs entry point",
    )
    parser.add_argument(
        "job",
        choices=list(JOBS.keys()),
        help="Which job to run.",
    )
    args = parser.parse_args()

    print(f"Running job: {args.job}", flush=True)
    module = importlib.import_module(JOBS[args.job])
    if not hasattr(module, "main"):
        print(f"ERROR: module {JOBS[args.job]} has no main() function", file=sys.stderr)
        return 1

    module.main()
    return 0


if __name__ == "__main__":
    sys.exit(main())