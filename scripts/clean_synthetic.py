"""Remove synthetic YAMLs from data/raw, leaving only the real dictionaries.

Real files are the ones present in data/real/ (matched by filename); anything
else in data/raw/ was produced by a generator. Dry-run by default:

    uv run python scripts/clean_synthetic.py            # show what would go
    uv run python scripts/clean_synthetic.py --apply    # actually delete

After cleaning, re-run the pipeline with thresholds sized to the real corpus:

    export ASKMYDOCS_DQ_MIN_FILES=1 ASKMYDOCS_DQ_MIN_NAMESPACES=1
    python -m spark_jobs ingest_catalog && ...

The embedding cache is content-keyed, so no cache cleanup is needed —
synthetic entries simply stop being referenced.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
RAW_DIR = REPO / "data" / "raw"
REAL_DIR = REPO / "data" / "real"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete synthetic YAMLs from data/raw (keeps real ones)")
    parser.add_argument("--apply", action="store_true",
                        help="actually delete; default is dry-run")
    args = parser.parse_args()

    if not RAW_DIR.is_dir():
        print(f"nothing to clean: {RAW_DIR} does not exist", file=sys.stderr)
        return 1
    real_names = {p.name for p in REAL_DIR.glob("*.yaml")} if REAL_DIR.is_dir() else set()
    if not real_names:
        print(f"refusing to run: {REAL_DIR} has no YAMLs, so *every* file in "
              f"data/raw would be deleted. Populate data/real first.",
              file=sys.stderr)
        return 1

    raw_files = sorted(RAW_DIR.glob("*.yaml"))
    synthetic = [p for p in raw_files if p.name not in real_names]
    real_kept = len(raw_files) - len(synthetic)

    print(f"data/raw: {len(raw_files)} YAMLs — {real_kept} real (kept), "
          f"{len(synthetic)} synthetic ({'deleting' if args.apply else 'would delete'})")

    for p in synthetic:
        if args.apply:
            p.unlink()
        else:
            print(f"  would delete {p.name}")

    if args.apply:
        print(f"\nDone. {real_kept} real YAMLs remain in data/raw.")
        print("Re-run the pipeline with real-corpus DQ thresholds, e.g.:")
        print("  export ASKMYDOCS_DQ_MIN_FILES=1 ASKMYDOCS_DQ_MIN_NAMESPACES=1")
    else:
        print("\nDry run — nothing deleted. Re-run with --apply to delete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
