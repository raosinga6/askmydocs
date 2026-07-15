"""Compare baseline vs optimized perf results side by side."""
import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASELINE = REPO / "data" / "perf_results_baseline.json"
OPTIMIZED = REPO / "data" / "perf_results_optimized.json"

base = json.loads(BASELINE.read_text())
opt = json.loads(OPTIMIZED.read_text())

print(f"{'metric':40} {'baseline':>15} {'optimized':>15} {'speedup':>10}")
print("-" * 85)

shared_keys = sorted(set(base.keys()) & set(opt.keys()))
for k in shared_keys:
    bv, ov = base[k], opt[k]
    if isinstance(bv, (int, float)) and isinstance(ov, (int, float)) and ov > 0:
        if "seconds" in k or k.startswith("0"):
            speedup = f"{bv / ov:.2f}x"
        else:
            speedup = f"{ov / bv:.2%}"
        print(f"{k:40} {bv:>15} {ov:>15} {speedup:>10}")
    else:
        print(f"{k:40} {str(bv):>15} {str(ov):>15} {'-':>10}")