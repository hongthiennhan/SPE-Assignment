"""CLI chạy lưới kịch bản mạng Fork-Join từ `configs/scenarios.json`.

    py -3.12 -m spe_sim.runner --out results/                     # toàn bộ, R = 30
    py -3.12 -m spe_sim.runner --out results/ --workers 16        # song song
    py -3.12 -m spe_sim.runner --out results/ --only spring_loom_vt -R 5 --t-sim 60

Ghi một CSV D3 cho mỗi kịch bản và `summary.csv` (mean ± CI 95 % của X, E[r], P95, P_loss).
Cần `PYTHONPATH=src` và Python ≥ 3.11.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from spe_sim.runner.study import SUMMARY_FILENAME, load_scenarios, run_grid


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m spe_sim.runner", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True, help="thư mục ghi CSV")
    ap.add_argument("--config", type=Path, default=None, help="file kịch bản (mặc định configs/scenarios.json)")
    ap.add_argument("-R", "--replications", type=int, default=None, help="số seed (mặc định: base.replications)")
    ap.add_argument("--t-sim", type=float, default=None, help="rút ngắn T_sim (giây ảo) để chạy thử")
    ap.add_argument("--kernel", choices=("coroutine", "sequential"), default="coroutine")
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--only", nargs="*", default=None, help="chỉ chạy kịch bản có id bắt đầu bằng các tiền tố này")
    ap.add_argument("--limit", type=int, default=None, help="giới hạn số (kịch bản × seed)")
    args = ap.parse_args(argv)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    data = load_scenarios(args.config)
    t0 = time.perf_counter()
    written = run_grid(
        args.out,
        data=data,
        kernel=args.kernel,
        replications=args.replications,
        t_sim=args.t_sim,
        limit=args.limit,
        workers=args.workers,
        only=args.only,
    )
    print(f"[ghi] {len(written)} CSV kịch bản + {args.out / SUMMARY_FILENAME} "
          f"sau {time.perf_counter() - t0:.1f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
