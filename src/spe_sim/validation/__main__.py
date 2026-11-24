"""CLI kiểm chứng hàng đợi đơn và trạm CPU.

    py -3.12 -m spe_sim.validation                         # bộ "generic" trên cài đặt của mạng
    py -3.12 -m spe_sim.validation --set project
    py -3.12 -m spe_sim.validation --set cpu               # A3: trạm CPU cô lập 4 runtime
    py -3.12 -m spe_sim.validation --set all --impl both   # đủ mọi thứ + đối chứng chéo
    py -3.12 -m spe_sim.validation --quick                 # R = 5, ~10 000 lượt đến, không ép

Cần `PYTHONPATH=src` (hoặc cài gói) và Python ≥ 3.11.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from spe_sim.validation.harness import (
    cross_check,
    evaluate,
    print_table,
    render_markdown,
    run_cases,
)
from spe_sim.validation.params import SUITES, build_cases

DEFAULT_OUT = Path(__file__).resolve().parents[3] / "docs" / "validation_results.md"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m spe_sim.validation", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--set", dest="suite", choices=(*SUITES, "all"), default="generic",
                    help="bộ tham số (mặc định: generic)")
    ap.add_argument("--impl", choices=("station", "reference", "both"), default="station",
                    help="cài đặt cần kiểm (mặc định: station = LP của mạng)")
    ap.add_argument("-R", "--replications", type=int, default=30)
    ap.add_argument("--arrivals", type=int, default=50_000, help="số lượt đến kỳ vọng mỗi lần chạy")
    ap.add_argument("--kernel", choices=("coroutine", "sequential"), default="coroutine",
                    help="engine cho cài đặt của mạng (mặc định: coroutine = asyncio)")
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--quick", action="store_true", help="R=5, ~10 000 lượt đến; không ép ngưỡng")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="file markdown kết quả")
    ap.add_argument("--no-write", action="store_true", help="không ghi file markdown")
    args = ap.parse_args(argv)

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    replications, n_arrivals = (5, 10_000) if args.quick else (args.replications, args.arrivals)
    suites = SUITES if args.suite == "all" else (args.suite,)
    impls = ("station", "reference") if args.impl == "both" else (args.impl,)
    cases = build_cases(suites)

    results = run_cases(
        cases,
        impls=impls,
        replications=replications,
        n_arrivals=n_arrivals,
        kernel=args.kernel,
        workers=args.workers,
    )
    rows = [row for impl in impls for row in evaluate(cases, results, impl)]
    checks = cross_check(cases, results) if args.impl == "both" else []
    print_table(rows)

    asserted = [r for r in rows if r.asserted]
    failures = [r for r in asserted if not r.ok]
    bad_checks = [c for c in checks if not c.overlap]
    outside = sum(not r.in_ci for r in rows)
    print(f"\n[kết quả] đạt {len(asserted) - len(failures)}/{len(asserted)} phép so có ép ngưỡng; "
          f"giá trị giải tích ngoài CI ở {outside}/{len(rows)} dòng")
    if checks:
        print(f"[đối chứng chéo] CI giao nhau {len(checks) - len(bad_checks)}/{len(checks)}")

    if args.quick:
        print("[kết quả] chế độ --quick: chỉ chạy thử, không ép ngưỡng, không ghi file")
        return 0
    if not args.no_write:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            render_markdown(rows, checks, replications=replications, n_arrivals=n_arrivals,
                            kernel=args.kernel) + "\n",
            encoding="utf-8",
        )
        print(f"[ghi] {args.out}")
    if failures or bad_checks:
        for r in failures:
            print(f"  TRƯỢT: {r.case.key} {r.metric} sai số {100 * r.err:.2f}%")
        for c in bad_checks:
            print(f"  LỆCH CHÉO: {c.case.key} {c.metric}")
        return 1
    print("[kết quả] TẤT CẢ ĐẠT ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
