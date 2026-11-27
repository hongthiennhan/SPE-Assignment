from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from statistics import mean, stdev
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent[3]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from spe_sim.core.config import ScenarioConfig
from spe_sim.core.records import write_csv
from spe_sim.core.rng import sample_percentile
from spe_sim.runner.study import load_scenarios, run_scenario, scenario_configs

T_CRIT_95: dict[int, float] = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
    10: 2.228, 15: 2.131, 20: 2.086, 29: 2.045,
}


def t_critical_95(df: int) -> float:
    if df <= 0:
        return 0.0
    if df in T_CRIT_95:
        return T_CRIT_95[df]
    if df > 29:
        return 1.96 + 2.48 / df
    keys = sorted(T_CRIT_95.keys())
    for lo, hi in zip(keys, keys[1:]):
        if lo < df < hi:
            w = (df - lo) / (hi - lo)
            return T_CRIT_95[lo] * (1.0 - w) + T_CRIT_95[hi] * w
    return 2.045


def summarize_ci95(values: list[float]) -> tuple[float, float, float]:
    valid = [v for v in values if v is not None and not math.isnan(v)]
    n = len(valid)
    if n == 0:
        return (math.nan, math.nan, math.nan)
    m = mean(valid)
    if n == 1:
        return (m, 0.0, 0.0)
    sd = stdev(valid)
    half_ci = t_critical_95(n - 1) * (sd / math.sqrt(n))
    return (m, sd, half_ci)


def _worker_run_one(task: tuple[str, str, ScenarioConfig, str, Path | None]) -> dict[str, Any]:
    """Chạy 1 (kịch bản, seed), tính metrics SI ngay trong RAM và trả về 1 dict gọn nhẹ."""
    group_name, scenario_id, cfg, kernel, sample_raw_dir = task
    res = run_scenario(cfg, kernel=kernel)

    if sample_raw_dir is not None:
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", scenario_id).strip("_")
        write_csv(sample_raw_dir / f"{safe}_seed{cfg.seed}.csv", res.records)

    t_warmup = cfg.t_warmup
    t_sim = cfg.t_sim
    eff_window_s = t_sim - t_warmup

    rt_s_list: list[float] = []
    n_total = 0
    n_completed = 0
    n_dropped = 0
    n_db_drop = 0
    n_timeout = 0
    n_other_loss = 0

    for rec in res.records:
        if rec.t_arrive < t_warmup or rec.t_arrive > t_sim:
            continue
        n_total += 1
        if rec.outcome == "completed":
            n_completed += 1
            if rec.t_complete is not None:
                rt_s = rec.t_complete - rec.t_arrive
            elif rec.r_ms is not None:
                rt_s = rec.r_ms / 1000.0
            else:
                continue
            if rt_s >= 0.0:
                rt_s_list.append(rt_s)
        else:
            if rec.outcome == "dropped":
                n_dropped += 1
            elif rec.outcome == "db_drop":
                n_db_drop += 1
            elif rec.outcome == "timeout":
                n_timeout += 1
            else:
                n_other_loss += 1

    n_lost = n_dropped + n_db_drop + n_timeout + n_other_loss
    throughput_req_s = n_completed / eff_window_s if eff_window_s > 0 else math.nan
    mean_rt_s = mean(rt_s_list) if rt_s_list else math.nan
    p95_s = sample_percentile(rt_s_list, 0.95) if rt_s_list else math.nan
    loss_rate = (n_lost / n_total) if n_total > 0 else 0.0

    return {
        "group": group_name,
        "scenario_id": scenario_id,
        "runtime_model": cfg.runtime_model,
        "join_mode": cfg.join_mode,
        "lam": cfg.lam,
        "n_db": cfg.n_db,
        "p_pin": cfg.p_pin,
        "p_uv": cfg.p_uv,
        "seed": cfg.seed,
        "t_sim_s": t_sim,
        "t_warmup_s": t_warmup,
        "n_total": n_total,
        "n_completed": n_completed,
        "n_dropped": n_dropped,
        "n_db_drop": n_db_drop,
        "n_timeout": n_timeout,
        "throughput_req_s": throughput_req_s,
        "mean_rt_s": mean_rt_s,
        "p95_s": p95_s,
        "loss_rate": loss_rate,
    }


PER_SEED_FIELDS = [
    "group", "scenario_id", "runtime_model", "join_mode", "lam",
    "n_db", "p_pin", "p_uv", "seed", "t_sim_s", "t_warmup_s",
    "n_total", "n_completed", "n_dropped", "n_db_drop", "n_timeout",
    "throughput_req_s", "mean_rt_s", "p95_s", "loss_rate",
]


def build_summary_ci95(per_seed_rows: list[dict[str, Any]], output_summary_csv: Path) -> None:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in per_seed_rows:
        grouped[row["scenario_id"]].append(row)

    summary_rows: list[dict[str, Any]] = []
    for scenario_id, rows in grouped.items():
        rows.sort(key=lambda r: int(r["seed"]))
        first = rows[0]

        x_m, x_sd, x_ci = summarize_ci95([float(r["throughput_req_s"]) for r in rows])
        er_m, er_sd, er_ci = summarize_ci95([float(r["mean_rt_s"]) for r in rows if str(r["mean_rt_s"]) != "nan"])
        p95_m, p95_sd, p95_ci = summarize_ci95([float(r["p95_s"]) for r in rows if str(r["p95_s"]) != "nan"])
        loss_m, loss_sd, loss_ci = summarize_ci95([float(r["loss_rate"]) for r in rows])

        drop_m, _, _ = summarize_ci95([int(r["n_dropped"]) / max(1, int(r["n_total"])) for r in rows])
        db_drop_m, _, _ = summarize_ci95([int(r["n_db_drop"]) / max(1, int(r["n_total"])) for r in rows])
        timeout_m, _, _ = summarize_ci95([int(r["n_timeout"]) / max(1, int(r["n_total"])) for r in rows])

        def to_ms(v: float) -> Any:
            return round(v * 1000.0, 4) if not math.isnan(v) else ""

        def fmt(v: float, ndigits: int = 6) -> Any:
            return round(v, ndigits) if not math.isnan(v) else ""

        summary_rows.append({
            "group": first["group"],
            "scenario_id": scenario_id,
            "runtime_model": first["runtime_model"],
            "join_mode": first["join_mode"],
            "lam": first["lam"],
            "n_db": first["n_db"],
            "p_pin": first["p_pin"],
            "p_uv": first["p_uv"],
            "n_seeds": len(rows),
            "throughput_mean_req_s": fmt(x_m),
            "throughput_std_req_s": fmt(x_sd),
            "throughput_ci95_req_s": fmt(x_ci),
            "e_r_mean_ms": to_ms(er_m),
            "e_r_std_ms": to_ms(er_sd),
            "e_r_ci95_ms": to_ms(er_ci),
            "p95_mean_ms": to_ms(p95_m),
            "p95_std_ms": to_ms(p95_sd),
            "p95_ci95_ms": to_ms(p95_ci),
            "loss_rate_mean": fmt(loss_m),
            "loss_rate_std": fmt(loss_sd),
            "loss_rate_ci95": fmt(loss_ci),
            "dropped_rate_mean": fmt(drop_m),
            "db_drop_rate_mean": fmt(db_drop_m),
            "timeout_rate_mean": fmt(timeout_m),
        })

    output_summary_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_summary_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
        writer.writeheader()
        writer.writerows(summary_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy ma trận thí nghiệm + xuất 95% CI.")
    parser.add_argument("--out-dir", type=Path, default=Path("results/final"))
    parser.add_argument("--kernel", type=str, default="coroutine", choices=["coroutine", "sequential"])
    parser.add_argument("--t-sim", type=float, default=None)
    parser.add_argument("--replications", type=int, default=None)
    parser.add_argument("--group", type=str, default="all", choices=["all", "main", "sensitivity"])
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 1))
    parser.add_argument(
        "--save-sample-seed",
        type=int,
        default=1000,
        help="Lưu CSV raw cho 1 seed mẫu để kiểm chứng (đặt -1 để tắt hoàn toàn)",
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    payload = load_scenarios()
    main_ids = {s["id"] for s in payload.get("scenarios", [])}
    if args.group == "main":
        payload = dict(payload, sensitivity=[])
    elif args.group == "sensitivity":
        payload = dict(payload, scenarios=[])

    seeds = payload["base"]["seeds"]
    if args.replications is not None:
        seeds = seeds[: args.replications]

    entries = scenario_configs(payload, seeds=seeds, t_sim=args.t_sim)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    per_seed_csv = args.out_dir / "per_seed_metrics.csv"
    summary_csv = args.out_dir / "summary_metrics_ci95.csv"
    sample_dir = (args.out_dir / "raw_sample") if args.save_sample_seed >= 0 else None
    if sample_dir is not None:
        sample_dir.mkdir(parents=True, exist_ok=True)

    existing_rows: list[dict[str, Any]] = []
    done_keys: set[tuple[str, int]] = set()
    if per_seed_csv.exists() and not args.overwrite:
        with per_seed_csv.open("r", encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                existing_rows.append(r)
                done_keys.add((r["scenario_id"], int(r["seed"])))

    tasks: list[tuple[str, str, ScenarioConfig, str, Path | None]] = []
    for scenario_id, cfg in entries:
        if (scenario_id, cfg.seed) in done_keys:
            continue
        grp = "main" if scenario_id in main_ids else "sensitivity"
        s_dir = sample_dir if (sample_dir is not None and cfg.seed == args.save_sample_seed) else None
        tasks.append((grp, scenario_id, cfg, args.kernel, s_dir))

    total_all = len(entries)
    print(
        f"Tổng={total_all} lượt | Đã có={len(done_keys)} | Cần chạy={len(tasks)} | "
        f"Workers={args.workers} | Kernel={args.kernel}"
    )

    t0 = time.perf_counter()
    if tasks:
        write_header = not per_seed_csv.exists() or args.overwrite
        mode = "w" if args.overwrite else "a"
        with per_seed_csv.open(mode, encoding="utf-8", newline="") as f_out, ProcessPoolExecutor(
            max_workers=args.workers
        ) as pool:
            writer = csv.DictWriter(f_out, fieldnames=PER_SEED_FIELDS)
            if write_header:
                writer.writeheader()
                f_out.flush()

            futures = [pool.submit(_worker_run_one, t) for t in tasks]
            completed_now = 0
            for fut in as_completed(futures):
                row = fut.result()
                writer.writerow(row)
                existing_rows.append(row)
                completed_now += 1
                if completed_now % 30 == 0 or completed_now == len(tasks):
                    f_out.flush()
                    elapsed = time.perf_counter() - t0
                    rate = completed_now / elapsed if elapsed > 0 else 0.0
                    eta_min = ((len(tasks) - completed_now) / rate / 60.0) if rate > 0 else 0.0
                    print(
                        f"Tiến độ: {len(done_keys) + completed_now}/{total_all} "
                        f"({completed_now}/{len(tasks)} lượt mới) | {elapsed:.1f}s | ETA ~{eta_min:.1f} phút"
                    )

    if existing_rows:
        build_summary_ci95(existing_rows, summary_csv)
        print(f"Đã cập nhật bảng tổng hợp 95% CI tại: {summary_csv}")


if __name__ == "__main__":
    main()