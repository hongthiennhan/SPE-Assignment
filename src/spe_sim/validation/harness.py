"""Harness kiểm chứng (A2, A3, A6): chạy R seed, CI 95 % Student-t, so với nghiệm giải tích.

Chuyển từ `scenarios/validation_single_queues.py` của repo hàng đợi đơn; công thức giải
tích nay nằm ở `spe_sim.core.analytics`, CI ở `spe_sim.core.stats`.

Tiêu chí đạt (spec §1.5.2): sai số tương đối < 5 % khi ρ ≤ 0.8, < 8 % khi ρ ≥ 0.9.
Cột "Theory in CI" được báo cáo (không ép), vì với hàng trăm phép so ở mức 95 % thì khoảng
5 % số dòng nằm ngoài CI là bình thường thống kê.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Any, Iterable

from spe_sim.core.stats import ci95
from spe_sim.validation.params import Case, threshold

__all__ = [
    "CrossCheck",
    "Row",
    "METRICS",
    "P_BLOCK_FLOOR",
    "cross_check",
    "evaluate",
    "render_markdown",
    "run_cases",
]

METRICS: tuple[str, ...] = ("W", "L", "U", "P_block")
#: Sai số tương đối của xác suất quá nhỏ không có nghĩa ⇒ không ép ngưỡng.
P_BLOCK_FLOOR = 0.01
#: P_block giải tích dưới mức này coi như 0 (ví dụ K = 64 ở ρ = 0.3 cho ~1e-30) ⇒ bỏ dòng.
P_BLOCK_NEGLIGIBLE = 1e-6
#: Seed mặc định trùng `configs/scenarios.json` (base.seeds = 1000..1029) — C3.
SEED_BASE = 1000


@dataclass(frozen=True, slots=True)
class Job:
    case: Case
    seed: int
    impl: str  # "station" | "reference"
    kernel: str
    n_arrivals: int


def _run_job(job: Job) -> tuple[str, str, int, dict[str, Any]]:
    """Chạy một (case, seed). Hàm mức module để `ProcessPoolExecutor` pickle được."""
    case = job.case
    t_sim = job.n_arrivals / case.lam
    t_warmup = 0.1 * t_sim
    if case.suite == "cpu":
        from spe_sim.validation.cpu_station import run_cpu_station

        result = run_cpu_station(
            case.runtime_model,  # type: ignore[arg-type]
            case.lam,
            t_sim=t_sim,
            t_warmup=t_warmup,
            seed=job.seed,
            kernel=job.kernel,
        )
    elif job.impl == "reference":
        from spe_sim.validation.reference_server import run_reference

        result = run_reference(
            case.kendall, case.lam, case.mu, c=case.c, capacity=case.capacity,
            t_sim=t_sim, t_warmup=t_warmup, seed=job.seed,
        )
    else:
        from spe_sim.validation.single_queue import run_station

        result = run_station(
            case.kendall, case.lam, case.mu, c=case.c, capacity=case.capacity,
            t_sim=t_sim, t_warmup=t_warmup, seed=job.seed, kernel=job.kernel,
        )
    return case.key, job.impl, job.seed, result


def run_cases(
    cases: Iterable[Case],
    *,
    impls: Iterable[str],
    replications: int,
    n_arrivals: int,
    kernel: str = "coroutine",
    workers: int | None = None,
    progress: bool = True,
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Chạy song song; trả `{(case.key, impl): [kết quả từng seed]}`."""
    seeds = [SEED_BASE + i for i in range(replications)]
    jobs = [
        Job(case, seed, impl, kernel, n_arrivals)
        for case in cases
        for impl in impls
        if not (case.suite == "cpu" and impl == "reference")  # trạm CPU chỉ có bản thật
        for seed in seeds
    ]
    if progress:
        print(f"[run] {len(jobs)} lượt chạy trên {workers or os.cpu_count()} worker ...", flush=True)
    t0 = time.perf_counter()
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for key, impl, _seed, result in pool.map(_run_job, jobs, chunksize=2):
            out.setdefault((key, impl), []).append(result)
    if progress:
        print(f"[run] xong sau {time.perf_counter() - t0:.1f} s (wall-clock)", flush=True)
    return out


@dataclass(frozen=True, slots=True)
class Row:
    case: Case
    impl: str
    metric: str
    mean: float
    half: float
    analytic: float
    err: float
    in_ci: bool
    asserted: bool
    ok: bool


def evaluate(
    cases: Iterable[Case],
    results: dict[tuple[str, str], list[dict[str, Any]]],
    impl: str,
) -> list[Row]:
    rows: list[Row] = []
    for case in cases:
        runs = results.get((case.key, impl))
        if not runs:
            continue
        ana = case.analytic()
        for metric in METRICS:
            a = ana[metric]
            if a is None or (metric == "P_block" and a < P_BLOCK_NEGLIGIBLE):
                continue
            values = [r[metric] for r in runs if r.get(metric) is not None]
            if not values:
                continue
            mean, half = ci95(values)
            err = abs(mean - a) / a if a else float("nan")
            asserted = not (metric == "P_block" and a < P_BLOCK_FLOOR)
            # Dung sai số học nhỏ: ví dụ U = 1 − 1e-12 ở ρ = 1.2 khi CI có độ rộng 0.
            in_ci = abs(mean - a) <= half + 1e-9 * max(1.0, abs(a))
            ok = (err < threshold(case.rho)) if asserted else True
            rows.append(Row(case, impl, metric, mean, half, a, err, in_ci, asserted, ok))
    return rows


@dataclass(frozen=True, slots=True)
class CrossCheck:
    case: Case
    metric: str
    station: tuple[float, float]
    reference: tuple[float, float]

    @property
    def overlap(self) -> bool:
        (m1, h1), (m2, h2) = self.station, self.reference
        return abs(m1 - m2) <= h1 + h2


def cross_check(
    cases: Iterable[Case], results: dict[tuple[str, str], list[dict[str, Any]]]
) -> list[CrossCheck]:
    """Đối chứng chéo `StationCore` ↔ bản tham chiếu: CI 95 % của hai bên phải giao nhau."""
    out: list[CrossCheck] = []
    for case in cases:
        a, b = results.get((case.key, "station")), results.get((case.key, "reference"))
        if not a or not b:
            continue
        for metric in METRICS:
            va = [r[metric] for r in a if r.get(metric) is not None]
            vb = [r[metric] for r in b if r.get(metric) is not None]
            if not va or not vb:
                continue
            if metric == "P_block" and max(va + vb) == 0.0:
                continue
            out.append(CrossCheck(case, metric, ci95(va), ci95(vb)))
    return out


# --------------------------------------------------------------------------------------
# Xuất bảng
# --------------------------------------------------------------------------------------
def _fmt(metric: str, value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 1000:.3f}" if metric == "W" else f"{value:.4f}"


def print_table(rows: list[Row]) -> None:
    head = (
        f"{'Suite':8} {'Thành phần':34} {'Model':8} {'ρ':>4} {'Chỉ số':7} "
        f"{'Mô phỏng (mean ± CI95)':>26} {'Giải tích':>10} {'%Sai':>7} {'inCI':>5} Kết quả"
    )
    print(head)
    print("-" * len(head))
    for r in rows:
        sim = f"{_fmt(r.metric, r.mean)} ± {_fmt(r.metric, r.half)}"
        verdict = ("ĐẠT" if r.ok else "TRƯỢT") if r.asserted else "n/a"
        print(
            f"{r.case.suite:8} {r.case.label[:34]:34} {r.case.kendall:8} {r.case.rho:>4} "
            f"{r.metric:7} {sim:>26} {_fmt(r.metric, r.analytic):>10} {100 * r.err:6.2f}% "
            f"{'yes' if r.in_ci else 'no':>5} {verdict}"
        )


def render_markdown(
    rows: list[Row],
    checks: list[CrossCheck],
    *,
    replications: int,
    n_arrivals: int,
    kernel: str,
) -> str:
    titles = {
        "generic": "Bộ tham số chung (harness gốc, 1/µ = 10 ms)",
        "project": "Bộ tham số dự án (spec Bảng 1)",
        "cpu": "Trạm CPU cô lập của 4 runtime (dispatcher thật, A3)",
    }
    lines = [
        "# Kết quả kiểm chứng hàng đợi đơn",
        "",
        "Sinh tự động bởi `python -m spe_sim.validation`.",
        "",
        f"- R = {replications} seed ({SEED_BASE}–{SEED_BASE + replications - 1}), "
        f"khoảng {n_arrivals} lượt đến mỗi lần chạy, warm-up = 10 % T_sim, kernel = `{kernel}`.",
        "- Tiêu chí (spec §1.5.2): sai số tương đối < 5 % khi ρ ≤ 0.8, < 8 % khi ρ ≥ 0.9.",
        f"- P_block < {P_BLOCK_FLOOR} không ép ngưỡng (sai số tương đối của xác suất quá nhỏ "
        "không có nghĩa).",
        "- W tính bằng ms; các chỉ số khác không thứ nguyên.",
        "",
    ]
    for impl in ("station", "reference"):
        impl_rows = [r for r in rows if r.impl == impl]
        if not impl_rows:
            continue
        impl_title = (
            "Cài đặt của mạng (`ServiceLP`/`StationCore`, `NetworkLinkLP`, dispatcher)"
            if impl == "station"
            else "Bản tham chiếu độc lập (`reference_server.Server`)"
        )
        lines += [f"## {impl_title}", ""]
        for suite, title in titles.items():
            suite_rows = [r for r in impl_rows if r.case.suite == suite]
            if not suite_rows:
                continue
            asserted = [r for r in suite_rows if r.asserted]
            passed = sum(r.ok for r in asserted)
            outside = sum(not r.in_ci for r in suite_rows)
            lines += [
                f"### {title}",
                "",
                f"Đạt {passed}/{len(asserted)} phép so có ép ngưỡng; giá trị giải tích nằm "
                f"ngoài CI 95 % ở {outside}/{len(suite_rows)} dòng.",
                "",
                "| Thành phần | Model | ρ | λ (req/s) | Chỉ số | Mô phỏng (mean ± CI95) "
                "| Giải tích | % Sai số | Trong CI | Kết quả |",
                "|---|---|---|---|---|---|---|---|---|---|",
            ]
            for r in suite_rows:
                sim = f"{_fmt(r.metric, r.mean)} ± {_fmt(r.metric, r.half)}"
                verdict = ("ĐẠT" if r.ok else "**TRƯỢT**") if r.asserted else "n/a (< 0.01)"
                lines.append(
                    f"| {r.case.label} | {r.case.kendall} | {r.case.rho} | {r.case.lam:.1f} "
                    f"| {r.metric} | {sim} | {_fmt(r.metric, r.analytic)} | {100 * r.err:.2f}% "
                    f"| {'có' if r.in_ci else 'không'} | {verdict} |"
                )
            lines.append("")
    if checks:
        bad = [c for c in checks if not c.overlap]
        lines += [
            "## Đối chứng chéo: cài đặt của mạng ↔ bản tham chiếu",
            "",
            f"CI 95 % của hai cài đặt giao nhau ở {len(checks) - len(bad)}/{len(checks)} phép so.",
            "",
        ]
        if bad:
            lines += [
                "| Thành phần | Model | ρ | Chỉ số | Mạng | Tham chiếu |",
                "|---|---|---|---|---|---|",
            ]
            for c in bad:
                lines.append(
                    f"| {c.case.label} | {c.case.kendall} | {c.case.rho} | {c.metric} "
                    f"| {_fmt(c.metric, c.station[0])} ± {_fmt(c.metric, c.station[1])} "
                    f"| {_fmt(c.metric, c.reference[0])} ± {_fmt(c.metric, c.reference[1])} |"
                )
            lines.append("")
    return "\n".join(lines)
