"""Tự review theo spec §2.1 + §3.3: chạy 4 runtime model tại λ0 = 150 req/s.

Chạy:  python -m tests.verify_baseline

Đối chiếu **ngưỡng bắt buộc** (spec §2.1):
    X ≥ 148.5 req/s (≥ 99 % lưu lượng),  E[r] ≤ 80 ms,  P95 ≤ 150 ms,  P_loss ≤ 1 %.

Và đối chiếu **giá trị đối chứng** của spec §3.3 (dung sai ±10 %, vì đây là kỳ vọng giải tích
của một mô hình đơn giản hoá, còn mô phỏng có thêm hàng đợi hữu hạn và mạng):
    Blocking ≈ 72.60 ms · Node.js ≈ 44.99 ms · Bun ≈ 41.70 ms · Loom ≈ 41.34 ms

Kèm một kịch bản quá tải (Blocking tại λ = 200 > trần 188.98 req/s) để chứng minh cơ chế
503 hoạt động thật, không chỉ nằm trên giấy.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from spe_sim.core.config import (  # noqa: E402
    LAMBDA_NOMINAL,
    T_SIM_S,
    T_WARMUP_S,
    ScenarioConfig,
)
from spe_sim.runner.study import run_scenario  # noqa: E402

#: spec §3.3 — E[r] đối chứng tại λ0 (ms) cho join 3/3.
SPEC_E_R_MS: dict[str, float] = {
    "blocking_baseline": 72.60,
    "nodejs_event_loop": 44.99,
    "bun_event_loop": 41.70,
    "spring_loom_vt": 41.34,
}
E_R_TOLERANCE = 0.10

#: spec §2.1 — ngưỡng bắt buộc tại λ0.
MIN_THROUGHPUT_RPS = 148.5
MAX_E_R_MS = 80.0
MAX_P95_MS = 150.0
MAX_P_LOSS = 0.01


@dataclass
class Row:
    runtime_model: str
    lam: float
    x: float
    e_r: float
    p95: float
    p_loss_pct: float
    expected_e_r: float | None
    seconds: float


def measure(runtime_model: str, lam: float, *, seed: int = 1000) -> tuple[Row, object]:
    config = ScenarioConfig(
        runtime_model=runtime_model,  # type: ignore[arg-type]
        join_mode="gather",
        lam=lam,
        seed=seed,
        t_sim=T_SIM_S,
        t_warmup=T_WARMUP_S,
    )
    started = time.perf_counter()
    result = run_scenario(config)
    elapsed = time.perf_counter() - started
    metrics = result.metrics
    row = Row(
        runtime_model=runtime_model,
        lam=lam,
        x=float(metrics["x_completed_rps"] or 0.0),
        e_r=float(metrics["e_r_ms"] or 0.0),
        p95=float(metrics["p95_ms"] or 0.0),
        p_loss_pct=float(metrics["p_loss"] or 0.0) * 100.0,
        expected_e_r=SPEC_E_R_MS.get(runtime_model),
        seconds=elapsed,
    )
    return row, result


def evaluate(row: Row) -> list[str]:
    problems: list[str] = []
    if row.lam == LAMBDA_NOMINAL:
        if row.x < MIN_THROUGHPUT_RPS:
            problems.append(f"X={row.x:.2f} < {MIN_THROUGHPUT_RPS}")
        if row.e_r > MAX_E_R_MS:
            problems.append(f"E[r]={row.e_r:.2f} > {MAX_E_R_MS}")
        if row.p95 > MAX_P95_MS:
            problems.append(f"P95={row.p95:.2f} > {MAX_P95_MS}")
        if row.p_loss_pct > MAX_P_LOSS * 100:
            problems.append(f"P_loss={row.p_loss_pct:.3f}% > 1%")
        if row.expected_e_r is not None:
            delta = abs(row.e_r - row.expected_e_r) / row.expected_e_r
            if delta > E_R_TOLERANCE:
                problems.append(
                    f"E[r]={row.e_r:.2f} lệch {delta*100:.1f}% so với spec {row.expected_e_r:.2f}"
                )
    return problems


def main() -> int:
    print(f"λ0 = {LAMBDA_NOMINAL:.0f} req/s · T_sim = {T_SIM_S:.0f}s · T_warmup = {T_WARMUP_S:.0f}s\n")
    header = (
        f"{'runtime_model':<18}{'X (req/s)':>10}{'E[r] (ms)':>11}{'P95 (ms)':>10}"
        f"{'P_loss':>9}{'spec E[r]':>11}{'lệch':>8}{'wall':>7}"
    )
    print(header)
    print("-" * len(header))
    failures = 0
    for model in SPEC_E_R_MS:
        row, result = measure(model, LAMBDA_NOMINAL)
        problems = evaluate(row)
        delta = (
            (row.e_r - row.expected_e_r) / row.expected_e_r * 100.0
            if row.expected_e_r
            else 0.0
        )
        print(
            f"{model:<18}{row.x:>10.2f}{row.e_r:>11.2f}{row.p95:>10.2f}"
            f"{row.p_loss_pct:>8.3f}%{row.expected_e_r or 0:>11.2f}{delta:>+7.1f}%"
            f"{row.seconds:>6.1f}s"
        )
        for problem in problems:
            print(f"    ↳ {problem}")
            failures += 1

        # bất biến cấu trúc
        adm = result.system.admission
        disp = result.system.dispatcher
        if adm.n_in_system != 0:
            print(f"    ↳ ΔN: còn {adm.n_in_system} request")
            failures += 1
        if disp.branch_requests != 3 * adm.admitted:
            print(f"    ↳ branch {disp.branch_requests} != 3×{adm.admitted}")
            failures += 1

    # Quá tải: Blocking tại λ = 200 vượt trần 12 × (1000/63.5) ≈ 188.98 req/s.
    row, result = measure("blocking_baseline", 200.0, seed=1001)
    print(
        f"\nQuá tải: blocking @ λ=200 → X={row.x:.2f} req/s, P_loss={row.p_loss_pct:.2f}% "
        f"(kỳ vọng có mất request vì vượt trần ≈188.98), dropped="
        f"{result.system.admission.dropped}, wall={row.seconds:.1f}s"
    )
    if row.p_loss_pct <= 0.0:
        print("    ↳ FAIL: quá tải mà không mất request — cơ chế 503 chưa hoạt động?")
        failures += 1

    print()
    if failures:
        print(f"{failures} vấn đề. ❌")
        return 1
    print("Đạt toàn bộ ngưỡng spec §2.1 và giá trị đối chứng §3.3. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
