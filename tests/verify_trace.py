"""T18 — Verification: deterministic trace phải khớp Hình 4 của spec.

Chạy:  python -m tests.verify_trace        (hoặc python tests/verify_trace.py)

Mỗi kịch bản dùng service **cố định** (12 / 22 / 27 ms, đã gồm 2 ms khứ hồi) nên kết quả phải
là đẳng thức, sai lệch 1e-9 s cũng là lỗi. Đây là bằng chứng cho Phase 3 của Cross-team.

Trace được kiểm trên **cả hai** engine:

- `sequential` — `SequentialEventDispatcher` (state machine đồng bộ),
- `coroutine`  — `BkSysCoEventLoop` + `asyncio.gather`/`as_completed`, đúng spec §1.1.2.

Hai engine phải cho cùng một con số; nếu lệch nghĩa là một trong hai đang sai.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from spe_sim.core.config import MS  # noqa: E402
from spe_sim.core.events import Outcome  # noqa: E402
from spe_sim.runner.deterministic import (  # noqa: E402
    ScenarioResult,
    run_deterministic,
    run_deterministic_coroutine,
)

TOLERANCE_MS = 1e-6

RUNNERS = {
    "sequential": run_deterministic,
    "coroutine": run_deterministic_coroutine,
}


@dataclass(frozen=True)
class Case:
    runtime_model: str
    join_mode: str
    expected_ms: float
    formula: str


CASES: tuple[Case, ...] = (
    Case("blocking_baseline", "gather", 63.50, "2.5 + 12 + 22 + 27"),
    Case("spring_loom_vt", "gather", 29.50, "1.5 + max(12,22,27) + 1.0"),
    Case("spring_loom_vt", "as_completed", 24.50, "1.5 + max(12,22) + 1.0"),
    Case("nodejs_event_loop", "gather", 30.20, "1.92 + 27 + 1.28"),
    Case("bun_event_loop", "as_completed", 24.00, "1.20 + 22 + 0.80"),
)


def main() -> int:
    failures: list[str] = []
    print(f"{'runtime_model':<18}{'join':<14}{'engine':<12}{'formula':<30}{'expect':>8}{'got':>10}  ok")
    print("-" * 96)

    for case in CASES:
        values: dict[str, float] = {}
        for engine, runner in RUNNERS.items():
            result: ScenarioResult = runner(case.runtime_model, case.join_mode)  # type: ignore[arg-type]
            if len(result.notices) != 1:
                failures.append(
                    f"{case.runtime_model}/{case.join_mode}/{engine}: {len(result.notices)} notice"
                )
                continue
            notice = result.notices[0]
            got_ms = (notice.t_complete - notice.t_arrive) / MS
            values[engine] = got_ms
            ok = (
                notice.outcome is Outcome.COMPLETED
                and abs(got_ms - case.expected_ms) <= TOLERANCE_MS
            )
            print(
                f"{case.runtime_model:<18}{case.join_mode:<14}{engine:<12}{case.formula:<30}"
                f"{case.expected_ms:>8.2f}{got_ms:>10.2f}  {'OK' if ok else 'FAIL'}"
            )
            if not ok:
                failures.append(
                    f"{case.runtime_model}/{case.join_mode}/{engine}: got {got_ms:.6f} ms, "
                    f"expect {case.expected_ms:.2f} ms, outcome={notice.outcome.value}"
                )
        if len(values) == 2 and abs(values["sequential"] - values["coroutine"]) > TOLERANCE_MS:
            failures.append(
                f"{case.runtime_model}/{case.join_mode}: hai engine lệch nhau "
                f"({values['sequential']:.9f} vs {values['coroutine']:.9f} ms)"
            )

    # Bất biến phụ: luôn đúng 3.0 nhánh cho mỗi request admitted, trên cả hai engine.
    for engine, runner in RUNNERS.items():
        result = runner("blocking_baseline", "gather", arrivals=(0.0, 0.1, 0.2))
        ratio = result.dispatcher.branch_requests / max(result.admission.admitted, 1)
        ok_ratio = abs(ratio - 3.0) <= 1e-9
        print(
            f"\n3.0 × N_admitted [{engine}]: {result.dispatcher.branch_requests} / "
            f"{result.admission.admitted} = {ratio:.1f}  {'OK' if ok_ratio else 'FAIL'}"
        )
        if not ok_ratio:
            failures.append(f"branch ratio {ratio} != 3.0 ({engine})")

    print()
    if failures:
        for line in failures:
            print(f"FAIL: {line}")
        return 1
    print("Tất cả trace khớp spec trên cả hai engine. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
