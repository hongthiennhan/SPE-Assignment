"""T19 — Verification: bất biến toàn hệ với **LP thật** (không phải service cố định).

Chạy:  python -m tests.verify_invariants

Kiểm trên mỗi kịch bản:
    ΔN = 0                : admitted + dropped = arrived, và n(t) = 0 ở cuối
    1 kết cục / request   : số notice = số admitted, không trùng request_id
    3.0 × N_admitted      : branch_requests = 3 × admitted
    causality/monotonic   : t_arrive ≤ t_dispatch ≤ t_complete, không lùi thời gian
    Little's Law          : |E[N] − X·E[r]| / E[N] ≤ 10 %  (spec §1.5.2)

`E[N]` lấy từ tích phân `N(t)` của dispatcher (`mean_in_system`), không phải trung bình mẫu
tại thời điểm chốt kết cục — hai thứ này khác nhau và chỉ cái đầu đúng cho Little.
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
from spe_sim.core.kernel import CoroutineKernel, SequentialKernel  # noqa: E402
from spe_sim.core.rng import SeedStreams  # noqa: E402
from spe_sim.lp.infra.admission import AdmissionControlLP  # noqa: E402
from spe_sim.lp.infra.auth_cluster import AuthClusterLP  # noqa: E402
from spe_sim.lp.infra.catalog_service import CatalogServiceLP  # noqa: E402
from spe_sim.lp.infra.db_pool import DbPoolLP  # noqa: E402
from spe_sim.lp.infra.network_link import NetworkLinkLP  # noqa: E402
from spe_sim.lp.infra.sinks import all_notices, build_sinks  # noqa: E402
from spe_sim.lp.infra.uv_pool import UvWorkerPoolLP  # noqa: E402
from spe_sim.lp.infra.source import SourceLP  # noqa: E402
from spe_sim.lp.runtime.models.base import build_model  # noqa: E402

LITTLE_TOLERANCE = 0.10


@dataclass(frozen=True)
class Scenario:
    runtime_model: str
    join_mode: str
    lam: float
    t_sim: float = 10.0
    seed: int = 7
    p_pin: float = 0.0
    p_uv: float = 0.0
    n_db: int = 24


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("blocking_baseline", "gather", 50.0),
    Scenario("nodejs_event_loop", "gather", 150.0),
    Scenario("bun_event_loop", "as_completed", 200.0),
    Scenario("spring_loom_vt", "gather", 250.0),
    Scenario("spring_loom_vt", "as_completed", 250.0, p_pin=0.20),
    Scenario("nodejs_event_loop", "gather", 150.0, p_uv=0.30, seed=11),
)


ENGINES = ("sequential", "coroutine")


def run(scenario: Scenario, engine: str = "sequential"):
    streams = SeedStreams(scenario.seed)
    source = SourceLP(scenario.lam, streams.stream("source"), scenario.t_sim)
    admission = AdmissionControlLP(
        scenario.runtime_model, scenario.join_mode  # type: ignore[arg-type]
    )
    model = build_model(
        scenario.runtime_model, p_pin=scenario.p_pin, p_uv=scenario.p_uv
    )
    builder = model.build_coroutine_dispatcher if engine == "coroutine" else model.build_dispatcher
    dispatcher = builder(
        scenario.join_mode,  # type: ignore[arg-type]
        rng=SeedStreams(scenario.seed).stream("dispatcher")
        if (scenario.p_pin or scenario.p_uv)
        else None,
    )
    link = NetworkLinkLP(streams.stream("network"))
    auth = AuthClusterLP(streams.stream("lp", "auth"))
    catalog = CatalogServiceLP(streams.stream("lp", "catalog"))
    db = DbPoolLP(streams.stream("lp", "db"), n_db=scenario.n_db)
    uv = UvWorkerPoolLP(streams.stream("lp", "uv"))
    sinks = build_sinks()

    kernel_cls = CoroutineKernel if engine == "coroutine" else SequentialKernel
    kernel = kernel_cls(end_time=scenario.t_sim + 1.0)
    kernel.add(
        source, admission, dispatcher, link, auth, catalog, db, uv, *sinks.values()
    )
    kernel.run()
    return source, admission, dispatcher, all_notices(sinks.values()), kernel


def evaluate(scenario: Scenario, engine: str = "sequential") -> tuple[list[str], dict[str, float | int]]:
    problems: list[str] = []
    source, admission, dispatcher, notices, kernel = run(scenario, engine)

    if admission.n_in_system != 0:
        problems.append(f"ΔN: còn {admission.n_in_system} request trong hệ thống")
    if admission.admitted + admission.dropped != source.emitted:
        problems.append(
            f"ΔN: {admission.admitted}+{admission.dropped} != {source.emitted} arrived"
        )
    if len(notices) != admission.admitted:
        problems.append(f"notice {len(notices)} != admitted {admission.admitted}")
    if len({n.request_id for n in notices}) != len(notices):
        problems.append("có request_id trùng trong kết cục")
    ratio = dispatcher.branch_requests / max(admission.admitted, 1)
    if abs(ratio - 3.0) > 1e-9:
        problems.append(f"branch ratio {ratio:.6f} != 3.0")
    for notice in notices:
        if not (notice.t_arrive <= notice.t_dispatch <= notice.t_complete):
            problems.append(f"vi phạm nhân quả ở request {notice.request_id}")
            break

    if not notices:
        return ["không có kết cục nào"], {}

    # Cửa sổ đo = toàn bộ thời gian kernel chạy, để X và E[N] cùng một khoảng.
    span = max(dispatcher.last_time, source.t_sim, kernel.current_time)
    r_s = sum((n.t_complete - n.t_arrive) for n in notices) / len(notices)
    x_rps = admission.admitted / span
    mean_n = dispatcher.mean_in_system(span)
    little = x_rps * r_s
    if mean_n > 1e-9 and abs(mean_n - little) / mean_n > LITTLE_TOLERANCE:
        problems.append(f"Little: E[N]={mean_n:.3f} vs X·E[r]={little:.3f}")

    row: dict[str, float | int] = {
        "admitted": admission.admitted,
        "dropped": admission.dropped,
        "e_r_ms": r_s / MS,
        "e_n": mean_n,
        "x_rps": x_rps,
    }
    return problems, row


def main() -> int:
    print(
        f"{'engine':<12}{'runtime':<18}{'join':<14}{'lambda':>7}{'p_pin':>7}{'p_uv':>6}"
        f"{'adm':>6}{'drop':>6}{'E[r]ms':>9}{'E[N]':>8}{'status':>8}"
    )
    print("-" * 108)
    failures = 0
    for engine in ENGINES:
        for scenario in SCENARIOS:
            problems, row = evaluate(scenario, engine)
            status = "OK" if not problems else "FAIL"
            print(
                f"{engine:<12}{scenario.runtime_model:<18}{scenario.join_mode:<14}{scenario.lam:>7.0f}"
                f"{scenario.p_pin:>7.2f}{scenario.p_uv:>6.2f}{row.get('admitted', 0):>6}"
                f"{row.get('dropped', 0):>6}{row.get('e_r_ms', 0.0):>9.2f}"
                f"{row.get('e_n', 0.0):>8.3f}{status:>8}"
            )
            for problem in problems:
                print(f"    ↳ {problem}")
                failures += 1

    print()
    if failures:
        print(f"{failures} vi phạm. ❌")
        return 1
    print("Tất cả bất biến đạt. ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
