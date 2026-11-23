"""Runner: lắp toàn bộ hệ thống từ `ScenarioConfig`, chạy một kịch bản, xuất CSV D3.

Đây là mảnh nối giữa `configs/scenarios.json` (T20) và `core/records.py` (D3). Nó cố ý
**mỏng**: không tính thống kê, không tính CI — việc đó của dev runner/thống kê.

    cfg = ScenarioConfig(runtime_model="bun_event_loop", join_mode="gather", lam=150, seed=1000)
    result = run_scenario(cfg)          # chạy T_sim giây ảo
    result.metrics["e_r_ms"]            # E[r] trên cửa sổ [T_warmup, T_sim]
    result.write_csv("results/bun_gather_150.csv")

Kiến trúc giữ nguyên: mỗi LP chỉ trả `Emission`; `core/kernel.py` là nơi duy nhất biết
`bkitevsim`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from spe_sim.core.config import ScenarioConfig
from spe_sim.core.events import OutcomeNotice
from spe_sim.core.kernel import CoroutineKernel, SequentialKernel
from spe_sim.core.records import RequestRecord, records_from_notices, write_csv
from spe_sim.core.rng import SeedStreams
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.auth_cluster import AuthClusterLP
from spe_sim.lp.infra.catalog_service import CatalogServiceLP
from spe_sim.lp.infra.db_pool import DbPoolLP
from spe_sim.lp.infra.metrics import MetricsRecorder
from spe_sim.lp.infra.network_link import NetworkLinkLP
from spe_sim.lp.infra.sinks import all_notices, build_sinks
from spe_sim.lp.infra.source import SourceLP
from spe_sim.lp.infra.uv_pool import UvWorkerPoolLP
from spe_sim.lp.runtime.models.base import build_model

__all__ = ["RunResult", "System", "build_system", "run_scenario"]


@dataclass
class System:
    """Các LP của một lần chạy, giữ lại để đọc thống kê nội bộ từng trạm."""

    kernel: SequentialKernel
    source: SourceLP
    admission: AdmissionControlLP
    dispatcher: Any
    link: NetworkLinkLP
    auth: AuthClusterLP
    catalog: CatalogServiceLP
    db: DbPoolLP
    uv: UvWorkerPoolLP
    sinks: dict[str, Any]


@dataclass
class RunResult:
    config: ScenarioConfig
    system: System
    notices: list[OutcomeNotice]
    metrics: dict[str, float | int | None]
    stats: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> list[RequestRecord]:
        cfg = self.config
        return records_from_notices(
            self.notices,
            seed=cfg.seed,
            runtime_model=cfg.runtime_model,
            join_mode=cfg.join_mode,
            lam=cfg.lam,
        )

    def write_csv(self, path: str | Path) -> Path:
        return write_csv(path, self.records)


def build_system(config: ScenarioConfig, *, kernel: str = "coroutine") -> System:
    """Dựng toàn bộ hệ thống cho một `ScenarioConfig` (chưa chạy).

    `kernel="coroutine"` (mặc định) chạy trên `BkSysCoEventLoop` — asyncio, join bằng
    `asyncio.gather`/`as_completed` — đúng spec §1.1.2. `kernel="sequential"` là bản đồng
    bộ dùng để đối chứng cơ chế.
    """
    if kernel not in ("coroutine", "sequential"):
        raise ValueError(f"kernel phải là 'coroutine' hoặc 'sequential', nhận {kernel!r}")
    streams = SeedStreams(config.seed)
    source = SourceLP(config.lam, streams.stream("source"), config.t_sim)
    admission = AdmissionControlLP(
        config.runtime_model, config.join_mode, capacity_k=config.capacity_k
    )
    model = build_model(config.runtime_model, p_pin=config.p_pin, p_uv=config.p_uv)
    rng = streams.stream("dispatcher") if (config.p_pin or config.p_uv) else None
    builder = model.build_coroutine_dispatcher if kernel == "coroutine" else model.build_dispatcher
    dispatcher = builder(
        config.join_mode,
        t_timeout=config.t_timeout,
        rng=rng,
    )
    link = NetworkLinkLP(streams.stream("network"))
    auth = AuthClusterLP(streams.stream("lp", "auth"))
    catalog = CatalogServiceLP(streams.stream("lp", "catalog"))
    db = DbPoolLP(streams.stream("lp", "db"), n_db=config.n_db)
    uv = UvWorkerPoolLP(streams.stream("lp", "uv"))
    sinks = build_sinks()

    # `end_time` phải lớn hơn T_sim: dispatcher của bkitevsim bỏ event vượt end_time
    # (report.md §9.4 #2) ⇒ cộng đệm để request cuối chạy xong, thống kê vẫn cắt ở T_sim.
    kernel_cls = CoroutineKernel if kernel == "coroutine" else SequentialKernel
    kernel = kernel_cls(end_time=config.t_sim + 1.0)
    kernel.add(
        source, admission, dispatcher, link, auth, catalog, db, uv, *sinks.values()
    )
    return System(
        kernel=kernel,
        source=source,
        admission=admission,
        dispatcher=dispatcher,
        link=link,
        auth=auth,
        catalog=catalog,
        db=db,
        uv=uv,
        sinks=sinks,
    )


def run_scenario(
    config: ScenarioConfig, *, record: bool = True, kernel: str = "coroutine"
) -> RunResult:
    """Chạy một kịch bản tới hết `T_sim` (drain thêm tối đa 1 s ảo)."""
    system = build_system(config, kernel=kernel)
    stats = system.kernel.run()
    notices = all_notices(system.sinks.values())

    metrics: dict[str, float | int | None] = {}
    if record:
        recorder = MetricsRecorder(config.t_warmup, config.t_sim, capacity_k=config.capacity_k)
        recorder.extend(notices)
        metrics = recorder.summary()
    return RunResult(config=config, system=system, notices=notices, metrics=metrics, stats=stats)
