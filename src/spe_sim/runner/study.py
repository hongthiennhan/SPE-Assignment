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

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from spe_sim.core.config import N_DB_DEFAULT, ScenarioConfig
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

__all__ = [
    "RunResult",
    "System",
    "build_system",
    "load_scenarios",
    "run_grid",
    "run_scenario",
    "scenario_configs",
]

#: `configs/scenarios.json` mặc định (study.py → runner → configs → repo root).
DEFAULT_SCENARIOS_PATH: Path = Path(__file__).resolve().parents[3] / "configs" / "scenarios.json"


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


def load_scenarios(path: str | Path | None = None) -> dict[str, Any]:
    """Đọc `configs/scenarios.json` (base + 72 kịch bản chính + 42 sensitivity)."""
    source = Path(path) if path is not None else DEFAULT_SCENARIOS_PATH
    return json.loads(source.read_text(encoding="utf-8"))


def scenario_configs(
    data: dict[str, Any],
    *,
    seeds: Iterable[int] | None = None,
    t_sim: float | None = None,
) -> list[tuple[str, ScenarioConfig]]:
    """Trải mỗi entry trong JSON thành `(id, ScenarioConfig)` cho từng seed.

    Mọi entry đều đi qua `ScenarioConfig` nên các ràng buộc của spec (§3.2: `p_pin ≤ 0.20`,
    `p_uv ≤ 0.30`, `T_warmup < T_sim`) được kiểm ngay lúc nạp, không đợi tới lúc chạy.
    """
    base = data["base"]
    chosen = list(base["seeds"] if seeds is None else seeds)
    if not chosen:
        raise ValueError("cần ít nhất một seed")
    duration = float(base["t_sim"] if t_sim is None else t_sim)
    warmup = float(base["t_warmup"]) if t_sim is None else min(float(base["t_warmup"]), duration / 10)
    out: list[tuple[str, ScenarioConfig]] = []
    entries = list(data["scenarios"]) + list(data.get("sensitivity", []))
    for entry in entries:
        for seed in chosen:
            out.append(
                (
                    entry["id"],
                    ScenarioConfig(
                        runtime_model=entry["runtime_model"],
                        join_mode=entry["join_mode"],
                        lam=float(entry["lam"]),
                        seed=int(seed),
                        n_db=int(entry.get("n_db", N_DB_DEFAULT)),
                        p_pin=float(entry.get("p_pin", 0.0)),
                        p_uv=float(entry.get("p_uv", 0.0)),
                        t_sim=duration,
                        t_warmup=warmup,
                        capacity_k=int(base["capacity_k"]),
                        t_timeout=float(base["t_timeout"]),
                    ),
                )
            )
    return out


def run_grid(
    out_dir: str | Path,
    *,
    data: dict[str, Any] | None = None,
    kernel: str = "coroutine",
    replications: int | None = None,
    t_sim: float | None = None,
    limit: int | None = None,
) -> list[Path]:
    """Chạy toàn bộ kịch bản trong `configs/scenarios.json` và ghi **một CSV / kịch bản**.

    Mỗi dòng trong CSV là một request của một replication (cột `seed` phân biệt), đúng schema
    D3 trong `core/records.py` — đây là đầu vào cho phần thống kê CI của dev khác.
    """
    payload = load_scenarios() if data is None else data
    seeds = payload["base"]["seeds"]
    if replications is not None:
        seeds = seeds[:replications]
    entries = scenario_configs(payload, seeds=seeds, t_sim=t_sim)
    if limit is not None:
        entries = entries[:limit]

    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    grouped: dict[str, list[RequestRecord]] = {}
    for scenario_id, config in entries:
        result = run_scenario(config, kernel=kernel)
        grouped.setdefault(scenario_id, []).extend(result.records)

    written: list[Path] = []
    for scenario_id, records in grouped.items():
        # `id` chứa "|" — không hợp lệ trong tên file trên Windows, nên đổi sang "_".
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", scenario_id).strip("_")
        written.append(write_csv(target / f"{safe}.csv", records))
    return written


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
