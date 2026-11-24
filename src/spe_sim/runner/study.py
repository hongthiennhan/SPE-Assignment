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

import csv
import json
import re
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

from spe_sim.core.config import N_DB_DEFAULT, ScenarioConfig
from spe_sim.core.stats import summarize_replications
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
    "SUMMARY_METRICS",
    "SUMMARY_FILENAME",
]

#: Chỉ số đầu ra chính (spec §2) — tóm tắt mean ± CI 95 % qua R replication (C1).
SUMMARY_METRICS: tuple[str, ...] = ("x_completed_rps", "e_r_ms", "p95_ms", "p_loss")
SUMMARY_FILENAME = "summary.csv"

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
    model = build_model(
        config.runtime_model, p_pin=config.p_pin, p_uv=config.p_uv, cpu_scale=config.cpu_scale
    )
    rng = streams.stream("dispatcher") if (config.p_pin or config.p_uv) else None
    # A4: Stochastic Workload ⇒ CPU ~ Exp trên stream riêng "cpu" (spec §1.5.1).
    cpu_rng = streams.stream("cpu") if config.cpu_dist == "exp" else None
    builder = model.build_coroutine_dispatcher if kernel == "coroutine" else model.build_dispatcher
    dispatcher = builder(
        config.join_mode,
        t_timeout=config.t_timeout,
        rng=rng,
        cpu_rng=cpu_rng,
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
    """Đọc `configs/scenarios.json` (base + 72 kịch bản chính + 192 sensitivity, xem `counts`)."""
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
                        cpu_dist=str(base.get("cpu_dist", "exp")),
                        cpu_scale=float(entry.get("cpu_scale", 1.0)),
                    ),
                )
            )
    return out


def _run_entry(args: tuple[str, ScenarioConfig, str]) -> tuple[str, list[RequestRecord], dict]:
    """Một (kịch bản, seed) — hàm mức module để `ProcessPoolExecutor` pickle được."""
    scenario_id, config, kernel = args
    result = run_scenario(config, kernel=kernel)
    return scenario_id, result.records, result.metrics


def _summary_row(scenario_id: str, config: ScenarioConfig, runs: list[dict]) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": scenario_id,
        "runtime_model": config.runtime_model,
        "join_mode": config.join_mode,
        "lam": config.lam,
        "n_db": config.n_db,
        "p_pin": config.p_pin,
        "p_uv": config.p_uv,
        "cpu_scale": config.cpu_scale,
        "replications": len(runs),
    }
    for key, value in summarize_replications(runs, SUMMARY_METRICS).items():
        row[f"{key}_mean"] = "" if value is None else f"{value[0]:.6f}"
        row[f"{key}_ci95"] = "" if value is None else f"{value[1]:.6f}"
    return row


def run_grid(
    out_dir: str | Path,
    *,
    data: dict[str, Any] | None = None,
    kernel: str = "coroutine",
    replications: int | None = None,
    t_sim: float | None = None,
    limit: int | None = None,
    workers: int = 1,
    only: Iterable[str] | None = None,
) -> list[Path]:
    """Chạy toàn bộ kịch bản trong `configs/scenarios.json` và ghi **một CSV / kịch bản**.

    Mỗi dòng trong CSV là một request của một replication (cột `seed` phân biệt), đúng schema
    D3 trong `core/records.py`. Ngoài ra ghi `summary.csv`: mỗi kịch bản một dòng với
    mean ± nửa độ rộng CI 95 % Student-t của X, E[r], P95, P_loss qua các replication (C1).

    `workers > 1` chạy song song bằng tiến trình. CSV của một kịch bản được ghi ngay khi đủ
    mọi seed của nó, nên bộ nhớ chỉ giữ một kịch bản mỗi lúc. `only` lọc theo tiền tố `id`.
    Trả danh sách CSV theo kịch bản (không gồm `summary.csv`).
    """
    payload = load_scenarios() if data is None else data
    seeds = payload["base"]["seeds"]
    if replications is not None:
        seeds = seeds[:replications]
    entries = scenario_configs(payload, seeds=seeds, t_sim=t_sim)
    if only is not None:
        prefixes = tuple(only)
        entries = [e for e in entries if e[0].startswith(prefixes)]
    if limit is not None:
        entries = entries[:limit]

    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    configs = {scenario_id: config for scenario_id, config in entries}
    jobs = [(scenario_id, config, kernel) for scenario_id, config in entries]

    def results() -> Iterator[tuple[str, list[RequestRecord], dict]]:
        if workers <= 1:
            yield from map(_run_entry, jobs)
            return
        with ProcessPoolExecutor(max_workers=workers) as pool:
            yield from pool.map(_run_entry, jobs)

    written: list[Path] = []
    summary: list[dict[str, Any]] = []
    current: str | None = None
    records: list[RequestRecord] = []
    metrics: list[dict] = []

    def flush() -> None:
        if current is None:
            return
        # `id` chứa "|" — không hợp lệ trong tên file trên Windows, nên đổi sang "_".
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", current).strip("_")
        written.append(write_csv(target / f"{safe}.csv", records))
        summary.append(_summary_row(current, configs[current], metrics))

    # `scenario_configs` xếp mọi seed của một kịch bản liền nhau; `pool.map` giữ thứ tự.
    for scenario_id, run_records, run_metrics in results():
        if scenario_id != current:
            flush()
            current, records, metrics = scenario_id, [], []
        records.extend(run_records)
        metrics.append(run_metrics)
    flush()

    if summary:
        with (target / SUMMARY_FILENAME).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(summary[0]))
            writer.writeheader()
            writer.writerows(summary)
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
