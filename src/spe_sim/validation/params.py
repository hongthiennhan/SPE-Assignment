"""Bộ tham số kiểm chứng (A2, A3, A6) — spec §1.5.2 và Bảng 1.

Ba bộ:
    generic : tham số chung của harness hàng đợi đơn gốc (1/µ = 10 ms, K nhỏ để P_block
              đo được). Giữ nguyên để so được với `docs/validation_results.md` cũ.
    project : đúng tham số dự án theo Bảng 1 / Bảng 4 (K = 64, n_db, Auth, Catalog, ...).
    cpu     : trạm CPU cô lập của 4 runtime (A3) — so với M/M/1/64 và M/M/c/64.

Mức tải theo spec §1.5.2: ρ ∈ {0.3, 0.5, 0.7, 0.8, 0.9}, thêm 1.2 cho hàng đợi hữu hạn.
Ngưỡng: sai số tương đối < 5 % khi ρ ≤ 0.8 và < 8 % khi ρ ≥ 0.9.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from spe_sim.core.analytics import ANALYTIC_MODELS
from spe_sim.core.config import (
    AUTH_CHANNELS,
    AUTH_SERVICE_S,
    CATALOG_CHANNELS,
    CATALOG_SERVICE_S,
    DB_SERVICE_S,
    N_DB_LEVELS,
    NET_LINK_ONE_WAY_S,
    UV_POOL_CHANNELS,
    UV_POOL_SERVICE_S,
    get_profile,
)
from spe_sim.validation.cpu_station import CPU_STATION_MODELS, cpu_station_queue

__all__ = [
    "Case",
    "RHOS",
    "RHOS_FINITE",
    "SUITES",
    "build_cases",
    "threshold",
]

RHOS: Final[tuple[float, ...]] = (0.3, 0.5, 0.7, 0.8, 0.9)
RHOS_FINITE: Final[tuple[float, ...]] = RHOS + (1.2,)
FINITE: Final[frozenset[str]] = frozenset({"M/M/1/K", "M/M/n/n", "M/M/c/K"})

#: Với M/M/∞, ρ không xác định; quy ước λ = ρ · C_REF_INF · µ (như harness gốc).
C_REF_INF: Final[int] = 10

SUITES: Final[tuple[str, ...]] = ("generic", "project", "cpu")


def threshold(rho: float) -> float:
    """Ngưỡng sai số tương đối của spec §1.5.2."""
    return 0.05 if rho <= 0.8 else 0.08


@dataclass(frozen=True, slots=True)
class Case:
    """Một cấu hình hàng đợi ở một mức ρ."""

    suite: str
    label: str  # thành phần dự án tương ứng (Bảng 1)
    kendall: str
    rho: float
    lam: float
    mu: float
    c: int | None = None
    capacity: int | None = None
    runtime_model: str | None = None  # chỉ suite "cpu"

    @property
    def key(self) -> str:
        return f"{self.suite}|{self.label}|{self.kendall}|rho={self.rho}"

    def analytic(self) -> dict[str, float | None]:
        fn = ANALYTIC_MODELS[self.kendall]
        if self.kendall in ("M/M/1", "M/M/inf"):
            return fn(self.lam, self.mu)  # type: ignore[operator]
        if self.kendall == "M/M/1/K":
            return fn(self.lam, self.mu, self.capacity)  # type: ignore[operator]
        if self.kendall in ("M/M/n/n", "M/M/c"):
            return fn(self.lam, self.mu, self.c)  # type: ignore[operator]
        return fn(self.lam, self.mu, self.c, self.capacity)  # type: ignore[operator]


def _servers(kendall: str, c: int | None) -> float:
    if kendall in ("M/M/1", "M/M/1/K"):
        return 1
    if kendall == "M/M/inf":
        return C_REF_INF
    assert c is not None
    return c


def _sweep(suite: str, label: str, kendall: str, mu: float, c: int | None = None,
           capacity: int | None = None, runtime_model: str | None = None) -> list[Case]:
    rhos = RHOS_FINITE if kendall in FINITE else RHOS
    return [
        Case(suite, label, kendall, rho, rho * _servers(kendall, c) * mu, mu, c, capacity, runtime_model)
        for rho in rhos
    ]


def _generic() -> list[Case]:
    mu = 1.0 / 0.010  # 1/µ = 10 ms
    return [
        *_sweep("generic", "generic", "M/M/1", mu),
        *_sweep("generic", "generic K=10", "M/M/1/K", mu, capacity=10),
        *_sweep("generic", "generic", "M/M/inf", mu),
        *_sweep("generic", "generic n=10", "M/M/n/n", mu, c=10),
        *_sweep("generic", "generic c=4", "M/M/c", mu, c=4),
        *_sweep("generic", "generic c=4 K=16", "M/M/c/K", mu, c=4, capacity=16),
    ]


def _project() -> list[Case]:
    node = get_profile("nodejs_event_loop")
    bun = get_profile("bun_event_loop")
    loom = get_profile("spring_loom_vt")
    blocking = get_profile("blocking_baseline")
    cases: list[Case] = []
    for label, prof in (("Node.js event loop", node), ("Bun event loop", bun)):
        mu = 1.0 / prof.cpu_total_s
        cases += _sweep("project", f"{label} (K=∞)", "M/M/1", mu)
        cases += _sweep("project", f"{label} K=64", "M/M/1/K", mu, capacity=64)
    cases += _sweep("project", "Network Link (1 chiều 1 ms)", "M/M/inf", 1.0 / NET_LINK_ONE_WAY_S)
    for n in N_DB_LEVELS:
        cases += _sweep("project", f"DB pool n_db={n}", "M/M/n/n", 1.0 / DB_SERVICE_S, c=n)
    cases += _sweep("project", "Auth Cluster", "M/M/c", 1.0 / AUTH_SERVICE_S, c=AUTH_CHANNELS)
    cases += _sweep("project", "Catalog Service", "M/M/c", 1.0 / CATALOG_SERVICE_S, c=CATALOG_CHANNELS)
    cases += _sweep("project", "libuv Worker Pool", "M/M/c", 1.0 / UV_POOL_SERVICE_S, c=UV_POOL_CHANNELS)
    cases += _sweep("project", "Loom carriers K=64", "M/M/c/K", 1.0 / loom.cpu_total_s,
                    c=loom.channels, capacity=64)
    cases += _sweep("project", "Blocking OS threads (CPU) K=64", "M/M/c/K",
                    1.0 / blocking.cpu_total_s, c=blocking.channels, capacity=64)
    return cases


def _cpu() -> list[Case]:
    cases: list[Case] = []
    for model in CPU_STATION_MODELS:
        kendall, mu, c, capacity = cpu_station_queue(model)
        cases += _sweep("cpu", f"{model} (dispatcher)", kendall, mu, c=c, capacity=capacity,
                        runtime_model=model)
    # M/M/1/K không có tham số c: bỏ c=1 cho gọn khi gọi công thức.
    return [
        Case(x.suite, x.label, x.kendall, x.rho, x.lam, x.mu,
             None if x.kendall == "M/M/1/K" else x.c, x.capacity, x.runtime_model)
        for x in cases
    ]


def build_cases(suites: tuple[str, ...] | list[str]) -> list[Case]:
    builders = {"generic": _generic, "project": _project, "cpu": _cpu}
    out: list[Case] = []
    for suite in suites:
        if suite not in builders:
            raise ValueError(f"suite phải thuộc {SUITES}, nhận {suite!r}")
        out += builders[suite]()
    return out
