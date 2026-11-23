"""Hằng số và cấu hình của hệ thống mô phỏng.

Nguồn số liệu (không tự đặt thêm):
    - spec §1.1.1 : 4 mô hình runtime, số kênh, chi phí CPU vòng lặp
    - spec §1.2   : System Boundary (SLA, K, Network Link, 3 downstream services)
    - spec §3.1   : phân loại Constants / Parameters / Variables, cơ chế 2 pha CPU
    - spec §3.2   : Study factors (λ, p_pin, p_uv, n_db)
    - spec §3.3   : giá trị đối chiếu giải tích
    - docs/team/Thống nhất DEV.md : tên định danh runtime_model / join_mode / EVENT_TYPES

QUY ƯỚC ĐƠN VỊ (spec §2.2, DEV.md §1):
    Mọi đại lượng thời gian nội bộ tính bằng **giây (s)**, tốc độ bằng **req/s**.
    Chỉ nhân 1000 sang ms ở biên xuất CSV / vẽ biểu đồ.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# --------------------------------------------------------------------------------------
# Hệ số đổi đơn vị — dùng duy nhất ở biên (CSV / biểu đồ)
# --------------------------------------------------------------------------------------
MS = 1e-3  # 1 ms = 0.001 s

# --------------------------------------------------------------------------------------
# Constants — Hạ tầng cố định (spec §1.2, §3.1)
# --------------------------------------------------------------------------------------
N_BRANCHES: int = 3  # m = 3 downstream services
N_CORES: int = 4  # c_cores = 4

# Internal Network Link: M/M/∞, khứ hồi 2.0 ms (1.0 ms mỗi chiều), Wq = 0
NET_LINK_ONE_WAY_S: float = 1.0 * MS
NET_LINK_RTT_S: float = 2.0 * MS
MU_NET: float = 1.0 / NET_LINK_RTT_S  # 500 req/s

# Ba downstream services dùng chung cho cả 4 runtime model
AUTH_CHANNELS: int = 8  # M/M/8
AUTH_SERVICE_S: float = 10.0 * MS
AUTH_CAPACITY_RPS: float = 800.0

CATALOG_CHANNELS: int = 16  # M/M/16
CATALOG_SERVICE_S: float = 20.0 * MS
CATALOG_CAPACITY_RPS: float = 800.0

DB_SERVICE_S: float = 25.0 * MS  # 1/µ3
DB_CAPACITY_RPS: float = 960.0  # tại n_db = 24
N_DB_DEFAULT: int = 24

# Thời gian đáp ứng trung bình mỗi nhánh (service + RTT), spec §3.3: 12 / 22 / 27 ms
BRANCH_MEAN_S: dict[str, float] = {
    "auth": AUTH_SERVICE_S + NET_LINK_RTT_S,  # 12 ms
    "catalog": CATALOG_SERVICE_S + NET_LINK_RTT_S,  # 22 ms
    "db": DB_SERVICE_S + NET_LINK_RTT_S,  # 27 ms
}

# --------------------------------------------------------------------------------------
# Parameters — Cấu hình được (spec §1.2, §3.1, §3.2)
# --------------------------------------------------------------------------------------
T_TIMEOUT_S: float = 0.300  # SLA §1.2
ADMISSION_CAPACITY_K: int = 64  # n(t) < K
T_SIM_S: float = 200.0  # spec §1.5.2
T_WARMUP_S: float = 20.0  # spec §1.5.2
R_REPLICATIONS: int = 30  # R = 30 random seeds

LAMBDA_NOMINAL: float = 150.0  # λ0
LAMBDA_LEVELS: tuple[float, ...] = (50, 100, 150, 200, 250, 300, 350, 400, 450)

N_DB_LEVELS: tuple[int, ...] = (10, 16, 24)
P_PIN_LEVELS: tuple[float, ...] = (0.0, 0.05, 0.20)
P_UV_LEVELS: tuple[float, ...] = (0.0, 0.15, 0.30)

UV_POOL_CHANNELS: int = 4  # c_uv = 4
UV_POOL_SERVICE_S: float = 12.0 * MS
PINNING_HOLD_S: float = 38.84 * MS  # E[max 3 nhánh] — spec §3.2/§3.3

# --------------------------------------------------------------------------------------
# Tên định danh cố định — DEV.md §4
# --------------------------------------------------------------------------------------
RuntimeModelName = Literal[
    "blocking_baseline",
    "nodejs_event_loop",
    "bun_event_loop",
    "spring_loom_vt",
]
JoinMode = Literal["gather", "as_completed"]

# Tên LP cố định — dùng làm `Emission.destination`, driver tra bảng này
LP_SOURCE: str = "source"
LP_ADMISSION: str = "admission"
LP_NETWORK_LINK: str = "network_link"
LP_DISPATCHER: str = "dispatcher"

SERVICE_LP_NAMES: dict[str, str] = {
    "auth": "auth_cluster",
    "catalog": "catalog_service",
    "db": "db_pool",
}

RUNTIME_MODELS: tuple[str, ...] = (
    "blocking_baseline",
    "nodejs_event_loop",
    "bun_event_loop",
    "spring_loom_vt",
)
JOIN_MODES: tuple[str, ...] = ("gather", "as_completed")


# --------------------------------------------------------------------------------------
# Profile 4 runtime model
# --------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class UvPool:
    """libuv Worker Pool (Node.js / Bun): M/M/c_uv, dùng cho tác vụ đồng bộ bị offload."""

    channels: int = UV_POOL_CHANNELS
    service_s: float = UV_POOL_SERVICE_S
    offload_prob: float = 0.0  # p_uv


@dataclass(frozen=True, slots=True)
class RuntimeProfile:
    """Tham số một mô hình runtime (spec §1.1.1, §3.1, §3.3)."""

    name: RuntimeModelName
    label: str
    kind: Literal["blocking", "event_loop", "virtual_thread"]
    channels: int
    """Số kênh phục vụ CPU: c_os (blocking), c_loop = 1 (Node/Bun), c_carrier = 4 (Loom)."""

    cpu_total_s: float
    """Tổng chi phí CPU mỗi request: S_cpu (blocking, Loom) hoặc S_loop (Node, Bun)."""

    ingress_s: float
    """Pha 1 (ingress + fan-out). Node: 60% S_loop, Bun: 60% S_loop, Loom: 1.5 ms."""

    callback_s: float
    """Pha 2 (join callback). Node: 40% S_loop, Bun: 40% S_loop, Loom: 1.0 ms."""

    holds_slot_during_io: bool
    """True ⇒ kênh bị giữ suốt pha chờ I/O (Java Blocking, và Loom khi bị pinning)."""

    sequential_branches: bool
    """True ⇒ gọi tuần tự 3 nhánh (chỉ Java Blocking)."""

    carrier_hold_s: float = 0.0
    """Thời gian carrier bị giữ khi Thread Pinning (Loom). 0 nếu không áp dụng."""

    uv_pool: UvPool | None = None
    theoretical_capacity_rps: float = 0.0

    def as_dict(self) -> dict:
        """Biểu diễn phẳng để ghi log/CSV tham số."""
        return {
            "name": self.name,
            "kind": self.kind,
            "channels": self.channels,
            "cpu_total_ms": self.cpu_total_s * 1000.0,
            "ingress_ms": self.ingress_s * 1000.0,
            "callback_ms": self.callback_s * 1000.0,
            "holds_slot_during_io": self.holds_slot_during_io,
            "sequential_branches": self.sequential_branches,
            "carrier_hold_ms": self.carrier_hold_s * 1000.0,
            "uv_channels": self.uv_pool.channels if self.uv_pool else 0,
            "uv_service_ms": (self.uv_pool.service_s * 1000.0) if self.uv_pool else 0.0,
            "uv_offload_prob": self.uv_pool.offload_prob if self.uv_pool else 0.0,
            "theoretical_capacity_rps": self.theoretical_capacity_rps,
        }


# Java Blocking: 1 OS thread giữ suốt 2.5 + 12 + 22 + 27 = 63.5 ms (§3.3)
_BLOCKING_HOLD_S: float = 2.5 * MS + sum(BRANCH_MEAN_S.values())  # 0.0635 s

PROFILES: dict[str, RuntimeProfile] = {
    "blocking_baseline": RuntimeProfile(
        name="blocking_baseline",
        label="Java Blocking (Thread-per-Request)",
        kind="blocking",
        channels=12,
        cpu_total_s=2.5 * MS,
        ingress_s=2.5 * MS,  # không chia 2 pha: CPU trước rồi gọi tuần tự 3 nhánh
        callback_s=0.0,
        holds_slot_during_io=True,
        sequential_branches=True,
        theoretical_capacity_rps=12 * (1.0 / _BLOCKING_HOLD_S),  # ≈ 188.98 req/s
    ),
    "nodejs_event_loop": RuntimeProfile(
        name="nodejs_event_loop",
        label="Node.js (V8 + libuv)",
        kind="event_loop",
        channels=1,
        cpu_total_s=3.2 * MS,
        ingress_s=0.60 * 3.2 * MS,  # 1.92 ms
        callback_s=0.40 * 3.2 * MS,  # 1.28 ms
        holds_slot_during_io=False,
        sequential_branches=False,
        uv_pool=UvPool(channels=4, service_s=12.0 * MS, offload_prob=0.0),
        theoretical_capacity_rps=1.0 / (3.2 * MS),  # 312.5 req/s
    ),
    "bun_event_loop": RuntimeProfile(
        name="bun_event_loop",
        label="Bun (JavaScriptCore + Zig)",
        kind="event_loop",
        channels=1,
        cpu_total_s=2.0 * MS,
        ingress_s=0.60 * 2.0 * MS,  # 1.20 ms
        callback_s=0.40 * 2.0 * MS,  # 0.80 ms
        holds_slot_during_io=False,
        sequential_branches=False,
        uv_pool=UvPool(channels=4, service_s=12.0 * MS, offload_prob=0.0),
        theoretical_capacity_rps=1.0 / (2.0 * MS),  # 500.0 req/s
    ),
    "spring_loom_vt": RuntimeProfile(
        name="spring_loom_vt",
        label="Java Spring Loom (Virtual Threads M:N)",
        kind="virtual_thread",
        channels=4,
        cpu_total_s=2.5 * MS,
        ingress_s=1.5 * MS,
        callback_s=1.0 * MS,
        holds_slot_during_io=False,  # unmount, trừ khi bị pinning
        sequential_branches=False,
        carrier_hold_s=PINNING_HOLD_S,
        theoretical_capacity_rps=4 * (1.0 / (2.5 * MS)),  # 1600 req/s
    ),
}

# Đối chiếu giải tích khi p_pin = 20% (spec §3.3)
LOOM_CAPACITY_PPIN20_RPS: float = 389.59


def get_profile(name: str, *, p_pin: float = 0.0, p_uv: float = 0.0) -> RuntimeProfile:
    """Lấy profile runtime và áp các tham số study factor (p_pin, p_uv)."""
    try:
        base = PROFILES[name]
    except KeyError as exc:  # pragma: no cover - lỗi lập trình
        raise ValueError(
            f"runtime_model không hợp lệ: {name!r}; phải là một trong {RUNTIME_MODELS}"
        ) from exc
    if p_pin < 0.0 or p_pin > 1.0:
        raise ValueError(f"p_pin phải trong [0, 1], nhận {p_pin}")
    if p_uv < 0.0 or p_uv > 1.0:
        raise ValueError(f"p_uv phải trong [0, 1], nhận {p_uv}")
    if base.uv_pool is None:
        uv = None
    else:
        uv = UvPool(
            channels=base.uv_pool.channels,
            service_s=base.uv_pool.service_s,
            offload_prob=p_uv,
        )
    return RuntimeProfile(
        name=base.name,
        label=base.label,
        kind=base.kind,
        channels=base.channels,
        cpu_total_s=base.cpu_total_s,
        ingress_s=base.ingress_s,
        callback_s=base.callback_s,
        holds_slot_during_io=base.holds_slot_during_io,
        sequential_branches=base.sequential_branches,
        carrier_hold_s=base.carrier_hold_s if p_pin > 0.0 else 0.0,
        uv_pool=uv,
        theoretical_capacity_rps=(
            LOOM_CAPACITY_PPIN20_RPS
            if base.name == "spring_loom_vt" and abs(p_pin - 0.20) < 1e-9
            else base.theoretical_capacity_rps
        ),
    )


# --------------------------------------------------------------------------------------
# Cấu hình một lần chạy (một điểm của không gian study factor)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ScenarioConfig:
    """Một kịch bản = (runtime_model, join_mode, λ, n_db, p_pin, p_uv, seed)."""

    runtime_model: RuntimeModelName
    join_mode: JoinMode
    lam: float
    seed: int
    n_db: int = N_DB_DEFAULT
    p_pin: float = 0.0
    p_uv: float = 0.0
    t_sim: float = T_SIM_S
    t_warmup: float = T_WARMUP_S
    capacity_k: int = ADMISSION_CAPACITY_K
    t_timeout: float = T_TIMEOUT_S

    def __post_init__(self) -> None:
        if self.runtime_model not in RUNTIME_MODELS:
            raise ValueError(f"runtime_model không hợp lệ: {self.runtime_model!r}")
        if self.join_mode not in JOIN_MODES:
            raise ValueError(f"join_mode không hợp lệ: {self.join_mode!r}")
        if self.lam <= 0.0:
            raise ValueError(f"λ phải > 0, nhận {self.lam}")
        if self.n_db not in N_DB_LEVELS:
            raise ValueError(f"n_db phải thuộc {N_DB_LEVELS}, nhận {self.n_db}")
        if not 0.0 <= self.p_pin <= 0.20:
            raise ValueError(f"p_pin phải trong [0, 0.20] (spec §3.2), nhận {self.p_pin}")
        if not 0.0 <= self.p_uv <= 0.30:
            raise ValueError(f"p_uv phải trong [0, 0.30] (spec §3.2), nhận {self.p_uv}")
        if not 0.0 <= self.t_warmup < self.t_sim:
            raise ValueError(
                f"cần 0 ≤ t_warmup < t_sim, nhận t_warmup={self.t_warmup}, t_sim={self.t_sim}"
            )
        if self.capacity_k < 1:
            raise ValueError(f"capacity_k phải ≥ 1, nhận {self.capacity_k}")
        if self.t_timeout <= 0.0:
            raise ValueError(f"t_timeout phải > 0, nhận {self.t_timeout}")
        if self.seed < 0:
            raise ValueError(f"seed phải ≥ 0, nhận {self.seed}")

    @property
    def profile(self) -> RuntimeProfile:
        return get_profile(self.runtime_model, p_pin=self.p_pin, p_uv=self.p_uv)

    @property
    def steady_state_s(self) -> float:
        """Độ dài cửa sổ đo sau khi bỏ warm-up (spec §2.1.1)."""
        return self.t_sim - self.t_warmup

    def as_dict(self) -> dict:
        return {
            "runtime_model": self.runtime_model,
            "join_mode": self.join_mode,
            "lam": self.lam,
            "seed": self.seed,
            "n_db": self.n_db,
            "p_pin": self.p_pin,
            "p_uv": self.p_uv,
            "t_sim": self.t_sim,
            "t_warmup": self.t_warmup,
            "capacity_k": self.capacity_k,
            "t_timeout": self.t_timeout,
        }
