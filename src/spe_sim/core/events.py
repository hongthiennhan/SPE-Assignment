"""Định nghĩa event và payload — hợp đồng giao tiếp giữa các LP.

Ngoài 6 payload liên LP còn có `Emission` (event sắp đặt) và `JobDone` (timer nội bộ
của LP dịch vụ) — xem docstring từng class. Tầng này vẫn thuần dữ liệu, không import
`bkitevsim`.

Tên `EVENT_TYPES` lấy nguyên văn từ `docs/team/Thống nhất DEV.md` §4:
    "REQ_ARRIVAL", "SUB_REQ", "SUB_RESP", "TIMEOUT_ALARM"

Module này KHÔNG import `bkitevsim`: payload chỉ là dữ liệu thuần, còn việc bọc
chúng thành `Event(timestamp=...)` của kernel do `spe_sim.core.kernel` đảm nhiệm.
Nhờ vậy tầng logic test được mà không cần kernel.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final

from spe_sim.core.config import JoinMode, RuntimeModelName

EVENT_TYPES: Final[tuple[str, ...]] = (
    "REQ_ARRIVAL",
    "SUB_REQ",
    "SUB_RESP",
    "TIMEOUT_ALARM",
)


class Outcome(str, Enum):
    """Ba kết cục loại trừ lẫn nhau (spec §1.3)."""

    COMPLETED = "completed"  # HTTP 200 — Done correctly
    TIMEOUT = "timeout"  # HTTP 504 — r > T_timeout
    DB_DROP = "db_drop"  # HTTP 504 — DB full ở All-Join 3/3
    DROPPED = "dropped"  # HTTP 503 — n(t) = K tại Admission Control


class Branch(str, Enum):
    """Định danh 3 nhánh downstream (spec §1.2)."""

    AUTH = "auth"
    CATALOG = "catalog"
    DB = "db"


BRANCHES: Final[tuple[Branch, ...]] = (Branch.AUTH, Branch.CATALOG, Branch.DB)


# --------------------------------------------------------------------------------------
# Payload
# --------------------------------------------------------------------------------------
@dataclass(slots=True)
class ReqArrival:
    """Client Source → Admission Control (λ, Poisson)."""

    request_id: int
    t_arrive: float


@dataclass(slots=True)
class Admitted:
    """Admission Control → Runtime Dispatcher (đã qua cổng n(t) < K)."""

    request_id: int
    t_arrive: float


@dataclass(slots=True)
class SubReq:
    """Dispatcher → Network Link → downstream service (1 lượt truy vấn nhánh)."""

    request_id: int
    branch: Branch
    attempt: int = 0
    t_sent: float = 0.0


@dataclass(slots=True)
class SubResp:
    """Downstream service → Network Link → Dispatcher."""

    request_id: int
    branch: Branch
    t_sent: float
    t_service_start: float
    t_service_end: float
    ok: bool = True
    reason: str | None = None  # "no_connection" khi DB pool đầy


@dataclass(slots=True)
class TimeoutAlarm:
    """SLA timer của Dispatcher: đặt tại t_arrive + T_timeout."""

    request_id: int
    t_deadline: float


@dataclass(slots=True)
class JobDone:
    """Báo giờ của **chính LP dịch vụ** (không phải event liên LP).

    Khi một job bắt đầu phục vụ, LP đặt cho mình một `JobDone` tại `t_start + service_s`.
    Tới hạn đó LP mới gọi `on_departure()` để nhả kênh và kéo job kế tiếp trong hàng đợi
    FIFO. Không có event này thì kênh không bao giờ được nhả.

    Tên không nằm trong `EVENT_TYPES` (DEV.md §4) vì đây là timer nội bộ của LP;
    lý do tách khỏi `SUB_RESP`: `SUB_RESP` là thông điệp *gửi đi* (đã rời LP),
    còn `JobDone` là mốc thời gian *quay về* LP để cập nhật trạng thái.
    """

    request_id: int
    branch: Branch
    t_sent: float
    t_start: float


@dataclass(slots=True)
class OutcomeNotice:
    """Dispatcher → Sink: chốt kết cục của một request."""

    request_id: int
    outcome: Outcome
    t_arrive: float
    t_dispatch: float
    t_complete: float
    n_in_system: int
    runtime_model: RuntimeModelName
    join_mode: JoinMode
    reason: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


Payload = ReqArrival | Admitted | SubReq | SubResp | JobDone | TimeoutAlarm | OutcomeNotice


@dataclass(frozen=True, slots=True)
class Emission:
    """Một event *sắp được đặt*: (thời điểm, LP đích, payload).

    Hợp đồng kernel-free: mỗi LP chỉ **trả về** danh sách `Emission`, không tự gọi
    `LadderQueue`/`schedule_event`. Driver (khi có `bkitevsim`) chỉ cần đổi mỗi
    `Emission` thành `Event(timestamp=..., source=lp, destination=..., payload=...)`
    và push vào queue. Nhờ vậy toàn bộ tầng LP test được bằng một vòng lặp `heapq`
    nhỏ, không cần kernel (project.md §12-K4).
    """

    timestamp: float
    destination: str
    payload: Payload

    def __post_init__(self) -> None:
        if self.timestamp < 0.0:
            raise ValueError(f"timestamp âm: {self.timestamp}")
        if not self.destination:
            raise ValueError("Emission thiếu LP đích")
