"""Engine trạm hàng đợi — M/M/c, M/M/c/K, M/M/n/n (Erlang loss) trên thời gian ảo.

Thiết kế: phần lõi **thuần trạng thái**, không import kernel và không tự sinh số ngẫu
nhiên. LP bọc ngoài sẽ:
    1. sample `service_s` từ `random.Random` của mình,
    2. gọi `on_arrival(t, service_s)` / `on_departure(t, service_s)`,
    3. đặt `Event` cho mốc `finish_s` trả về.

Nhờ vậy engine test được độc lập với `bkitevsim` (đang bị chặn — project.md §12-K4)
và dùng lại được cho 6 mô hình hàng đợi đơn của dev khác (nếu nhóm chốt dùng chung).

Ánh xạ (spec §1.4 Bảng 1):
    M/M/1     : channels=1, capacity=None, loss=False
    M/M/1/K   : channels=1, capacity=K
    M/M/∞     : channels=∞  → dùng `LossStationCore`? Không: dùng channels lớn hoặc
                cài riêng `InfiniteServerCore` (không xếp hàng, không từ chối)
    M/M/n/n   : channels=n, loss=True
    M/M/c     : channels=c, capacity=None
    M/M/c/K   : channels=c, capacity=K
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Final

REASON_CAPACITY: Final[str] = "capacity"  # buffer đầy (M/M/c/K)
REASON_NO_CONNECTION: Final[str] = "no_connection"  # Erlang loss (M/M/n/n)


@dataclass(frozen=True, slots=True)
class Admission:
    """Kết quả một lần khách đến trạm."""

    accepted: bool
    start_s: float | None = None
    finish_s: float | None = None
    queued: bool = False
    reason: str | None = None

    @property
    def rejected(self) -> bool:
        return not self.accepted


class StationCore:
    """Trạm M/M/c hoặc M/M/c/K hoặc M/M/n/n (loss).

    Tham số
    -------
    channels : số kênh phục vụ song song (c).
    capacity : tổng sức chứa K (kể cả đang phục vụ). `None` ⇒ K = ∞.
    loss     : True ⇒ M/M/n/n, không có chỗ chờ, đầy là từ chối ngay.
    """

    __slots__ = (
        "name",
        "channels",
        "capacity",
        "loss",
        "_busy",
        "_queue",
        "_last_t",
        "_area_in_system",
        "_area_busy",
        "_accepted",
        "_rejected_capacity",
        "_rejected_loss",
        "_served",
        "_max_in_system",
    )

    def __init__(
        self,
        name: str,
        channels: int,
        *,
        capacity: int | None = None,
        loss: bool = False,
    ) -> None:
        if channels < 1:
            raise ValueError("channels phải ≥ 1")
        if capacity is not None and capacity < channels:
            raise ValueError(f"capacity K={capacity} phải ≥ channels c={channels}")
        if loss and capacity is not None and capacity != channels:
            raise ValueError("M/M/n/n: capacity phải bằng channels (không có chỗ chờ)")

        self.name = name
        self.channels = channels
        self.capacity = capacity
        self.loss = loss

        self._busy: int = 0
        self._queue: deque[float] = deque()  # thời điểm đến của khách đang chờ
        self._last_t: float = 0.0
        self._area_in_system: float = 0.0
        self._area_busy: float = 0.0
        self._accepted: int = 0
        self._rejected_capacity: int = 0
        self._rejected_loss: int = 0
        self._served: int = 0
        self._max_in_system: int = 0

    # ------------------------------------------------------------------ nội bộ
    @property
    def in_system(self) -> int:
        return self._busy + len(self._queue)

    @property
    def waiting(self) -> int:
        return len(self._queue)

    @property
    def busy(self) -> int:
        """Số kênh đang phục vụ."""
        return self._busy

    @property
    def accepted(self) -> int:
        """Số khách đã được nhận (kể cả đang chờ)."""
        return self._accepted

    @property
    def served(self) -> int:
        return self._served

    @property
    def rejected_total(self) -> int:
        return self._rejected_capacity + self._rejected_loss

    def _max_waiting(self) -> int:
        if self.loss:
            return 0
        if self.capacity is None:
            return 2**31
        return self.capacity - self.channels

    def _observe(self, t: float) -> None:
        if t < self._last_t:
            raise ValueError(
                f"{self.name}: thời gian lùi ({t} < {self._last_t}) — vi phạm monotonicity"
            )
        dt = t - self._last_t
        self._area_in_system += self.in_system * dt
        self._area_busy += self._busy * dt
        self._last_t = t
        self._max_in_system = max(self._max_in_system, self.in_system)

    # ------------------------------------------------------------------ sự kiện
    def on_arrival(self, t: float, service_s: float) -> Admission:
        """Khách đến tại `t`, thời gian phục vụ đã sample là `service_s`."""
        if service_s < 0.0:
            raise ValueError("service_s phải ≥ 0")
        self._observe(t)

        if self._busy < self.channels:
            self._busy += 1
            self._accepted += 1
            self._max_in_system = max(self._max_in_system, self.in_system)
            return Admission(
                accepted=True,
                start_s=t,
                finish_s=t + service_s,
                queued=False,
            )

        if self.loss:
            self._rejected_loss += 1
            return Admission(accepted=False, reason=REASON_NO_CONNECTION)

        if len(self._queue) >= self._max_waiting():
            self._rejected_capacity += 1
            return Admission(accepted=False, reason=REASON_CAPACITY)

        self._queue.append(t)
        self._accepted += 1
        self._max_in_system = max(self._max_in_system, self.in_system)
        return Admission(accepted=True, queued=True)

    def on_departure(self, t: float, next_service_s: float = 0.0) -> Admission | None:
        """Một kênh vừa xong `t`. Nếu có khách chờ, khách đầu FIFO được phục vụ ngay."""
        self._observe(t)
        if self._busy <= 0:
            raise ValueError(f"{self.name}: departure khi không có kênh nào bận")
        self._busy -= 1
        self._served += 1

        if not self._queue:
            return None

        if next_service_s < 0.0:
            raise ValueError("next_service_s phải ≥ 0")
        self._queue.popleft()
        self._busy += 1
        return Admission(
            accepted=True,
            start_s=t,
            finish_s=t + next_service_s,
            queued=False,
        )

    # ------------------------------------------------------------------ thống kê
    def snapshot(self, t_end: float) -> dict[str, float | int]:
        """Chỉ số trạng thái dừng của trạm trong [0, t_end] (giây).

        `mean_in_system` và `mean_busy_servers` là trung bình theo thời gian (time-weighted),
        phục vụ Verification bằng Little's Law (spec §1.5.2).
        """
        if t_end <= 0.0:
            raise ValueError("t_end phải > 0")
        self._observe(t_end)
        duration = t_end
        return {
            "name": self.name,
            "channels": self.channels,
            "capacity": self.capacity if self.capacity is not None else -1,
            "accepted": self._accepted,
            "served": self._served,
            "rejected_capacity": self._rejected_capacity,
            "rejected_loss": self._rejected_loss,
            "rejected_total": self._rejected_capacity + self._rejected_loss,
            "mean_in_system": self._area_in_system / duration,
            "mean_busy_servers": self._area_busy / duration,
            "utilization": self._area_busy / (self.channels * duration),
            "max_in_system": self._max_in_system,
            "in_system_at_end": self.in_system,
        }


class InfiniteServerCore:
    """M/M/∞ — mỗi khách có kênh riêng, không bao giờ xếp hàng, không từ chối.

    Dùng cho Internal Network Link (`1/µ_net = 2 ms`, `Wq = 0`, spec §1.2).
    """

    __slots__ = ("name", "_accepted", "_last_t", "_area_in_system", "_served")

    def __init__(self, name: str) -> None:
        self.name = name
        self._accepted = 0
        self._served = 0
        self._last_t = 0.0
        self._area_in_system = 0.0

    @property
    def in_system(self) -> int:
        return self._accepted - self._served

    def _observe(self, t: float) -> None:
        if t < self._last_t:
            raise ValueError(f"{self.name}: thời gian lùi ({t} < {self._last_t})")
        self._area_in_system += self.in_system * (t - self._last_t)
        self._last_t = t

    def on_arrival(self, t: float, service_s: float) -> Admission:
        if service_s < 0.0:
            raise ValueError("service_s phải ≥ 0")
        self._observe(t)
        self._accepted += 1
        return Admission(accepted=True, start_s=t, finish_s=t + service_s, queued=False)

    def on_departure(self, t: float) -> Admission:
        self._observe(t)
        if self._served >= self._accepted:
            raise ValueError(f"{self.name}: departure khi trạm rỗng")
        self._served += 1
        return Admission(accepted=True)

    def snapshot(self, t_end: float) -> dict[str, float | int]:
        if t_end <= 0.0:
            raise ValueError("t_end phải > 0")
        self._observe(t_end)
        return {
            "name": self.name,
            "accepted": self._accepted,
            "served": self._served,
            "rejected_total": 0,
            "mean_in_system": self._area_in_system / t_end,
            "in_system_at_end": self.in_system,
        }
