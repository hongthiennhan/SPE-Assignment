"""Đo chỉ số trạm trên cửa sổ `[T_warmup, T_sim]` từ vết thời gian của từng khách.

Vì sao không dùng `StationCore.snapshot()`: snapshot tích lũy từ `t = 0`, không loại được
giai đoạn khởi động. Ở đây mỗi khách để lại một bộ mốc `(t_arrive, t_start, t_end)` nên
mọi đại lượng trung bình theo thời gian tính **chính xác** bằng tổng phần giao với cửa sổ:

    L     = Σ |[t_arrive, t_end] ∩ W| / |W|          (số khách trung bình trong hệ thống)
    busy  = Σ |[t_start,  t_end] ∩ W| / |W|          (số kênh bận trung bình)
    Lq    = L − busy,  U = busy / c
    W, Wq = trung bình (t_end − t_arrive), (t_start − t_arrive) của khách **đến** trong W
    X     = số khách đến trong W và được phục vụ xong / |W|
    P_block = số khách đến trong W bị từ chối / số khách đến trong W

Đơn vị SI (giây, req/s) — chỉ đổi ms khi in bảng.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

__all__ = ["Visit", "WindowStats"]


@dataclass(slots=True)
class Visit:
    """Một lượt khách qua trạm. `t_start/t_end = None` nghĩa là bị từ chối."""

    t_arrive: float
    t_start: float | None = None
    t_end: float | None = None

    @property
    def blocked(self) -> bool:
        return self.t_end is None


def _overlap(a: float, b: float, lo: float, hi: float) -> float:
    return max(0.0, min(b, hi) - max(a, lo))


@dataclass
class WindowStats:
    """Gom `Visit` rồi xuất bảng chỉ số cùng khoá với `spe_sim.core.analytics`."""

    t_warmup: float
    t_end: float
    channels: float  # math.inf cho M/M/∞
    visits: list[Visit] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not 0.0 <= self.t_warmup < self.t_end:
            raise ValueError(f"cần 0 ≤ t_warmup < t_end, nhận {self.t_warmup}/{self.t_end}")

    def add(self, visit: Visit) -> None:
        if visit.t_end is not None:
            if visit.t_start is None or not (visit.t_arrive <= visit.t_start <= visit.t_end):
                raise ValueError(f"vi phạm nhân quả: {visit}")
        self.visits.append(visit)

    def summary(self) -> dict[str, float | None]:
        lo, hi = self.t_warmup, self.t_end
        span = hi - lo
        area_n = area_busy = 0.0
        arrivals = blocked = served = 0
        resp_sum = wait_sum = 0.0
        for v in self.visits:
            if v.t_end is not None:
                area_n += _overlap(v.t_arrive, v.t_end, lo, hi)
                area_busy += _overlap(v.t_start, v.t_end, lo, hi)  # type: ignore[arg-type]
            if not lo <= v.t_arrive <= hi:
                continue
            arrivals += 1
            if v.blocked:
                blocked += 1
                continue
            served += 1
            resp_sum += v.t_end - v.t_arrive  # type: ignore[operator]
            wait_sum += v.t_start - v.t_arrive  # type: ignore[operator]
        mean_n = area_n / span
        mean_busy = area_busy / span
        return {
            "L": mean_n,
            "Lq": mean_n - mean_busy,
            "W": resp_sum / served if served else None,
            "Wq": wait_sum / served if served else None,
            "X": served / span,
            "P_block": blocked / arrivals if arrivals else 0.0,
            "U": (mean_busy / self.channels) if math.isfinite(self.channels) else None,
            "arrivals": float(arrivals),
        }
