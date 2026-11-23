"""`SourceLP` — nguồn khách hàng Poisson, tắt ở `T_sim` (spec §1.2, §1.5.2).

Không import `bkitevsim`: LP chỉ phát ra `Emission`, để driver đặt event.
Nguồn là LP duy nhất **tự chạy theo đồng hồ** (không có event vào), nên tách
thành `peek()` + `take()`:

    while (t := source.peek()) is not None:
        kernel.push(source.take())

`peek()` trả `None` khi mốc đến kế tiếp đã vượt `T_sim` ⇒ driver dừng sinh,
nhưng vẫn chạy tiếp các event đang có trong queue tới khi rỗng.
"""

from __future__ import annotations

import random
from typing import Final

from spe_sim.core.config import LP_ADMISSION
from spe_sim.core.events import Emission, ReqArrival
from spe_sim.core.rng import sample_interarrival

__all__ = ["SourceLP"]


class SourceLP:
    """Sinh chuỗi đến Poisson với cường độ λ (req/s), id tăng dần từ 0."""

    def __init__(
        self,
        lam: float,
        rng: random.Random,
        t_sim: float,
        *,
        name: str = "source",
    ) -> None:
        if lam <= 0.0:
            raise ValueError(f"λ phải > 0, nhận {lam}")
        if t_sim <= 0.0:
            raise ValueError(f"t_sim phải > 0, nhận {t_sim}")
        self.name: Final[str] = name
        self.lam = lam
        self.t_sim = t_sim
        self._rng = rng
        self._next_id = 0
        self.emitted = 0
        # Khách đầu tiên: hệ thống rỗng tại t = 0 ⇒ khoảng đến đầu tiên ~ Exp(λ).
        self._t_next: float | None = sample_interarrival(rng, lam)

    # ------------------------------------------------------------------ API
    def peek(self) -> float | None:
        """Mốc đến kế tiếp, hoặc `None` nếu đã vượt `T_sim`."""
        t = self._t_next
        if t is None or t > self.t_sim:
            return None
        return t

    def take(self) -> Emission:
        """Lấy khách kế tiếp (đã kiểm tra `peek()`), sinh `REQ_ARRIVAL`."""
        t = self.peek()
        if t is None:
            raise LookupError(f"{self.name}: không còn khách trong [0, t_sim={self.t_sim}]")

        request_id = self._next_id
        self._next_id += 1
        self.emitted += 1
        self._t_next = t + sample_interarrival(self._rng, self.lam)

        return Emission(
            timestamp=t,
            destination=LP_ADMISSION,
            payload=ReqArrival(request_id=request_id, t_arrive=t),
        )

    def reset(self) -> None:
        """Đưa nguồn về trạng thái đầu (dùng cho test tái lập)."""
        self._next_id = 0
        self.emitted = 0
        self._t_next = sample_interarrival(self._rng, self.lam)

    def __repr__(self) -> str:  # pragma: no cover - chỉ để debug
        return f"SourceLP(name={self.name!r}, lam={self.lam}, emitted={self.emitted})"
