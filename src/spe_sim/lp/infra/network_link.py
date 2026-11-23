"""`NetworkLinkLP` — Internal Network Link, M/M/∞, khứ hồi 2.0 ms (spec §1.2).

Mô hình: **hai chặng một chiều độc lập**, mỗi chặng ~ Exp(1/µ) với `1/µ = 1.0 ms`,
nên khứ hồi trung bình = 2.0 ms đúng spec §3.3 (12/22/27 ms = service + 2 ms).

Vì là M/M/∞ nên `Wq = 0` **theo cấu trúc**: không có kênh hữu hạn, không có hàng đợi,
không cần `StationCore` — LP chỉ là một độ trễ ngẫu nhiên. Nhờ vậy không cần timer
nội bộ để nhả kênh (khác `service_lp.py`).

Luồng:
    SUB_REQ  (dispatcher → link) → sau 1 ms → service tương ứng nhánh
    SUB_RESP (service → link)    → sau 1 ms → LP_DISPATCHER

GIẢ ĐỊNH (ghi trong project.md §9, chưa được spec chốt): spec chỉ nêu "khứ hồi 2.0 ms"
mà không nói phân phối của từng chiều; tôi dùng hai chặng Exp(1.0 ms) độc lập.
Nếu nhóm chốt "một biến Exp(2.0 ms) cho cả khứ hồi" thì chỉ cần đổi cách sample.
"""

from __future__ import annotations

import random
from typing import Final

from spe_sim.core.config import (
    LP_DISPATCHER,
    NET_LINK_ONE_WAY_S,
    SERVICE_LP_NAMES,
)
from spe_sim.core.events import Emission, SubReq, SubResp
from spe_sim.core.rng import sample_exponential

__all__ = ["NetworkLinkLP"]


class NetworkLinkLP:
    """Độ trễ mạng hai chiều, mỗi chiều Exp(`mean_one_way_s`), không xếp hàng."""

    def __init__(
        self,
        rng: random.Random,
        *,
        mean_one_way_s: float = NET_LINK_ONE_WAY_S,
        name: str = "network_link",
    ) -> None:
        if mean_one_way_s <= 0.0:
            raise ValueError(f"mean_one_way_s phải > 0, nhận {mean_one_way_s}")
        self.name: Final[str] = name
        self._rng = rng
        self._mean = mean_one_way_s
        self.sent = 0
        self.returned = 0

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, SubReq):
            self.sent += 1
            delay = sample_exponential(self._rng, self._mean)
            return [
                Emission(
                    timestamp=now + delay,
                    destination=SERVICE_LP_NAMES[payload.branch.value],
                    payload=payload,
                )
            ]
        if isinstance(payload, SubResp):
            self.returned += 1
            delay = sample_exponential(self._rng, self._mean)
            return [
                Emission(
                    timestamp=now + delay,
                    destination=LP_DISPATCHER,
                    payload=payload,
                )
            ]
        raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")

    @property
    def mean_round_trip_s(self) -> float:
        """Khứ hồi trung bình (giây) — dùng cho Verify/đối chiếu §3.3."""
        return 2.0 * self._mean
