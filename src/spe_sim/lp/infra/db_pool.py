"""`DbPoolLP` — connection pool DB: **M/M/n/n** (Erlang loss), `1/µ = 25 ms`.

Khác hai dịch vụ còn lại: `n_db` kênh và **không có chỗ chờ** (spec §1.2 Bảng 1).
Đầy kết nối ⇒ từ chối ngay (`SUB_RESP.ok = False`, `reason = "no_connection"`);
dispatcher biến nó thành `DB_DROP` (504) ở All-Join 3/3 — xem spec §1.3.

Tổng thời gian nhánh DB ≈ 27 ms khi thành công — đối chiếu `BRANCH_MEAN_S["db"]`.
"""

from __future__ import annotations

import random

from spe_sim.core.config import DB_SERVICE_S, N_DB_DEFAULT
from spe_sim.lp.infra.service_lp import ServiceLP

__all__ = ["DbPoolLP"]


class DbPoolLP(ServiceLP):
    """M/M/n/n: `n_db` kết nối, `capacity = channels`, không xếp hàng."""

    def __init__(self, rng: random.Random, *, n_db: int = N_DB_DEFAULT) -> None:
        super().__init__(
            name="db_pool",
            channels=n_db,
            mean_service_s=DB_SERVICE_S,
            rng=rng,
            capacity=n_db,
            loss=True,
        )

    @property
    def n_db(self) -> int:
        return self.core.channels

    @property
    def blocking_probability(self) -> float:
        """Tỷ lệ từ chối đo được — so với Erlang-B lý thuyết khi verify."""
        total = self.core.accepted + self.core.rejected_total
        if total == 0:
            return 0.0
        return self.core.rejected_total / total
