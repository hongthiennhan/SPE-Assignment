"""`AuthClusterLP` — dịch vụ Auth: M/M/8, `1/µ = 10 ms` (spec §1.2 Bảng 1, §3.3).

Tổng thời gian nhánh Auth đo được (kể cả 2 ms mạng) phải ≈ 12 ms — đối chiếu
`BRANCH_MEAN_S["auth"]` trong `core/config.py`.
"""

from __future__ import annotations

import random

from spe_sim.core.config import AUTH_CHANNELS, AUTH_SERVICE_S
from spe_sim.lp.infra.service_lp import ServiceLP

__all__ = ["AuthClusterLP"]


class AuthClusterLP(ServiceLP):
    """M/M/8: 8 kênh, hàng đợi không giới hạn (chỉ K ở admission mới chặn)."""

    def __init__(self, rng: random.Random, *, channels: int = AUTH_CHANNELS) -> None:
        super().__init__(
            name="auth_cluster",
            channels=channels,
            mean_service_s=AUTH_SERVICE_S,
            rng=rng,
        )
