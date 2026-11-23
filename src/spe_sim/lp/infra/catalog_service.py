"""`CatalogServiceLP` — dịch vụ Catalog: M/M/16, `1/µ = 20 ms` (spec §1.2, §3.3).

Tổng thời gian nhánh Catalog ≈ 22 ms — đối chiếu `BRANCH_MEAN_S["catalog"]`.
"""

from __future__ import annotations

import random

from spe_sim.core.config import CATALOG_CHANNELS, CATALOG_SERVICE_S
from spe_sim.lp.infra.service_lp import ServiceLP

__all__ = ["CatalogServiceLP"]


class CatalogServiceLP(ServiceLP):
    """M/M/16: 16 kênh, hàng đợi không giới hạn."""

    def __init__(self, rng: random.Random, *, channels: int = CATALOG_CHANNELS) -> None:
        super().__init__(
            name="catalog_service",
            channels=channels,
            mean_service_s=CATALOG_SERVICE_S,
            rng=rng,
        )
