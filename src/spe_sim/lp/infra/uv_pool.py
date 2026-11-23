"""`UvWorkerPoolLP` — libuv Worker Pool: M/M/c_uv, `1/µ = 12 ms`, `c_uv = 4`.

Node.js/Bun đẩy tác vụ đồng bộ nặng sang pool này với xác suất `p_uv` (spec §1.1.1, §3.2).
LP thuần, dùng lại `StationCore` cho hàng đợi FIFO + kênh, và timer nội bộ `UvDone`
cùng kiểu `ServiceLP`: nhận job → giữ kênh → tới hạn nhả kênh và kéo job kế tiếp.

Luồng:
    UvTask (dispatcher → pool)  → [nhận ngay] → UvDone(pool) tại `now + s`
                                → [xếp hàng]  → không phát gì
    UvDone(pool)                → UvDone(dispatcher) + (nếu còn chờ) start job kế tiếp

GIẢ ĐỊNH (§9.6): offload thay cho pha ingress trên event loop, nên r của request bị
offload tăng thêm ~12 ms nhưng loop được nhả trong lúc chờ.
"""

from __future__ import annotations

import random
from collections import deque
from typing import Final

from spe_sim.core.config import (
    LP_DISPATCHER,
    UV_POOL_CHANNELS,
    UV_POOL_SERVICE_S,
)
from spe_sim.core.events import Emission, UvDone, UvTask
from spe_sim.core.rng import sample_exponential
from spe_sim.lp.infra.station import StationCore

__all__ = ["UvWorkerPoolLP"]


class UvWorkerPoolLP:
    """M/M/4, không giới hạn hàng đợi, phục vụ job offload từ dispatcher."""

    name: Final[str] = "uv_pool"

    def __init__(
        self,
        rng: random.Random,
        *,
        channels: int = UV_POOL_CHANNELS,
        service_s: float = UV_POOL_SERVICE_S,
        name: str = "uv_pool",
    ) -> None:
        if service_s <= 0.0:
            raise ValueError(f"service_s phải > 0, nhận {service_s}")
        self.name = name  # type: ignore[misc]
        self._rng = rng
        self._mean = service_s
        self.core = StationCore(name, channels)
        self._pending: deque[UvTask] = deque()
        self._active: set[int] = set()

    # ------------------------------------------------------------------ sự kiện
    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, UvTask):
            return self._on_task(payload, now)
        if isinstance(payload, UvDone):
            return self._on_done(payload, now)
        raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")

    def _on_task(self, task: UvTask, now: float) -> list[Emission]:
        if task.request_id in self._active:
            raise ValueError(f"{self.name}: job {task.request_id} đã ở trong pool")
        admission = self.core.on_arrival(now, sample_exponential(self._rng, self._mean))
        if admission.rejected:  # pragma: no cover - pool không giới hạn sức chứa
            raise RuntimeError(f"{self.name}: pool từ chối job {task.request_id}")
        if admission.queued:
            self._pending.append(task)
            return []
        self._active.add(task.request_id)
        return [
            Emission(
                timestamp=admission.finish_s if admission.finish_s is not None else now,
                destination=self.name,
                payload=UvDone(task.request_id, task.t_submit, now),
            )
        ]

    def _on_done(self, done: UvDone, now: float) -> list[Emission]:
        self._active.discard(done.request_id)
        out: list[Emission] = [
            Emission(
                timestamp=now,
                destination=LP_DISPATCHER,
                payload=UvDone(done.request_id, done.t_submit, done.t_start),
            )
        ]
        if self._pending:
            nxt = self._pending.popleft()
            service = sample_exponential(self._rng, self._mean)
            admission = self.core.on_departure(now, service)
            assert admission is not None and admission.finish_s is not None
            self._active.add(nxt.request_id)
            out.append(
                Emission(
                    timestamp=admission.finish_s,
                    destination=self.name,
                    payload=UvDone(nxt.request_id, nxt.t_submit, now),
                )
            )
        else:
            self.core.on_departure(now)
        return out

    # ------------------------------------------------------------------ thống kê
    @property
    def busy(self) -> int:
        return self.core.busy

    @property
    def waiting(self) -> int:
        return self.core.waiting

    def snapshot(self, t_end: float) -> dict[str, float | int]:
        return self.core.snapshot(t_end)
