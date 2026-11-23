"""Khung chung cho LP dịch vụ downstream (M/M/c hoặc M/M/n/n).

Bọc `StationCore` (thuần trạng thái, không random) và thêm phần mà core cố ý
không làm:

1. **sample thời gian phục vụ** từ `random.Random` riêng của LP (DEV.md §3),
2. **hàng đợi FIFO theo danh tính request** — `StationCore` chỉ giữ *thời điểm đến*,
   nên LP giữ thêm `deque[SubReq]` song song để biết ai được kéo lên,
3. **timer `JobDone`** để tới hạn thì nhả kênh và kéo job kế tiếp.

Luồng một job:
    SUB_REQ  → [nhận ngay]  → JobDone(t_start + s)
             → [xếp hàng]   → (không phát gì)
             → [bị từ chối] → SUB_RESP(ok=False, reason=...)   (chỉ M/M/n/n)
    JobDone  → SUB_RESP(ok=True, t_service_start, t_service_end) → network_link
             → nếu hàng đợi còn: kéo job kế, đặt JobDone mới

Chú ý: LP **không** huỷ job khi dispatcher đã hết hạn SLA. Job vẫn chạy xong (đúng
vật lý: DB đã nhận query), dispatcher tự bỏ qua response muộn. Nhờ vậy ΔN và
`3.0 × N_admitted` vẫn kiểm được ở mức hạ tầng.
"""

from __future__ import annotations

import random
from collections import deque
from typing import Final

from spe_sim.core.config import LP_NETWORK_LINK
from spe_sim.core.events import Branch, Emission, JobDone, SubReq, SubResp
from spe_sim.core.rng import sample_exponential
from spe_sim.lp.infra.station import StationCore

__all__ = ["ServiceLP"]


class ServiceLP:
    """Một trạm dịch vụ downstream có random riêng và timer nội bộ."""

    def __init__(
        self,
        name: str,
        channels: int,
        mean_service_s: float,
        rng: random.Random,
        *,
        capacity: int | None = None,
        loss: bool = False,
    ) -> None:
        if mean_service_s <= 0.0:
            raise ValueError(f"{name}: mean_service_s phải > 0, nhận {mean_service_s}")
        self.name: Final[str] = name
        self._rng = rng
        self._mean = mean_service_s
        self.core = StationCore(name, channels, capacity=capacity, loss=loss)
        self._pending: deque[SubReq] = deque()  # song song với hàng đợi của core
        self._jobs: dict[int, tuple[Branch, float, float]] = {}
        self.rejected = 0

    # ------------------------------------------------------------------ sự kiện
    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, SubReq):
            return self._on_request(payload, now)
        if isinstance(payload, JobDone):
            return self._on_done(payload, now)
        raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")

    def _on_request(self, req: SubReq, now: float) -> list[Emission]:
        if req.request_id in self._jobs:
            raise ValueError(f"{self.name}: request {req.request_id} đang phục vụ")

        admission = self.core.on_arrival(now, sample_exponential(self._rng, self._mean))

        if not admission.accepted:
            self.rejected += 1
            return [
                Emission(
                    timestamp=now,
                    destination=LP_NETWORK_LINK,
                    payload=SubResp(
                        request_id=req.request_id,
                        branch=req.branch,
                        t_sent=req.t_sent,
                        t_service_start=now,
                        t_service_end=now,
                        ok=False,
                        reason=admission.reason,
                    ),
                )
            ]

        if admission.queued:
            self._pending.append(req)
            return []

        return self._start_job(req, now, admission.finish_s)

    def _on_done(self, done: JobDone, now: float) -> list[Emission]:
        started = self._jobs.pop(done.request_id, None)
        if started is None:
            raise ValueError(f"{self.name}: JobDone cho request lạ {done.request_id}")

        # Nhả kênh; nếu có job đang chờ thì core kéo job đó lên ngay tại `now`.
        if self._pending:
            nxt = self._pending.popleft()
            next_service = sample_exponential(self._rng, self._mean)
            admission = self.core.on_departure(now, next_service)
            assert admission is not None and admission.finish_s is not None
            out = [
                Emission(
                    timestamp=now,
                    destination=LP_NETWORK_LINK,
                    payload=SubResp(
                        request_id=done.request_id,
                        branch=done.branch,
                        t_sent=done.t_sent,
                        t_service_start=done.t_start,
                        t_service_end=now,
                        ok=True,
                    ),
                )
            ]
            out.extend(self._start_job(nxt, now, admission.finish_s))
            return out

        self.core.on_departure(now)
        return [
            Emission(
                timestamp=now,
                destination=LP_NETWORK_LINK,
                payload=SubResp(
                    request_id=done.request_id,
                    branch=done.branch,
                    t_sent=done.t_sent,
                    t_service_start=done.t_start,
                    t_service_end=now,
                    ok=True,
                ),
            )
        ]

    def _start_job(self, req: SubReq, now: float, finish_s: float | None) -> list[Emission]:
        assert finish_s is not None
        self._jobs[req.request_id] = (req.branch, req.t_sent, now)
        return [
            Emission(
                timestamp=finish_s,
                destination=self.name,
                payload=JobDone(
                    request_id=req.request_id,
                    branch=req.branch,
                    t_sent=req.t_sent,
                    t_start=now,
                ),
            )
        ]

    # ------------------------------------------------------------------ thống kê
    @property
    def busy(self) -> int:
        return self.core.busy

    @property
    def in_system(self) -> int:
        return self.core.in_system

    def snapshot(self, t_end: float) -> dict[str, float | int]:
        return self.core.snapshot(t_end)

    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}(name={self.name!r}, in_system={self.in_system})"

