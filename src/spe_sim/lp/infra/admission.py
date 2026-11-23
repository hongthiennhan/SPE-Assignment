"""`AdmissionControlLP` — cổng `n(t) < K`, từ chối bằng HTTP 503 (spec §1.2, §1.3).

`n(t)` là số request **đang ở trong hệ thống**: đã admit và chưa có kết cục.
Đây là biến quyết định của Admission Control, không phải hàng đợi riêng — buffer
`K` nằm ở phía dispatcher, admission chỉ *đếm* và *chặn*.

Hợp đồng kernel-free: `handle_event()` trả `Emission`.
Luồng vào/ra:
    REQ_ARRIVAL  → ADMITTED (LP_DISPATCHER)  nếu `n(t) < K`
                 → OUTCOME_NOTICE(DROPPED) sang sink nếu đầy
    OUTCOME_NOTICE (do dispatcher gửi kèm khi chốt kết cục) → nhả slot.

Dùng chính `OutcomeNotice` làm tín hiệu nhả slot (thay vì thêm payload mới) vì
dispatcher đã có sẵn đối tượng đó và gửi song song cho sink — một nguồn sự thật
duy nhất cho "request này đã rời hệ thống".
"""

from __future__ import annotations

from typing import Final

from spe_sim.core.config import (
    ADMISSION_CAPACITY_K,
    LP_DISPATCHER,
    JoinMode,
    RuntimeModelName,
)
from spe_sim.core.events import (
    Admitted,
    Emission,
    Outcome,
    OutcomeNotice,
    ReqArrival,
)
from spe_sim.lp.infra.sinks import SINK_DROPPED

__all__ = ["AdmissionControlLP"]

REASON_ADMISSION_FULL: Final[str] = "admission_full"  # n(t) = K


class AdmissionControlLP:
    """Cổng vào: giữ tập request đang hoạt động, chặn khi đạt `capacity_k`."""

    def __init__(
        self,
        runtime_model: RuntimeModelName,
        join_mode: JoinMode,
        *,
        capacity_k: int = ADMISSION_CAPACITY_K,
        name: str = "admission",
    ) -> None:
        if capacity_k < 1:
            raise ValueError(f"K phải ≥ 1, nhận {capacity_k}")
        self.name: Final[str] = name
        self.capacity_k = capacity_k
        self.runtime_model = runtime_model
        self.join_mode = join_mode

        self._active: dict[int, float] = {}  # request_id -> t_arrive
        self.admitted = 0
        self.dropped = 0
        self.max_n_in_system = 0

    # ------------------------------------------------------------------ trạng thái
    @property
    def n_in_system(self) -> int:
        return len(self._active)

    def is_active(self, request_id: int) -> bool:
        return request_id in self._active

    # ------------------------------------------------------------------ sự kiện
    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, ReqArrival):
            return self._on_arrival(payload, now)
        if isinstance(payload, OutcomeNotice):
            self._on_release(payload)
            return []
        raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")

    def _on_arrival(self, arrival: ReqArrival, now: float) -> list[Emission]:
        if arrival.request_id in self._active:
            raise ValueError(f"{self.name}: request {arrival.request_id} đã ở trong hệ thống")

        if self.n_in_system < self.capacity_k:
            self._active[arrival.request_id] = arrival.t_arrive
            self.admitted += 1
            self.max_n_in_system = max(self.max_n_in_system, self.n_in_system)
            return [
                Emission(
                    timestamp=now,
                    destination=LP_DISPATCHER,
                    payload=Admitted(request_id=arrival.request_id, t_arrive=arrival.t_arrive),
                )
            ]

        self.dropped += 1
        return [
            Emission(
                timestamp=now,
                destination=SINK_DROPPED,
                payload=OutcomeNotice(
                    request_id=arrival.request_id,
                    outcome=Outcome.DROPPED,
                    t_arrive=arrival.t_arrive,
                    t_dispatch=now,
                    t_complete=now,
                    n_in_system=self.n_in_system,
                    runtime_model=self.runtime_model,
                    join_mode=self.join_mode,
                    reason=REASON_ADMISSION_FULL,
                ),
            )
        ]

    def _on_release(self, notice: OutcomeNotice) -> None:
        arrival = self._active.pop(notice.request_id, None)
        if arrival is None:
            raise ValueError(
                f"{self.name}: nhả slot cho request {notice.request_id} không có trong hệ thống"
            )
        if abs(arrival - notice.t_arrive) > 1e-9:
            raise ValueError(
                f"{self.name}: t_arrive lệch cho request {notice.request_id} "
                f"({arrival} != {notice.t_arrive})"
            )
