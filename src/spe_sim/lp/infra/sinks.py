"""Sink LP — nơi kết cục của request dừng lại và được ghi thành bản ghi D3.

Ba sink theo ba nhóm HTTP status (spec §1.3):
    sink.completed : 200  → Outcome.COMPLETED
    sink.timeout   : 504  → Outcome.TIMEOUT và Outcome.DB_DROP (cùng 504, khác nguyên nhân)
    sink.dropped   : 503  → Outcome.DROPPED (nghẽn ở Admission Control)

Vì sao `TIMEOUT` và `DB_DROP` chung một sink: schema D3 chỉ có một cột `outcome`,
phân biệt hai nguyên nhân bằng giá trị enum + `reason`. Gộp theo mã HTTP giúp dev vẽ
biểu đồ `P_loss(λ)` cộng trực tiếp mà không phải lọc lại.

Sink **không** import `bkitevsim`: `handle_event()` luôn trả danh sách rỗng.
"""

from __future__ import annotations

from typing import Final, Iterable

from spe_sim.core.events import Emission, Outcome, OutcomeNotice
from spe_sim.core.records import RequestRecord, records_from_notices

__all__ = [
    "SINK_COMPLETED",
    "SINK_TIMEOUT",
    "SINK_DROPPED",
    "SINK_NAMES",
    "SinkLP",
    "build_sinks",
]

SINK_COMPLETED: Final[str] = "sink.completed"
SINK_TIMEOUT: Final[str] = "sink.timeout"
SINK_DROPPED: Final[str] = "sink.dropped"

SINK_NAMES: Final[tuple[str, ...]] = (SINK_COMPLETED, SINK_TIMEOUT, SINK_DROPPED)


class SinkLP:
    """Thu `OutcomeNotice` đúng nhóm của mình; nhận sai nhóm là lỗi lập trình."""

    def __init__(
        self,
        name: str,
        outcomes: frozenset[Outcome] | None = None,
    ) -> None:
        self.name = name
        self._outcomes = outcomes
        self.notices: list[OutcomeNotice] = []

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if not isinstance(payload, OutcomeNotice):
            raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")
        if self._outcomes is not None and payload.outcome not in self._outcomes:
            raise ValueError(
                f"{self.name}: nhận sai kết cục {payload.outcome.value!r} "
                f"(chỉ nhận {sorted(o.value for o in self._outcomes)})"
            )
        self.notices.append(payload)
        return []

    # ------------------------------------------------------------------ tiện ích
    @property
    def count(self) -> int:
        return len(self.notices)

    def records(
        self,
        *,
        seed: int,
        runtime_model: str,
        join_mode: str,
        lam: float,
    ) -> list[RequestRecord]:
        """Chuyển sang `RequestRecord` đúng schema D3 (T4)."""
        return records_from_notices(
            self.notices,
            seed=seed,
            runtime_model=runtime_model,
            join_mode=join_mode,
            lam=lam,
        )

    def __repr__(self) -> str:  # pragma: no cover
        return f"SinkLP(name={self.name!r}, count={self.count})"


def build_sinks() -> dict[str, SinkLP]:
    """Tạo bộ 3 sink chuẩn, khoá là tên LP đích dùng trong `Emission.destination`."""
    return {
        SINK_COMPLETED: SinkLP(SINK_COMPLETED, frozenset({Outcome.COMPLETED})),
        SINK_TIMEOUT: SinkLP(
            SINK_TIMEOUT, frozenset({Outcome.TIMEOUT, Outcome.DB_DROP})
        ),
        SINK_DROPPED: SinkLP(SINK_DROPPED, frozenset({Outcome.DROPPED})),
    }


def all_notices(sinks: Iterable[SinkLP]) -> list[OutcomeNotice]:
    """Gộp notice của nhiều sink, sắp theo `t_complete` rồi `request_id`."""
    out: list[OutcomeNotice] = []
    for sink in sinks:
        out.extend(sink.notices)
    out.sort(key=lambda n: (n.t_complete, n.request_id))
    return out
