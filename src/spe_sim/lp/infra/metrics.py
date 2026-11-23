"""`MetricsRecorder` — gom kết cục thành các chỉ số của §2.1.2 / D5.

Chỉ tính trên **cửa sổ đo** `[t_warmup, T_sim]` (spec §1.5.2, §2.1.1); notice có
`t_complete < t_warmup` vẫn được giữ trong `n_total` nhưng không vào thống kê.

Các chỉ số:
    X         : throughput (req/s) — `completed / steady_window`
    E[r]      : thời gian đáp ứng trung bình của request **thành công** (ms)
    P95/P99   : phân vị thời gian đáp ứng (⚠ quy ước K10: linear interpolation)
    P_loss    : `1 − completed/n_window` (gộp timeout + db_drop + dropped)
    E[n]      : 2 cách đọc — Little (`X·E[r]`) và trung bình mẫu `n_in_system`

⚠ `n_in_system` trong `OutcomeNotice` là **ảnh chụp tại thời điểm chốt kết cục**, không
phải tích phân theo thời gian. Vì vậy `e_n_sampled` chỉ là chỉ báo phụ; giá trị dùng để
verify Little's Law là `e_n_little`. Việc lấy trung bình theo thời gian thật thuộc về
`AdmissionControlLP`/`StationCore.snapshot()` (đã có `mean_in_system`).

Module này **không** tính CI/Student-t — đó là việc của dev thống kê (project.md §7 D5).
"""

from __future__ import annotations

from collections import Counter
from typing import Final

from spe_sim.core.config import ADMISSION_CAPACITY_K
from spe_sim.core.events import Outcome, OutcomeNotice
from spe_sim.core.rng import sample_percentile

__all__ = ["MetricsRecorder", "MS_PER_S"]

MS_PER_S: Final[float] = 1000.0


class MetricsRecorder:
    """Tích luỹ `OutcomeNotice` và xuất bảng chỉ số của một lần chạy."""

    def __init__(
        self,
        t_warmup: float,
        t_end: float,
        *,
        capacity_k: int = ADMISSION_CAPACITY_K,
    ) -> None:
        if t_end <= 0.0:
            raise ValueError(f"t_end phải > 0, nhận {t_end}")
        if not 0.0 <= t_warmup < t_end:
            raise ValueError(f"cần 0 ≤ t_warmup < t_end, nhận {t_warmup}/{t_end}")
        self.t_warmup = t_warmup
        self.t_end = t_end
        self.steady_window_s = t_end - t_warmup
        self.capacity_k = capacity_k
        self.n_total = 0
        self._window: list[OutcomeNotice] = []

    # ------------------------------------------------------------------ nạp
    def record(self, notice: OutcomeNotice) -> None:
        self.n_total += 1
        if notice.t_complete >= self.t_warmup:
            self._window.append(notice)

    def extend(self, notices: list[OutcomeNotice]) -> None:
        for notice in notices:
            self.record(notice)

    @property
    def window_notices(self) -> list[OutcomeNotice]:
        return list(self._window)

    # ------------------------------------------------------------------ xuất
    def summary(self) -> dict[str, float | int | None]:
        """Bảng chỉ số; giá trị `None` khi chưa có mẫu (ví dụ 0 request thành công)."""
        window = self._window
        n_window = len(window)
        cuts = Counter(notice.outcome for notice in window)

        completed = [n for n in window if n.outcome is Outcome.COMPLETED]
        r_ms = sorted((n.t_complete - n.t_arrive) * MS_PER_S for n in completed)

        e_r_ms = sum(r_ms) / len(r_ms) if r_ms else None
        x_completed = len(r_ms) / self.steady_window_s
        p_loss = (n_window - len(r_ms)) / n_window if n_window else None

        sampled_n = [n.n_in_system for n in window]
        return {
            "t_warmup": self.t_warmup,
            "t_end": self.t_end,
            "steady_window_s": self.steady_window_s,
            "n_total": self.n_total,
            "n_window": n_window,
            "n_completed": cuts.get(Outcome.COMPLETED, 0),
            "n_timeout": cuts.get(Outcome.TIMEOUT, 0),
            "n_db_drop": cuts.get(Outcome.DB_DROP, 0),
            "n_dropped": cuts.get(Outcome.DROPPED, 0),
            "x_completed_rps": x_completed,
            "x_terminal_rps": n_window / self.steady_window_s,
            "e_r_ms": e_r_ms,
            "p95_ms": sample_percentile(r_ms, 0.95) if r_ms else None,
            "p99_ms": sample_percentile(r_ms, 0.99) if r_ms else None,
            "max_r_ms": r_ms[-1] if r_ms else None,
            "p_loss": p_loss,
            "e_n_little": (x_completed * e_r_ms / MS_PER_S) if e_r_ms is not None else None,
            "e_n_sampled": (sum(sampled_n) / len(sampled_n)) if sampled_n else None,
            "max_n_in_system": max(sampled_n) if sampled_n else 0,
            "capacity_k": self.capacity_k,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"MetricsRecorder(t_warmup={self.t_warmup}, t_end={self.t_end}, "
            f"n_window={len(self._window)})"
        )
