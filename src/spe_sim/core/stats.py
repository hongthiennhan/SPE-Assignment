"""Thống kê dùng chung: khoảng tin cậy 95% Student-t trên R replication (spec §1.5.2).

Gộp từ harness hàng đợi đơn (`validation_single_queues.py`) để cả phần kiểm chứng 6 hàng
đợi đơn lẫn kết quả mạng Fork-Join dùng cùng một cách tính CI.

    mean, half = ci95([x_1, ..., x_R])          # CI = mean ± half
    table = summarize_replications(results, ["x_completed_rps", "e_r_ms"])
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Iterable, Mapping, Sequence

__all__ = ["T_TABLE_975", "ci95", "summarize_replications", "t_quantile_975"]

#: Phân vị 0.975 của Student-t theo bậc tự do — dự phòng khi không có `scipy`.
T_TABLE_975: dict[int, float] = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
    9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074,
    23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045,
    30: 2.042, 39: 2.023, 49: 2.010, 59: 2.001, 99: 1.984,
}


def t_quantile_975(df: int) -> float:
    """t_{0.975, df}. Dùng `scipy` nếu có; không thì tra bảng (lấy df gần nhất phía dưới
    trong bảng — cho giá trị lớn hơn, tức CI rộng hơn một chút, an toàn)."""
    if df < 1:
        raise ValueError(f"df phải ≥ 1, nhận {df}")
    try:
        from scipy.stats import t  # type: ignore[import-not-found]

        return float(t.ppf(0.975, df))
    except ImportError:
        if df in T_TABLE_975:
            return T_TABLE_975[df]
        lower = [k for k in T_TABLE_975 if k <= df]
        return T_TABLE_975[max(lower)] if lower else 1.960


def ci95(values: Sequence[float]) -> tuple[float, float]:
    """Trung bình mẫu và nửa độ rộng CI 95% Student-t. Một mẫu ⇒ nửa độ rộng = 0."""
    if not values:
        raise ValueError("cần ít nhất một mẫu")
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean, 0.0
    half = t_quantile_975(len(values) - 1) * statistics.stdev(values) / math.sqrt(len(values))
    return mean, half


def summarize_replications(
    results: Iterable[Mapping[str, Any]],
    keys: Sequence[str],
) -> dict[str, tuple[float, float, int] | None]:
    """CI 95% cho từng `key` qua các replication.

    Trả `{key: (mean, half_width, n)}`; replication có giá trị `None` (ví dụ E[r] khi không
    có request thành công) bị bỏ qua, và `None` nếu không còn mẫu nào.
    """
    rows = list(results)
    out: dict[str, tuple[float, float, int] | None] = {}
    for key in keys:
        values = [float(row[key]) for row in rows if row.get(key) is not None]
        if not values:
            out[key] = None
            continue
        mean, half = ci95(values)
        out[key] = (mean, half, len(values))
    return out
