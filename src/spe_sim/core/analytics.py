"""Công thức giải tích dùng cho đối chiếu (Validation) và cho hằng số dẫn xuất.

KHOẢNG TRỐNG K9 (project.md §12)
--------------------------------
Spec §3.2/§3.3 cho *giá trị* `E[max(R1,R2,R3)] ≈ 38.84 ms` và "Quorum 2/3 → 16.13 ms"
nhưng KHÔNG cho công thức; `pe4`–`pe7` cũng không có order statistics.

Cách dẫn xuất dùng ở đây (ghi rõ để Cross-team kiểm):

    Với X_i ~ Exp(rate_i) độc lập, đặt S_k(t) = P(ít nhất k biến > t).
    Thì E[X_(k)] = ∫_0^∞ S_k(t) dt   (tail-sum formula, đúng cho biến không âm).

    S_k(t) được tính CHÍNH XÁC theo tổ hợp: duyệt mọi tập con S với |S| ≥ k,
        S_k(t) = Σ_S Π_{i∈S} e^{-t/m_i} · Π_{i∉S} (1 − e^{-t/m_i})
    (m_i là kỳ vọng của X_i). Tích phân làm bằng Simpson trên [0, 60·max(m_i)],
    phần đuôi < 1e-24 nên bỏ qua.

Kiểm chứng: với (12, 22, 27) ms → E[max] = 38.8372 ms  (spec: 38.84)
                              E[order 2] = 16.1328 ms  (spec: 16.13)
"""

from __future__ import annotations

import math
from typing import Sequence

from spe_sim.core.config import BRANCH_MEAN_S

_SIMPSON_INTERVALS = 8000
_TAIL_FACTOR = 60.0


# --------------------------------------------------------------------------------------
# Order statistics của biến mũ (dùng cho join policy và Thread Pinning)
# --------------------------------------------------------------------------------------
def _p_at_least_ge_exceed(means: Sequence[float], ge: int, t: float) -> float:
    """P(ít nhất `ge` biến trong `means` vượt quá `t`) — chính xác theo tổ hợp."""
    n = len(means)
    exceed = [math.exp(-t / m) for m in means]
    total = 0.0
    for mask in range(1 << n):
        if mask.bit_count() < ge:
            continue
        product = 1.0
        for i in range(n):
            product *= exceed[i] if (mask >> i) & 1 else (1.0 - exceed[i])
        total += product
    return total


def expected_kth_order_statistic(means: Sequence[float], k: int) -> float:
    """E[X_(k)] của `n` biến mũ độc lập với kỳ vọng `means` (1 ≤ k ≤ n).

    `X_(k)` là **thống kê thứ tự tăng dần**: `k = 1` là min, `k = n` là max.
    Liên hệ: `X_(k) > t` ⇔ có ít nhất `n − k + 1` biến vượt `t`.

    `k = n` cho E[max] (All-Join 3/3); `k = 2` với n = 3 cho E[phần tử giữa]
    (thời điểm nhánh thứ 2 hoàn tất ⇒ Quorum 2/3).
    """
    n = len(means)
    if not 1 <= k <= n:
        raise ValueError(f"k phải trong [1, {n}], nhận {k}")
    if any(m <= 0.0 for m in means):
        raise ValueError("mọi mean phải > 0")

    need = n - k + 1  # số biến phải vượt t để X_(k) > t
    upper = _TAIL_FACTOR * max(means)
    h = upper / _SIMPSON_INTERVALS
    total = _p_at_least_ge_exceed(means, need, 0.0) + _p_at_least_ge_exceed(means, need, upper)
    for i in range(1, _SIMPSON_INTERVALS):
        weight = 4.0 if i % 2 else 2.0
        total += weight * _p_at_least_ge_exceed(means, need, i * h)
    return total * h / 3.0


def expected_max(means: Sequence[float]) -> float:
    """E[max(X_1..X_n)] ≈ 38.84 ms với (12, 22, 27) ms."""
    return expected_kth_order_statistic(means, len(means))


def expected_second_of_three(means: Sequence[float]) -> float:
    """E[X_(2)] với n = 3 ≈ 16.13 ms với (12, 22, 27) ms — cơ sở của Quorum 2/3."""
    if len(means) != 3:
        raise ValueError("hàm này chỉ dùng cho đúng 3 nhánh")
    return expected_kth_order_statistic(means, 2)


def branch_order_stats() -> dict[str, float]:
    """Giá trị đối chiếu cho 3 nhánh mặc định (auth 12, catalog 22, db 27 ms)."""
    means = [BRANCH_MEAN_S["auth"], BRANCH_MEAN_S["catalog"], BRANCH_MEAN_S["db"]]
    return {
        "e_max_s": expected_max(means),
        "e_max_ms": expected_max(means) * 1000.0,
        "e_second_s": expected_second_of_three(means),
        "e_second_ms": expected_second_of_three(means) * 1000.0,
    }


# --------------------------------------------------------------------------------------
# Erlang-B / Erlang-C (spec §1.4 Bảng 1) — dùng cho Validation và cho DB pool
# --------------------------------------------------------------------------------------
def erlang_b(channels: int, offered_load: float) -> float:
    """Erlang-B: xác suất từ chối của M/M/n/n với a = λ/µ."""
    if channels < 1:
        raise ValueError("channels phải ≥ 1")
    if offered_load < 0.0:
        raise ValueError("offered_load phải ≥ 0")
    inverse = 1.0
    term = 1.0
    for i in range(1, channels + 1):
        term *= offered_load / i
        inverse += term
    term = 1.0
    for i in range(1, channels + 1):
        term *= offered_load / i
    return term / inverse


def erlang_c(channels: int, offered_load: float) -> float:
    """Erlang-C: xác suất một job phải xếp hàng của M/M/c (ρ = a/c < 1)."""
    if channels < 1:
        raise ValueError("channels phải ≥ 1")
    rho = offered_load / channels
    if rho >= 1.0:
        raise ValueError(f"cần ρ = a/c < 1 để có nghiệm ổn định, nhận ρ = {rho}")
    numerator = offered_load**channels / math.factorial(channels) * (channels / (channels - offered_load))
    partial = sum(offered_load**i / math.factorial(i) for i in range(channels))
    return numerator / (partial + numerator)


def mean_wait_mmc(channels: int, service_mean_s: float, lam: float) -> float:
    """Wq của M/M/c (giây) = C(c,a) / (cµ − λ)."""
    if channels < 1:
        raise ValueError("channels phải ≥ 1")
    offered = lam * service_mean_s
    if offered / channels >= 1.0:
        raise ValueError("hệ thống quá tải (ρ ≥ 1), Wq không xác định")
    return erlang_c(channels, offered) / (channels / service_mean_s - lam)


def mean_system_mm1k(lam: float, mu: float, capacity: int) -> dict[str, float]:
    """M/M/1/K: các chỉ số trạng thái dừng (p_K, λ_eff, E[n], E[r]).

    Dùng Continuous-Time Markov Chain hữu hạn trạng thái p_0..p_K (spec §1.4).
    """
    if lam <= 0.0 or mu <= 0.0 or capacity < 1:
        raise ValueError("cần λ > 0, µ > 0, K ≥ 1")
    rho = lam / mu
    if abs(rho - 1.0) < 1e-12:
        p = [1.0 / (capacity + 1)] * (capacity + 1)
    else:
        norm = (1.0 - rho) / (1.0 - rho ** (capacity + 1))
        p = [norm * rho**n for n in range(capacity + 1)]
    p_k = p[capacity]
    lam_eff = lam * (1.0 - p_k)
    mean_n = sum(n * p[n] for n in range(capacity + 1))
    mean_r = mean_n / lam_eff if lam_eff > 0 else float("inf")
    return {
        "p_drop": p_k,
        "lambda_eff": lam_eff,
        "mean_n": mean_n,
        "mean_r_s": mean_r,
        "rho": rho,
    }


def theoretical_capacities() -> dict[str, float]:
    """Trần thông lượng lý thuyết của 4 runtime model (spec §2.1.1, §3.3)."""
    return {
        "blocking_baseline": 12 * (1.0 / 0.0635),  # 188.98 req/s
        "nodejs_event_loop": 1.0 / 0.0032,  # 312.5 req/s
        "bun_event_loop": 1.0 / 0.0020,  # 500.0 req/s
        "spring_loom_vt": 4 * (1.0 / 0.0025),  # 1600 req/s
    }
