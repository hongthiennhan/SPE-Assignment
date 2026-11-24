"""A5 + C1: công thức giải tích gộp từ harness hàng đợi đơn và module thống kê chung.

Kiểm ba điều:
1. Công thức truy hồi (Erlang-B/C) trùng dạng giai thừa của spec Bảng 1.
2. Sáu hàm hàng đợi đơn nhất quán với nhau (trường hợp riêng của M/M/c/K).
3. Tái hiện đúng các số giải tích của spec §3.3.
"""

from __future__ import annotations

import math
import unittest

from spe_sim.core.analytics import (
    ANALYTIC_MODELS,
    erlang_b,
    erlang_c,
    mean_system_mm1k,
    mm1,
    mm1k,
    mmc,
    mmck,
    mminf,
    mmnn,
)
from spe_sim.core.stats import ci95, summarize_replications, t_quantile_975


def _erlang_b_factorial(n: int, a: float) -> float:
    """Dạng gốc của spec: (aⁿ/n!) / Σ_{i=0..n} aⁱ/i!."""
    return (a**n / math.factorial(n)) / sum(a**i / math.factorial(i) for i in range(n + 1))


def _erlang_c_factorial(c: int, a: float) -> float:
    top = a**c / math.factorial(c) * (c / (c - a))
    return top / (sum(a**i / math.factorial(i) for i in range(c)) + top)


class ErlangFormulaTest(unittest.TestCase):
    def test_erlang_b_recursion_matches_factorial_form(self) -> None:
        for n, a in ((1, 0.5), (10, 7.0), (16, 12.0), (24, 3.75), (24, 20.0)):
            with self.subTest(n=n, a=a):
                self.assertAlmostEqual(erlang_b(n, a), _erlang_b_factorial(n, a), delta=1e-12)

    def test_erlang_c_matches_factorial_form(self) -> None:
        for c, a in ((1, 0.5), (4, 3.2), (8, 1.5), (16, 3.0)):
            with self.subTest(c=c, a=a):
                self.assertAlmostEqual(erlang_c(c, a), _erlang_c_factorial(c, a), delta=1e-12)


class SingleQueueConsistencyTest(unittest.TestCase):
    def test_mm1k_equals_mean_system_mm1k(self) -> None:
        legacy = mean_system_mm1k(150.0, 312.5, 64)
        merged = mm1k(150.0, 312.5, 64)
        self.assertAlmostEqual(merged["L"], legacy["mean_n"], places=12)
        self.assertAlmostEqual(merged["W"], legacy["mean_r_s"], places=12)
        self.assertAlmostEqual(merged["P_block"], legacy["p_drop"], places=12)

    def test_mmck_with_one_channel_is_mm1k(self) -> None:
        for lam in (50.0, 300.0, 450.0):
            a, b = mmck(lam, 312.5, 1, 64), mm1k(lam, 312.5, 64)
            for key in ("L", "Lq", "W", "Wq", "X", "P_block", "U"):
                with self.subTest(lam=lam, key=key):
                    self.assertAlmostEqual(a[key], b[key], places=9)

    def test_mmck_with_k_equal_c_is_erlang_loss(self) -> None:
        a, b = mmck(480.0, 40.0, 16, 16), mmnn(480.0, 40.0, 16)
        for key in ("L", "X", "P_block", "U"):
            with self.subTest(key=key):
                self.assertAlmostEqual(a[key], b[key], places=9)

    def test_mmck_with_huge_k_approaches_mmc(self) -> None:
        a, b = mmck(600.0, 100.0, 8, 2000), mmc(600.0, 100.0, 8)
        for key in ("L", "W", "Wq", "U"):
            with self.subTest(key=key):
                self.assertAlmostEqual(a[key], b[key], places=7)

    def test_mm1_and_mminf_closed_forms(self) -> None:
        r = mm1(80.0, 100.0)
        self.assertAlmostEqual(r["L"], 4.0)
        self.assertAlmostEqual(r["W"], 0.05)
        inf = mminf(500.0, 1000.0)
        self.assertEqual(inf["Wq"], 0.0)
        self.assertAlmostEqual(inf["L"], 0.5)
        self.assertIsNone(inf["U"])

    def test_littles_law_holds_in_every_formula(self) -> None:
        cases = [
            mm1(70.0, 100.0),
            mm1k(120.0, 100.0, 10),
            mminf(300.0, 100.0),
            mmnn(800.0, 100.0, 10),
            mmc(360.0, 100.0, 4),
            mmck(480.0, 100.0, 4, 16),
        ]
        for r in cases:
            self.assertAlmostEqual(r["L"], r["X"] * r["W"], places=9)

    def test_registry_has_six_models(self) -> None:
        self.assertEqual(
            set(ANALYTIC_MODELS), {"M/M/1", "M/M/1/K", "M/M/inf", "M/M/n/n", "M/M/c", "M/M/c/K"}
        )

    def test_unstable_open_queues_raise(self) -> None:
        with self.assertRaises(ValueError):
            mm1(100.0, 100.0)
        with self.assertRaises(ValueError):
            mmc(400.0, 100.0, 4)


class SpecSection33Test(unittest.TestCase):
    """Số giải tích của spec §3.3 phải tái hiện được từ các hàm gộp."""

    def test_db_pool_erlang_b_negligible(self) -> None:
        self.assertLess(erlang_b(24, 150.0 / 40.0), 1e-10)  # < 10⁻⁸ %

    def test_blocking_baseline_mm12_64(self) -> None:
        mu = 1.0 / 0.0635
        at150 = mmck(150.0, mu, 12, 64)
        self.assertAlmostEqual(at150["Wq"] * 1e3, 9.10, delta=0.01)
        self.assertAlmostEqual(at150["W"] * 1e3, 72.60, delta=0.01)
        at200 = mmck(200.0, mu, 12, 64)
        self.assertAlmostEqual(at200["W"] * 1e3, 260.99, delta=0.01)
        self.assertAlmostEqual(at200["P_block"] * 100, 5.74, delta=0.01)

    def test_event_loops_mm1_64(self) -> None:
        self.assertAlmostEqual(mm1k(150.0, 312.5, 64)["W"] * 1e3, 6.15, delta=0.01)
        self.assertAlmostEqual(mm1k(300.0, 312.5, 64)["W"] * 1e3, 63.79, delta=0.01)
        self.assertAlmostEqual(mm1k(400.0, 312.5, 64)["P_block"] * 100, 21.88, delta=0.01)
        self.assertAlmostEqual(mm1k(150.0, 500.0, 64)["W"] * 1e3, 2.86, delta=0.01)
        self.assertAlmostEqual(mm1k(400.0, 500.0, 64)["W"] * 1e3, 10.00, delta=0.01)

    def test_loom_carrier_pool(self) -> None:
        self.assertAlmostEqual(mmck(150.0, 400.0, 4, 64)["W"] * 1e3, 2.50, delta=0.01)
        # p_pin = 20%: giữ carrier 2.5 + 0.2 × 38.84 = 10.27 ms; spec làm tròn ra 3.21 %.
        pinned = mmck(400.0, 1.0 / 0.010268, 4, 64)
        self.assertAlmostEqual(pinned["P_block"] * 100, 3.21, delta=0.05)


class StatsTest(unittest.TestCase):
    def test_t_quantile_table_and_df29(self) -> None:
        self.assertAlmostEqual(t_quantile_975(29), 2.045, delta=1e-3)
        self.assertAlmostEqual(t_quantile_975(1), 12.706, delta=1e-3)

    def test_ci95_known_sample(self) -> None:
        mean, half = ci95([1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertAlmostEqual(mean, 3.0)
        self.assertAlmostEqual(half, 2.776 * math.sqrt(2.5) / math.sqrt(5), delta=1e-3)

    def test_ci95_single_sample_has_zero_width(self) -> None:
        self.assertEqual(ci95([7.0]), (7.0, 0.0))

    def test_summarize_skips_none(self) -> None:
        table = summarize_replications(
            [{"x": 1.0, "y": None}, {"x": 3.0, "y": None}], ["x", "y"]
        )
        self.assertEqual(table["x"][0], 2.0)
        self.assertEqual(table["x"][2], 2)
        self.assertIsNone(table["y"])


if __name__ == "__main__":
    unittest.main()
