"""P95 — hai quy ước phân vị (K10) và mức chênh lệch thực tế."""

from __future__ import annotations

import random
import unittest

from spe_sim.core.rng import sample_percentile


class TestPercentileConventions(unittest.TestCase):
    def test_linear_matches_numpy_definition(self) -> None:
        data = [1.0, 2.0, 3.0, 4.0]
        self.assertAlmostEqual(sample_percentile(data, 0.5), 2.5)
        self.assertAlmostEqual(sample_percentile(data, 0.95), 3.85)
        self.assertAlmostEqual(sample_percentile(data, 0.0), 1.0)
        self.assertAlmostEqual(sample_percentile(data, 1.0), 4.0)

    def test_nearest_rank_returns_a_real_sample(self) -> None:
        data = [10.0, 20.0, 30.0, 40.0, 50.0]
        self.assertEqual(sample_percentile(data, 0.95, method="nearest_rank"), 50.0)
        self.assertEqual(sample_percentile(data, 0.5, method="nearest_rank"), 30.0)
        for q in (0.01, 0.5, 0.9, 0.95, 0.99):
            self.assertIn(sample_percentile(data, q, method="nearest_rank"), data)

    def test_single_sample_and_validation(self) -> None:
        self.assertEqual(sample_percentile([7.0], 0.95), 7.0)
        self.assertEqual(sample_percentile([7.0], 0.95, method="nearest_rank"), 7.0)
        with self.assertRaises(ValueError):
            sample_percentile([], 0.95)
        with self.assertRaises(ValueError):
            sample_percentile([1.0], 1.5)
        with self.assertRaises(ValueError):
            sample_percentile([1.0], 0.95, method="median")

    def test_two_conventions_are_close_on_large_sample(self) -> None:
        rng = random.Random(3)
        data = [rng.expovariate(1 / 40.0) for _ in range(5000)]
        linear = sample_percentile(data, 0.95)
        nearest = sample_percentile(data, 0.95, method="nearest_rank")
        self.assertLess(abs(linear - nearest) / linear, 0.01)
        self.assertGreater(nearest, 100.0)  # ~p95 của Exp(mean 40) ≈ 119.8


if __name__ == "__main__":
    unittest.main()
