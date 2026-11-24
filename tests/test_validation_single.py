"""A2 (bản rút gọn để chạy nhanh): 6 hàng đợi đơn trên **LP của mạng** khớp giải tích.

Bản đầy đủ (R = 30, ~50 000 lượt đến, ngưỡng 5 % / 8 %) chạy bằng
`python -m spe_sim.validation --set all --impl both`. Ở đây chỉ R = 4 seed, ~8 000 lượt
đến, ρ = 0.5 (và 1.2 cho hàng đợi hữu hạn), ngưỡng nới 10 %.
"""

from __future__ import annotations

import unittest

from spe_sim.core.stats import ci95
from spe_sim.validation.params import Case, build_cases
from spe_sim.validation.reference_server import run_reference
from spe_sim.validation.single_queue import run_station

SEEDS = (1000, 1001, 1002, 1003)
ARRIVALS = 8_000
TOLERANCE = 0.10


def _run(case: Case, seed: int, impl: str = "station", kernel: str = "sequential") -> dict:
    t_sim = ARRIVALS / case.lam
    kw = dict(c=case.c, capacity=case.capacity, t_sim=t_sim, t_warmup=0.1 * t_sim, seed=seed)
    if impl == "reference":
        return run_reference(case.kendall, case.lam, case.mu, **kw)
    return run_station(case.kendall, case.lam, case.mu, kernel=kernel, **kw)


def _pick(kendall: str, rho: float) -> Case:
    return next(c for c in build_cases(["generic"]) if c.kendall == kendall and c.rho == rho)


class StationMatchesAnalyticTest(unittest.TestCase):
    def _check(self, case: Case, metrics: tuple[str, ...]) -> None:
        runs = [_run(case, seed) for seed in SEEDS]
        ana = case.analytic()
        for metric in metrics:
            mean, _ = ci95([r[metric] for r in runs])
            with self.subTest(model=case.kendall, rho=case.rho, metric=metric):
                self.assertLess(abs(mean - ana[metric]) / ana[metric], TOLERANCE)

    def test_open_queues_at_half_load(self) -> None:
        for kendall in ("M/M/1", "M/M/c", "M/M/inf"):
            metrics = ("W", "L") if kendall == "M/M/inf" else ("W", "L", "U")
            self._check(_pick(kendall, 0.5), metrics)

    def test_finite_queues_under_overload(self) -> None:
        for kendall in ("M/M/1/K", "M/M/c/K", "M/M/n/n"):
            self._check(_pick(kendall, 1.2), ("L", "U", "P_block"))


class KernelsAndImplementationsTest(unittest.TestCase):
    def test_coroutine_and_sequential_kernels_identical(self) -> None:
        case = _pick("M/M/c/K", 0.9)
        a = _run(case, 1000, kernel="sequential")
        b = _run(case, 1000, kernel="coroutine")
        for key in ("L", "W", "U", "P_block", "X"):
            self.assertAlmostEqual(a[key], b[key], places=12)

    def test_reference_server_agrees_with_station(self) -> None:
        """Đối chứng chéo: hai cài đặt độc lập, CI 95 % của L phải giao nhau."""
        case = _pick("M/M/c/K", 0.9)
        station = ci95([_run(case, s)["L"] for s in SEEDS])
        reference = ci95([_run(case, s, impl="reference")["L"] for s in SEEDS])
        self.assertLessEqual(abs(station[0] - reference[0]), station[1] + reference[1])


class ProjectParameterSetTest(unittest.TestCase):
    def test_project_suite_covers_table_1(self) -> None:
        labels = {c.label for c in build_cases(["project"])}
        for expected in ("Node.js event loop K=64", "Bun event loop K=64", "Auth Cluster",
                         "Catalog Service", "libuv Worker Pool", "DB pool n_db=24",
                         "Loom carriers K=64"):
            self.assertIn(expected, labels)
        kendalls = {c.kendall for c in build_cases(["project"])}
        self.assertEqual(kendalls, {"M/M/1", "M/M/1/K", "M/M/inf", "M/M/n/n", "M/M/c", "M/M/c/K"})

    def test_finite_models_include_overload_level(self) -> None:
        for case in build_cases(["generic", "project", "cpu"]):
            if case.kendall in ("M/M/1/K", "M/M/c/K", "M/M/n/n") and case.rho == 1.2:
                break
        else:  # pragma: no cover
            self.fail("thiếu mức ρ = 1.2 cho hàng đợi hữu hạn")


if __name__ == "__main__":
    unittest.main()
