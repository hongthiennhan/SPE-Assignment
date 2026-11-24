"""A3 (bản rút gọn): trạm CPU cô lập của runtime khớp M/M/1/64 và M/M/c/64.

Dùng **dispatcher thật** + Admission Control thật, backend trả lời tức thì (xem
`spe_sim.validation.cpu_station`). Bản đầy đủ: `python -m spe_sim.validation --set cpu`.
"""

from __future__ import annotations

import unittest

from spe_sim.core.analytics import mm1k, mmck
from spe_sim.core.stats import ci95
from spe_sim.validation.cpu_station import cpu_station_queue, run_cpu_station

SEEDS = (1000, 1001, 1002)
ARRIVALS = 8_000
TOLERANCE = 0.10


def _analytic(model: str, lam: float) -> dict:
    kendall, mu, c, capacity = cpu_station_queue(model)
    return mm1k(lam, mu, capacity) if kendall == "M/M/1/K" else mmck(lam, mu, c, capacity)


def _mean(model: str, rho: float, metric: str, kernel: str = "sequential") -> tuple[float, float]:
    _, mu, c, _ = cpu_station_queue(model)
    lam = rho * c * mu
    t_sim = ARRIVALS / lam
    values = [
        run_cpu_station(model, lam, t_sim=t_sim, t_warmup=0.1 * t_sim, seed=s, kernel=kernel)[metric]
        for s in SEEDS
    ]
    return ci95(values)[0], _analytic(model, lam)[metric]


class CpuStationTest(unittest.TestCase):
    def test_queue_mapping_matches_table_1(self) -> None:
        self.assertEqual(cpu_station_queue("nodejs_event_loop")[:3], ("M/M/1/K", 312.5, 1))
        self.assertEqual(cpu_station_queue("bun_event_loop")[:3], ("M/M/1/K", 500.0, 1))
        self.assertEqual(cpu_station_queue("spring_loom_vt")[:3], ("M/M/c/K", 400.0, 4))
        self.assertEqual(cpu_station_queue("blocking_baseline")[:3], ("M/M/c/K", 400.0, 12))
        self.assertEqual(cpu_station_queue("nodejs_event_loop")[3], 64)

    def test_event_loop_is_mm1_64(self) -> None:
        for rho, metric in ((0.5, "W"), (0.5, "U"), (1.2, "P_block"), (1.2, "L")):
            sim, ana = _mean("nodejs_event_loop", rho, metric)
            with self.subTest(rho=rho, metric=metric):
                self.assertLess(abs(sim - ana) / ana, TOLERANCE)

    def test_loom_carrier_pool_is_mm4_64(self) -> None:
        for rho, metric in ((0.5, "W"), (0.9, "U"), (1.2, "P_block")):
            sim, ana = _mean("spring_loom_vt", rho, metric)
            with self.subTest(rho=rho, metric=metric):
                self.assertLess(abs(sim - ana) / ana, TOLERANCE)

    def test_both_dispatchers_identical(self) -> None:
        _, mu, c, _ = cpu_station_queue("bun_event_loop")
        lam = 0.9 * c * mu
        runs = [
            run_cpu_station("bun_event_loop", lam, t_sim=4.0, t_warmup=0.4, seed=7, kernel=k)
            for k in ("sequential", "coroutine")
        ]
        for key in ("L", "W", "U", "P_block"):
            self.assertAlmostEqual(runs[0][key], runs[1][key], places=12)


if __name__ == "__main__":
    unittest.main()
