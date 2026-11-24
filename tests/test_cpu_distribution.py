"""A4 + B2: thời gian CPU ~ Exp trong Stochastic Workload và hệ số `cpu_scale`."""

from __future__ import annotations

import random
import statistics
import unittest

from spe_sim.core.config import ScenarioConfig, get_profile
from spe_sim.lp.runtime.dispatcher import sample_cpu_phases
from spe_sim.lp.runtime.models.base import build_model
from spe_sim.runner.study import run_scenario


class SampleCpuPhasesTest(unittest.TestCase):
    def test_deterministic_without_rng(self) -> None:
        self.assertEqual(sample_cpu_phases(0.00192, 0.00128, None), (0.00192, 0.00128))

    def test_exponential_total_keeps_mean_and_split(self) -> None:
        rng = random.Random(42)
        draws = [sample_cpu_phases(0.0015, 0.0010, rng) for _ in range(40_000)]
        totals = [a + b for a, b in draws]
        self.assertAlmostEqual(statistics.fmean(totals), 0.0025, delta=0.0025 * 0.02)
        # Biến mũ: độ lệch chuẩn ≈ kỳ vọng (CV = 1).
        self.assertAlmostEqual(statistics.stdev(totals) / statistics.fmean(totals), 1.0, delta=0.03)
        for ingress, callback in draws[:100]:
            self.assertAlmostEqual(ingress / (ingress + callback), 0.6, places=12)


class CpuScaleTest(unittest.TestCase):
    def test_cpu_scale_doubles_event_loop_cost(self) -> None:
        node = get_profile("nodejs_event_loop", cpu_scale=2.0)
        self.assertAlmostEqual(node.cpu_total_s * 1000, 6.4)
        self.assertAlmostEqual(node.ingress_s * 1000, 3.84)
        self.assertAlmostEqual(node.callback_s * 1000, 2.56)
        self.assertAlmostEqual(node.theoretical_capacity_rps, 156.25)

    def test_cpu_scale_only_for_event_loops(self) -> None:
        for name in ("spring_loom_vt", "blocking_baseline"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                get_profile(name, cpu_scale=2.0)
        with self.assertRaises(ValueError):
            build_model("spring_loom_vt", cpu_scale=2.0)

    def test_scenario_config_validates_new_fields(self) -> None:
        ok = ScenarioConfig("bun_event_loop", "gather", 150.0, 1, cpu_scale=2.0)
        self.assertAlmostEqual(ok.profile.cpu_total_s * 1000, 4.0)
        with self.assertRaises(ValueError):
            ScenarioConfig("bun_event_loop", "gather", 150.0, 1, cpu_scale=3.0)
        with self.assertRaises(ValueError):
            ScenarioConfig("spring_loom_vt", "gather", 150.0, 1, cpu_scale=2.0)
        with self.assertRaises(ValueError):
            ScenarioConfig("bun_event_loop", "gather", 150.0, 1, cpu_dist="normal")  # type: ignore[arg-type]


class StochasticWorkloadRunTest(unittest.TestCase):
    def _cfg(self, **kw) -> ScenarioConfig:
        base = dict(runtime_model="nodejs_event_loop", join_mode="gather", lam=150.0,
                    seed=1000, t_sim=6.0, t_warmup=1.0)
        base.update(kw)
        return ScenarioConfig(**base)  # type: ignore[arg-type]

    def test_exp_and_det_differ_but_each_reproduces(self) -> None:
        exp1 = run_scenario(self._cfg(), kernel="sequential").metrics["e_r_ms"]
        exp2 = run_scenario(self._cfg(), kernel="sequential").metrics["e_r_ms"]
        det = run_scenario(self._cfg(cpu_dist="det"), kernel="sequential").metrics["e_r_ms"]
        self.assertEqual(exp1, exp2)
        self.assertNotEqual(exp1, det)

    def test_engines_agree_with_exponential_cpu(self) -> None:
        cfg = self._cfg(runtime_model="spring_loom_vt", p_pin=0.20, lam=200.0)
        seq = run_scenario(cfg, kernel="sequential").notices
        cor = run_scenario(cfg, kernel="coroutine").notices
        self.assertEqual(len(seq), len(cor))
        for a, b in zip(seq, cor):
            self.assertEqual((a.request_id, a.outcome), (b.request_id, b.outcome))
            self.assertAlmostEqual(a.t_complete, b.t_complete, places=12)


if __name__ == "__main__":
    unittest.main()
