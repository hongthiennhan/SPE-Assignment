"""Test T13–T17: 4 runtime model, Thread Pinning (`p_pin`) và libuv offload (`p_uv`)."""

from __future__ import annotations

import unittest

from spe_sim.core.config import MS, PROFILES, RUNTIME_MODELS
from spe_sim.core.events import Outcome
from spe_sim.lp.runtime.models.base import MODEL_REGISTRY, build_model, model_names
from spe_sim.runner.deterministic import run_deterministic


class TestRegistry(unittest.TestCase):
    def test_all_four_models_registered(self) -> None:
        self.assertEqual(set(model_names()), set(RUNTIME_MODELS))
        self.assertEqual(set(MODEL_REGISTRY), set(RUNTIME_MODELS))

    def test_profiles_match_config(self) -> None:
        for name in RUNTIME_MODELS:
            model = build_model(name)
            self.assertEqual(model.profile.name, PROFILES[name].name)
            self.assertEqual(model.profile.channels, PROFILES[name].channels)

    def test_unsupported_factors_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_model("blocking_baseline", p_pin=0.05)
        with self.assertRaises(ValueError):
            build_model("blocking_baseline", p_uv=0.15)
        with self.assertRaises(ValueError):
            build_model("nodejs_event_loop", p_pin=0.05)
        with self.assertRaises(ValueError):
            build_model("spring_loom_vt", p_uv=0.15)

    def test_unknown_model_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_model("tomcat")

    def test_pin_or_uv_requires_rng(self) -> None:
        with self.assertRaises(ValueError):
            build_model("spring_loom_vt", p_pin=0.1).build_dispatcher("gather")

    def test_describe_has_handover_fields(self) -> None:
        info = build_model("spring_loom_vt", p_pin=0.20).describe()
        self.assertEqual(info["model_name"], "spring_loom_vt")
        self.assertAlmostEqual(info["p_pin"], 0.20)
        self.assertAlmostEqual(info["theoretical_capacity_rps"], 389.59)


class TestPinning(unittest.TestCase):
    def test_blocking_always_pins(self) -> None:
        result = run_deterministic("blocking_baseline", "gather", arrivals=(0.0, 0.1))
        self.assertEqual(result.dispatcher.pinned_requests, 2)
        self.assertAlmostEqual(result.r_ms()[0], 63.5, places=9)

    def test_loom_without_pinning_never_holds_carrier(self) -> None:
        result = run_deterministic("spring_loom_vt", "gather", arrivals=(0.0, 0.1))
        self.assertEqual(result.dispatcher.pinned_requests, 0)
        for r in result.r_ms():
            self.assertAlmostEqual(r, 29.5, places=9)

    def test_loom_full_pinning_serializes_on_four_carriers(self) -> None:
        arrivals = tuple(0.0 for _ in range(8))
        result = run_deterministic(
            "spring_loom_vt", "gather", arrivals=arrivals, p_pin=1.0, seed=1
        )
        self.assertEqual(result.dispatcher.pinned_requests, 8)
        rs = sorted(result.r_ms())
        # 4 carrier: 4 request đầu xong ở 29.5 ms, 4 request sau bị chờ thêm 29.5 ms.
        self.assertAlmostEqual(rs[0], 29.5, places=9)
        self.assertAlmostEqual(rs[-1], 59.0, places=9)

    def test_partial_pinning_is_deterministic(self) -> None:
        kwargs = dict(arrivals=tuple(0.0 for _ in range(20)), p_pin=0.2, seed=5)
        a = run_deterministic("spring_loom_vt", "gather", **kwargs)
        b = run_deterministic("spring_loom_vt", "gather", **kwargs)
        c = run_deterministic("spring_loom_vt", "gather", **{**kwargs, "seed": 6})
        self.assertEqual([round(r, 9) for r in a.r_ms()], [round(r, 9) for r in b.r_ms()])
        self.assertNotEqual(
            [round(r, 9) for r in a.r_ms()], [round(r, 9) for r in c.r_ms()]
        )
        self.assertGreater(a.dispatcher.pinned_requests, 0)
        self.assertLess(a.dispatcher.pinned_requests, 20)


class TestUvOffload(unittest.TestCase):
    def test_offload_adds_worker_pool_delay(self) -> None:
        result = run_deterministic(
            "nodejs_event_loop", "gather", p_uv=1.0, seed=2
        )
        self.assertEqual(result.dispatcher.uv_requests, 1)
        # 12 (libuv) + 1.92 (ingress) + 27 (max nhánh) + 1.28 (callback)
        self.assertAlmostEqual(result.r_ms()[0], 42.20, places=9)
        self.assertIs(result.notices[0].outcome, Outcome.COMPLETED)

    def test_no_offload_when_p_uv_zero(self) -> None:
        result = run_deterministic("bun_event_loop", "gather")
        self.assertEqual(result.dispatcher.uv_requests, 0)
        self.assertAlmostEqual(result.r_ms()[0], 24.0 + 3.0 * 0.0 + 27.0 - 22.0, places=9)

    def test_bun_offload_matches_expected_sum(self) -> None:
        result = run_deterministic("bun_event_loop", "as_completed", p_uv=1.0, seed=3)
        # 12 + 1.20 + 22 + 0.80
        self.assertAlmostEqual(result.r_ms()[0], 36.0, places=9)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
