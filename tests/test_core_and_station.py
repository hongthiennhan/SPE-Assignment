"""Test tầng contract + engine trạm — chạy được khi CHƯA có `bkitevsim`.

Chạy:  python -m unittest discover -s tests -t . -v
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from spe_sim.core import analytics  # noqa: E402
from spe_sim.core.config import (  # noqa: E402
    ADMISSION_CAPACITY_K,
    BRANCH_MEAN_S,
    LAMBDA_LEVELS,
    PROFILES,
    ScenarioConfig,
    T_TIMEOUT_S,
    get_profile,
)
from spe_sim.core.events import BRANCHES, EVENT_TYPES, Outcome  # noqa: E402
from spe_sim.core.kernel import KernelUnavailable, is_available, unavailable_reason  # noqa: E402
from spe_sim.core.records import (  # noqa: E402
    CSV_COLUMNS,
    RequestRecord,
    read_csv,
    validate_columns,
    write_csv,
)
from spe_sim.core.rng import (  # noqa: E402
    SeedStreams,
    sample_bernoulli,
    sample_exponential,
    sample_interarrival,
    sample_percentile,
)
from spe_sim.lp.infra.station import (  # noqa: E402
    REASON_CAPACITY,
    REASON_NO_CONNECTION,
    InfiniteServerCore,
    StationCore,
)


class TestConfig(unittest.TestCase):
    def test_spec_constants(self) -> None:
        self.assertAlmostEqual(T_TIMEOUT_S, 0.300, places=12)
        self.assertEqual(ADMISSION_CAPACITY_K, 64)
        self.assertEqual(len(BRANCHES), 3)
        self.assertEqual(LAMBDA_LEVELS, (50, 100, 150, 200, 250, 300, 350, 400, 450))

    def test_branch_means_are_12_22_27_ms(self) -> None:
        self.assertAlmostEqual(BRANCH_MEAN_S["auth"] * 1000, 12.0, places=9)
        self.assertAlmostEqual(BRANCH_MEAN_S["catalog"] * 1000, 22.0, places=9)
        self.assertAlmostEqual(BRANCH_MEAN_S["db"] * 1000, 27.0, places=9)

    def test_profile_values(self) -> None:
        blocking = PROFILES["blocking_baseline"]
        self.assertEqual(blocking.channels, 12)
        self.assertTrue(blocking.holds_slot_during_io)
        self.assertTrue(blocking.sequential_branches)
        self.assertAlmostEqual(blocking.theoretical_capacity_rps, 188.976, delta=0.01)

        node = PROFILES["nodejs_event_loop"]
        self.assertAlmostEqual(node.cpu_total_s * 1000, 3.2, places=9)
        self.assertAlmostEqual(node.ingress_s * 1000, 1.92, places=9)
        self.assertAlmostEqual(node.callback_s * 1000, 1.28, places=9)
        self.assertAlmostEqual(node.theoretical_capacity_rps, 312.5, places=9)

        bun = PROFILES["bun_event_loop"]
        self.assertAlmostEqual(bun.ingress_s * 1000, 1.20, places=9)
        self.assertAlmostEqual(bun.callback_s * 1000, 0.80, places=9)
        self.assertAlmostEqual(bun.theoretical_capacity_rps, 500.0, places=9)

        loom = PROFILES["spring_loom_vt"]
        self.assertEqual(loom.channels, 4)
        self.assertAlmostEqual(loom.ingress_s * 1000, 1.5, places=9)
        self.assertAlmostEqual(loom.callback_s * 1000, 1.0, places=9)
        self.assertAlmostEqual(loom.theoretical_capacity_rps, 1600.0, places=9)
        self.assertFalse(loom.holds_slot_during_io)

    def test_get_profile_applies_study_factors(self) -> None:
        loom_pin = get_profile("spring_loom_vt", p_pin=0.20)
        self.assertAlmostEqual(loom_pin.theoretical_capacity_rps, 389.59, places=9)
        self.assertGreater(loom_pin.carrier_hold_s, 0.0)

        node_uv = get_profile("nodejs_event_loop", p_uv=0.30)
        self.assertIsNotNone(node_uv.uv_pool)
        self.assertAlmostEqual(node_uv.uv_pool.offload_prob, 0.30, places=12)

    def test_scenario_validation(self) -> None:
        cfg = ScenarioConfig(runtime_model="bun_event_loop", join_mode="gather", lam=150.0, seed=7)
        self.assertAlmostEqual(cfg.steady_state_s, 180.0)

        with self.assertRaises(ValueError):
            ScenarioConfig(runtime_model="bogus", join_mode="gather", lam=150.0, seed=1)
        with self.assertRaises(ValueError):
            ScenarioConfig(runtime_model="bun_event_loop", join_mode="bogus", lam=150.0, seed=1)
        with self.assertRaises(ValueError):
            ScenarioConfig(runtime_model="bun_event_loop", join_mode="gather", lam=150.0, seed=1, n_db=12)
        with self.assertRaises(ValueError):
            ScenarioConfig(runtime_model="bun_event_loop", join_mode="gather", lam=150.0, seed=1, p_pin=0.5)
        with self.assertRaises(ValueError):
            ScenarioConfig(runtime_model="bun_event_loop", join_mode="gather", lam=150.0, seed=1, p_uv=0.4)


class TestEventsAndKernelBoundary(unittest.TestCase):
    def test_event_type_names_frozen_by_team_convention(self) -> None:
        self.assertEqual(
            EVENT_TYPES,
            ("REQ_ARRIVAL", "SUB_REQ", "SUB_RESP", "TIMEOUT_ALARM"),
        )

    def test_outcome_values_match_spec(self) -> None:
        self.assertEqual(
            {o.value for o in Outcome},
            {"completed", "timeout", "db_drop", "dropped"},
        )

    def test_kernel_absence_is_reported_not_hidden(self) -> None:
        # Máy hiện tại chưa có bkitevsim → adapter phải báo rõ, không crash khi import.
        if is_available():
            self.skipTest("bkitevsim đã được cài; test này chỉ kiểm nhánh thiếu kernel")
        reason = unavailable_reason()
        self.assertIsNotNone(reason)
        self.assertIn("bkitevsim", reason)
        from spe_sim.core import kernel

        with self.assertRaises(KernelUnavailable):
            kernel.make_event(timestamp=0.0, source=None, destination=None, payload=None)


class TestRng(unittest.TestCase):
    def test_same_seed_same_stream(self) -> None:
        a = SeedStreams(42).stream("source")
        b = SeedStreams(42).stream("source")
        self.assertEqual([a.random() for _ in range(5)], [b.random() for _ in range(5)])

    def test_streams_are_independent(self) -> None:
        streams = SeedStreams(42)
        self.assertNotEqual(streams.stream("auth").random(), streams.stream("catalog").random())

    def test_exponential_matches_expected_mean(self) -> None:
        rng = SeedStreams(1).stream("test")
        samples = [sample_exponential(rng, 0.010) for _ in range(20000)]
        mean = sum(samples) / len(samples)
        self.assertAlmostEqual(mean, 0.010, delta=0.0006)  # sai số ~6% của 10 ms

    def test_interarrival_rate(self) -> None:
        rng = SeedStreams(2).stream("source")
        samples = [sample_interarrival(rng, 150.0) for _ in range(20000)]
        mean = sum(samples) / len(samples)
        self.assertAlmostEqual(mean, 1.0 / 150.0, delta=0.002)

    def test_bernoulli_and_percentile(self) -> None:
        rng = SeedStreams(3).stream("t")
        hits = sum(sample_bernoulli(rng, 0.20) for _ in range(10000))
        self.assertAlmostEqual(hits / 10000, 0.20, delta=0.02)
        self.assertAlmostEqual(sample_percentile([1.0, 2.0, 3.0, 4.0, 5.0], 0.95), 4.8)


class TestRecords(unittest.TestCase):
    def _record(self, **overrides: object) -> RequestRecord:
        base: dict[str, object] = {
            "seed": 5,
            "runtime_model": "nodejs_event_loop",
            "join_mode": "gather",
            "lam": 150.0,
            "request_id": 1,
            "t_arrive": 1.0,
            "t_dispatch": 1.001,
            "t_complete": 1.0295,
            "outcome": Outcome.COMPLETED,
            "n_in_system": 3,
        }
        base.update(overrides)
        return RequestRecord(**base)  # type: ignore[arg-type]

    def test_r_ms_is_derived_in_ms(self) -> None:
        self.assertAlmostEqual(self._record().r_ms, 29.5, places=6)

    def test_causality_is_enforced(self) -> None:
        with self.assertRaises(ValueError):
            self._record(t_dispatch=0.5)
        with self.assertRaises(ValueError):
            self._record(t_complete=0.5)

    def test_csv_schema_matches_contract_d3(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            write_csv(path, [self._record()])
            validate_columns(path)
            rows = read_csv(path)
        self.assertEqual(tuple(rows[0]), CSV_COLUMNS)
        self.assertEqual(rows[0]["outcome"], "completed")
        self.assertEqual(rows[0]["r_ms"], "29.500")


class TestAnalytics(unittest.TestCase):
    def test_order_statistics_match_spec_38_84_and_16_13(self) -> None:
        means = [BRANCH_MEAN_S["auth"], BRANCH_MEAN_S["catalog"], BRANCH_MEAN_S["db"]]
        e_max_ms = analytics.expected_max(means) * 1000.0
        e_second_ms = analytics.expected_second_of_three(means) * 1000.0

        # spec §3.2/§3.3: 38.84 ms và 16.13 ms (sai lệch tích phân < 0.005 ms)
        self.assertAlmostEqual(e_max_ms, 38.84, delta=0.01)
        self.assertAlmostEqual(e_second_ms, 16.13, delta=0.01)
        self.assertAlmostEqual(e_max_ms, 38.8372, delta=0.005)
        self.assertAlmostEqual(e_second_ms, 16.1328, delta=0.005)

    def test_deterministic_trace_branch_totals(self) -> None:
        # Hình 4 spec §1.5.1: CPU 2.5 ms + max(12, 22, 27) và max(12, 22)
        branches = [12.0, 22.0, 27.0]
        self.assertAlmostEqual(2.5 + max(branches), 29.50, places=9)
        self.assertAlmostEqual(2.5 + sum(branches), 63.50, places=9)  # 2.5 + 12 + 22 + 27
        self.assertAlmostEqual(2.5 + sorted(branches)[1], 24.50, places=9)

    def test_erlang_b_at_db_default_load(self) -> None:
        # λ0 = 150 req/s, 1/µ3 = 25 ms ⇒ a = 3.75; spec: B(24, 3.75) < 1e-8 %
        self.assertLess(analytics.erlang_b(24, 3.75), 1e-9)
        self.assertAlmostEqual(analytics.erlang_b(1, 1.0), 0.5, places=12)
        self.assertAlmostEqual(analytics.erlang_b(2, 1.0), 0.2, places=12)

    def test_mmc_wait_is_negligible_at_lambda_0(self) -> None:
        # Auth M/M/8: a = 150 * 0.010 = 1.5, ρ = 0.1875 ⇒ Wq < 0.01 ms (spec §3.3)
        wq = analytics.mean_wait_mmc(channels=8, service_mean_s=0.010, lam=150.0)
        self.assertLess(wq, 1e-5)
        # Catalog M/M/16: a = 150 * 0.020 = 3.0
        wq2 = analytics.mean_wait_mmc(channels=16, service_mean_s=0.020, lam=150.0)
        self.assertLess(wq2, 1e-5)

    def test_mm1k_closed_form(self) -> None:
        stats = analytics.mean_system_mm1k(lam=2.0, mu=3.0, capacity=1)
        self.assertAlmostEqual(stats["rho"], 2 / 3, places=12)
        self.assertAlmostEqual(stats["p_drop"], 0.4, places=12)
        self.assertAlmostEqual(stats["mean_n"], 0.4, places=12)
        self.assertAlmostEqual(stats["mean_r_s"], 1 / 3, places=12)

    def test_theoretical_capacities(self) -> None:
        caps = analytics.theoretical_capacities()
        self.assertAlmostEqual(caps["blocking_baseline"], 188.98, delta=0.01)
        self.assertAlmostEqual(caps["nodejs_event_loop"], 312.5, places=9)
        self.assertAlmostEqual(caps["bun_event_loop"], 500.0, places=9)
        self.assertAlmostEqual(caps["spring_loom_vt"], 1600.0, places=9)


class TestStationCore(unittest.TestCase):
    def test_mmc_unbounded_queue_and_promotion(self) -> None:
        st = StationCore("auth", channels=8)
        first = st.on_arrival(0.0, service_s=0.010)
        self.assertTrue(first.accepted)
        self.assertAlmostEqual(first.finish_s or -1, 0.010, places=12)

        # 8 kênh đầy → khách thứ 9 phải xếp hàng, chưa có finish time
        for i in range(1, 8):
            st.on_arrival(i * 0.0001, service_s=0.010)
        queued = st.on_arrival(0.001, service_s=0.010)
        self.assertTrue(queued.accepted)
        self.assertTrue(queued.queued)
        self.assertIsNone(queued.finish_s)
        self.assertEqual(st.waiting, 1)

        promoted = st.on_departure(0.010, next_service_s=0.010)
        self.assertIsNotNone(promoted)
        assert promoted is not None
        self.assertAlmostEqual(promoted.finish_s or -1, 0.020, places=12)
        self.assertEqual(st.waiting, 0)

    def test_mmck_rejects_when_buffer_full(self) -> None:
        # M/M/1/K với K = 2: 1 đang phục vụ + 1 chờ, khách thứ 3 bị loại
        st = StationCore("dispatcher", channels=1, capacity=2)
        st.on_arrival(0.0, service_s=1.0)
        st.on_arrival(0.5, service_s=1.0)
        rejected = st.on_arrival(0.6, service_s=1.0)
        self.assertTrue(rejected.rejected)
        self.assertEqual(rejected.reason, REASON_CAPACITY)
        self.assertEqual(st.snapshot(2.0)["rejected_capacity"], 1)

    def test_mmnn_is_erlang_loss(self) -> None:
        st = StationCore("db_pool", channels=2, loss=True)
        self.assertTrue(st.on_arrival(0.0, service_s=0.025).accepted)
        self.assertTrue(st.on_arrival(0.001, service_s=0.025).accepted)
        third = st.on_arrival(0.002, service_s=0.025)
        self.assertTrue(third.rejected)
        self.assertEqual(third.reason, REASON_NO_CONNECTION)

    def test_time_weighted_mean_in_system(self) -> None:
        st = StationCore("q", channels=1, capacity=2)
        st.on_arrival(0.0, service_s=1.0)  # in_system 1 trong [0, 0.5)
        st.on_arrival(0.5, service_s=1.0)  # in_system 2 trong [0.5, 1.0)
        st.on_arrival(0.6, service_s=1.0)  # bị loại, in_system vẫn 2
        st.on_departure(1.0, next_service_s=1.0)  # in_system 1 trong [1.0, 2.0]
        stats = st.snapshot(2.0)
        self.assertAlmostEqual(stats["mean_in_system"], 1.25, places=12)
        self.assertAlmostEqual(stats["accepted"], 2)
        self.assertAlmostEqual(stats["served"], 1)
        self.assertEqual(stats["max_in_system"], 2)

    def test_monotonicity_is_enforced(self) -> None:
        st = StationCore("q", channels=1)
        st.on_arrival(1.0, service_s=0.1)
        with self.assertRaises(ValueError):
            st.on_arrival(0.5, service_s=0.1)

    def test_infinite_server_never_queues(self) -> None:
        link = InfiniteServerCore("net_link")
        for i in range(100):
            decision = link.on_arrival(i * 0.0001, service_s=0.002)
            self.assertTrue(decision.accepted)
            self.assertFalse(decision.queued)
        self.assertEqual(link.in_system, 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
