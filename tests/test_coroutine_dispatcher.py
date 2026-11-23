"""Bản **asyncio** của dispatcher (spec §1.1.2) phải cho cùng kết quả với bản đồng bộ.

Finding F1 (`report.md` §11.3): spec mô tả mô phỏng bằng coroutine + `asyncio.gather` /
`asyncio.as_completed`. Các test dưới đây chứng minh `CoroutineKernel` +
`CoroutineDispatcherLP` không chỉ chạy được mà **trùng khớp tuyệt đối** với
`SequentialKernel` trên cùng bộ LP: trace, SLA, Time Pinning, DB từ chối và bất biến ΔN.
"""

from __future__ import annotations

import unittest

from spe_sim.core.config import MS
from spe_sim.core.events import Outcome
from spe_sim.runner.deterministic import (
    run_deterministic,
    run_deterministic_coroutine,
)

TOLERANCE_MS = 1e-6


def r_ms(result) -> list[float]:
    return [(n.t_complete - n.t_arrive) / MS for n in result.notices]


class TestTraceParity(unittest.TestCase):
    """Cùng kịch bản, cùng con số trên hai engine."""

    CASES = (
        ("blocking_baseline", "gather", 63.50),
        ("spring_loom_vt", "gather", 29.50),
        ("spring_loom_vt", "as_completed", 24.50),
        ("nodejs_event_loop", "gather", 30.20),
        ("bun_event_loop", "as_completed", 24.00),
    )

    def test_trace_matches_both_engines(self) -> None:
        for model, join, expected in self.CASES:
            with self.subTest(model=model, join=join):
                for runner in (run_deterministic, run_deterministic_coroutine):
                    result = runner(model, join)
                    self.assertEqual(len(result.notices), 1)
                    self.assertIs(result.notices[0].outcome, Outcome.COMPLETED)
                    self.assertAlmostEqual(r_ms(result)[0], expected, delta=TOLERANCE_MS)


class TestCoroutineSemantics(unittest.TestCase):
    def test_timeout_is_cancelled_at_sla(self) -> None:
        # Service 400 ms > SLA 300 ms ⇒ chốt timeout đúng 300.00 ms, không phải 400.
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("blocking_baseline", "gather", auth_s=0.4)
            self.assertIs(result.notices[0].outcome, Outcome.TIMEOUT)
            self.assertAlmostEqual(r_ms(result)[0], 300.0, delta=TOLERANCE_MS)

    def test_response_exactly_on_deadline_completes(self) -> None:
        # K11: 1.2 + 298 + 0.8 = 300 ms đúng mốc ⇒ phải completed, không timeout.
        delays = {"auth_s": 0.298, "catalog_s": 0.298, "db_s": 0.298}
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("bun_event_loop", "gather", **delays)
            self.assertIs(result.notices[0].outcome, Outcome.COMPLETED)
            self.assertAlmostEqual(r_ms(result)[0], 300.0, delta=TOLERANCE_MS)

    def test_db_rejection_is_a_drop_not_a_timeout(self) -> None:
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("bun_event_loop", "gather", db_ok=False)
            notice = result.notices[0]
            self.assertIs(notice.outcome, Outcome.DB_DROP)
            self.assertEqual(notice.reason, "db_full")

    def test_quorum_completes_when_db_rejects(self) -> None:
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("bun_event_loop", "as_completed", db_ok=False)
            self.assertIs(result.notices[0].outcome, Outcome.COMPLETED)

    def test_pinning_holds_carrier_in_both_engines(self) -> None:
        arrivals = tuple(i * 0.001 for i in range(8))
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("spring_loom_vt", "gather", p_pin=1.0, arrivals=arrivals)
            values = [round(v, 6) for v in r_ms(result)]
            self.assertEqual(values[:4], [29.5] * 4)
            self.assertGreater(min(values[4:]), 29.5)

    def test_slot_queue_is_fifo(self) -> None:
        # 4 carrier: request 5..8 phải xếp hàng và xong sau, không vượt mặt.
        arrivals = tuple(i * 0.001 for i in range(8))
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("spring_loom_vt", "gather", arrivals=arrivals)
            self.assertEqual([round(v, 6) for v in r_ms(result)], [29.5] * 8)


class TestUvParity(unittest.TestCase):
    """`p_uv` (libuv Worker Pool) trên engine coroutine — trước đây chưa kiểm."""

    def test_uv_offload_matches_between_engines(self) -> None:
        for p_uv in (1.0, 0.5):
            with self.subTest(p_uv=p_uv):
                sync = run_deterministic("bun_event_loop", "gather", p_uv=p_uv, seed=3)
                coro = run_deterministic_coroutine("bun_event_loop", "gather", p_uv=p_uv, seed=3)
                self.assertEqual(r_ms(sync), r_ms(coro))

    def test_uv_offload_adds_pool_delay(self) -> None:
        # p_uv = 1 ⇒ có thêm 12 ms trên pool, nên r lớn hơn rõ rệt so với p_uv = 0.
        without = r_ms(run_deterministic_coroutine("bun_event_loop", "gather"))[0]
        with_uv = r_ms(run_deterministic_coroutine("bun_event_loop", "gather", p_uv=1.0))[0]
        self.assertAlmostEqual(with_uv - without, 12.0, delta=1e-6)

    def test_uv_offload_respects_pool_channels(self) -> None:
        # 4 kênh: 5 job cùng lúc ⇒ job thứ 5 phải chờ 12 ms.
        arrivals = tuple([0.0] * 5)
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner("bun_event_loop", "gather", p_uv=1.0, arrivals=arrivals, seed=1)
            values = sorted(r_ms(result))
            self.assertEqual(len(values), 5)
            self.assertGreater(values[-1] - values[0], 11.0)


class TestCoroutineInvariants(unittest.TestCase):
    def test_mean_in_system_is_time_integral(self) -> None:
        # 4 request cùng lúc, mỗi cái 29.5 ms ⇒ N(t) = 4 trên [0, 29.5ms], E[N] = 4.
        result = run_deterministic_coroutine(
            "spring_loom_vt", "gather", arrivals=(0.0, 0.0, 0.0, 0.0)
        )
        span = max(result.notices[0].t_complete, 0.001)
        self.assertAlmostEqual(result.dispatcher.mean_in_system(span), 4.0, delta=1e-9)

    def test_no_requests_left_behind(self) -> None:
        for runner in (run_deterministic, run_deterministic_coroutine):
            result = runner(
                "bun_event_loop", "gather", arrivals=tuple(i * 0.01 for i in range(20))
            )
            self.assertEqual(result.admission.n_in_system, 0)
            self.assertEqual(len(result.notices), 20)
            self.assertEqual(result.dispatcher.branch_requests, 3 * result.admission.admitted)

    def test_runs_are_reproducible(self) -> None:
        first = run_deterministic_coroutine("spring_loom_vt", "as_completed", p_pin=0.3, seed=5)
        second = run_deterministic_coroutine("spring_loom_vt", "as_completed", p_pin=0.3, seed=5)
        self.assertEqual(r_ms(first), r_ms(second))


if __name__ == "__main__":
    unittest.main()
