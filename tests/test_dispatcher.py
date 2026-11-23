"""Test `RuntimeDispatcherLP` (T12) trên kernel thật, với service **tất định**.

Service thật trong `lp/infra/*` sample thời gian mũ, nên không dùng để khẳng định một con số
chính xác được. Ở đây thay bằng `_FixedService` (độ trễ cố định) và `_ExactLink` (không thêm
độ trễ), nhờ vậy mỗi kết cục là một đẳng thức kiểm được:

    r = ingress + (các nhánh) + callback

Đây là nền để T18 so trace 63.50 / 29.50 / 24.50 ms.
"""

from __future__ import annotations

import unittest

from spe_sim.core.config import LP_DISPATCHER, SERVICE_LP_NAMES, get_profile
from spe_sim.core.events import (
    Branch,
    Emission,
    Outcome,
    ReqArrival,
    SubReq,
    SubResp,
)
from spe_sim.core.kernel import SequentialKernel
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.sinks import (
    SINK_COMPLETED,
    SINK_TIMEOUT,
    build_sinks,
)
from spe_sim.lp.runtime.dispatcher import RuntimeDispatcherLP

MS = 1e-3


class _ExactLink:
    """Network link không trễ: chỉ định tuyến `SUB_REQ` sang đúng service."""

    name = "network_link"

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, SubReq):
            return [
                Emission(
                    timestamp=now,
                    destination=SERVICE_LP_NAMES[payload.branch.value],
                    payload=payload,
                )
            ]
        if isinstance(payload, SubResp):
            return [Emission(timestamp=now, destination=LP_DISPATCHER, payload=payload)]
        raise TypeError(type(payload).__name__)


class _FixedService:
    """Service trả lời sau đúng `delay_s` (hoặc từ chối ngay nếu `ok=False`)."""

    def __init__(
        self,
        name: str,
        delay_s: float,
        *,
        ok: bool = True,
        reason: str | None = "no_connection",
    ) -> None:
        self.name = name
        self.delay_s = delay_s
        self.ok = ok
        self.reason = reason

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if not isinstance(payload, SubReq):
            raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")
        if not self.ok:
            return [
                Emission(
                    timestamp=now,
                    destination=LP_DISPATCHER,
                    payload=SubResp(
                        payload.request_id,
                        payload.branch,
                        t_sent=payload.t_sent,
                        t_service_start=now,
                        t_service_end=now,
                        ok=False,
                        reason=self.reason,
                    ),
                )
            ]
        end = now + self.delay_s
        return [
            Emission(
                timestamp=end,
                destination=LP_DISPATCHER,
                payload=SubResp(
                    payload.request_id,
                    payload.branch,
                    t_sent=payload.t_sent,
                    t_service_start=now,
                    t_service_end=end,
                ),
            )
        ]


def _run(
    runtime_model: str,
    join_mode: str,
    *,
    auth_s: float = 12 * MS,
    catalog_s: float = 22 * MS,
    db_s: float = 27 * MS,
    db_ok: bool = True,
    arrivals: tuple[float, ...] = (0.0,),
    p_pin: float = 0.0,
):
    profile = get_profile(runtime_model, p_pin=p_pin)
    admission = AdmissionControlLP(runtime_model, join_mode)  # type: ignore[arg-type]
    dispatcher = RuntimeDispatcherLP(profile, join_mode)  # type: ignore[arg-type]
    link = _ExactLink()
    auth = _FixedService("auth_cluster", auth_s)
    catalog = _FixedService("catalog_service", catalog_s)
    db = _FixedService("db_pool", db_s, ok=db_ok)
    sinks = build_sinks()

    kernel = SequentialKernel(end_time=1.0)
    kernel.add(admission, dispatcher, link, auth, catalog, db, *sinks.values())
    for request_id, t in enumerate(arrivals):
        kernel.adapters["admission"].emit(
            Emission(timestamp=t, destination="admission", payload=ReqArrival(request_id, t))
        )
    kernel.run()
    return kernel, admission, dispatcher, sinks


def _only(notices: list) -> object:
    assert len(notices) == 1, notices
    return notices[0]


class TestTraceFormula(unittest.TestCase):
    """r = ingress + tổng/max nhánh + callback, đúng hồ sơ của từng runtime."""

    def test_blocking_gather_is_63_50_ms(self) -> None:
        _, admission, dispatcher, sinks = _run("blocking_baseline", "gather")
        notice = _only(sinks[SINK_COMPLETED].notices)
        self.assertEqual(notice.outcome, Outcome.COMPLETED)
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 63.5, places=9)
        self.assertEqual(admission.n_in_system, 0)
        self.assertEqual(dispatcher.branch_requests, 3)

    def test_loom_gather_matches_29_50_ms(self) -> None:
        _, _, _, sinks = _run("spring_loom_vt", "gather")
        notice = _only(sinks[SINK_COMPLETED].notices)
        # 1.5 (ingress) + max(12, 22, 27) + 1.0 (callback)
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 29.5, places=9)

    def test_loom_quorum_matches_24_50_ms(self) -> None:
        _, _, _, sinks = _run("spring_loom_vt", "as_completed")
        notice = _only(sinks[SINK_COMPLETED].notices)
        # 1.5 + max(12, 22) + 1.0  (bỏ DB)
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 24.5, places=9)

    def test_node_gather(self) -> None:
        _, _, _, sinks = _run("nodejs_event_loop", "gather")
        notice = _only(sinks[SINK_COMPLETED].notices)
        # 1.92 + 27 + 1.28
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 30.2, places=9)

    def test_bun_quorum(self) -> None:
        _, _, _, sinks = _run("bun_event_loop", "as_completed")
        notice = _only(sinks[SINK_COMPLETED].notices)
        # 1.20 + 22 + 0.80
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 24.0, places=9)

    def test_three_branch_requests_per_admitted(self) -> None:
        arrivals = (0.0, 0.05, 0.1, 0.15, 0.2)
        _, admission, dispatcher, sinks = _run("bun_event_loop", "gather", arrivals=arrivals)
        self.assertEqual(admission.admitted, 5)
        self.assertEqual(dispatcher.branch_requests, 3 * admission.admitted)
        self.assertEqual(len(sinks[SINK_COMPLETED].notices), 5)


class TestJoinModes(unittest.TestCase):
    def test_db_rejection_is_db_drop_in_gather(self) -> None:
        _, _, _, sinks = _run("bun_event_loop", "gather", db_ok=False)
        notice = _only(sinks[SINK_TIMEOUT].notices)
        self.assertEqual(notice.outcome, Outcome.DB_DROP)
        self.assertEqual(notice.reason, "db_full")

    def test_db_rejection_does_not_matter_in_quorum(self) -> None:
        _, _, _, sinks = _run("bun_event_loop", "as_completed", db_ok=False)
        notice = _only(sinks[SINK_COMPLETED].notices)
        self.assertEqual(notice.outcome, Outcome.COMPLETED)
        # 1.20 + max(12, 22) + 0.80 — không chờ DB
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 24.0, places=9)

    def test_quorum_third_branch_is_ignored_when_late(self) -> None:
        _, _, dispatcher, sinks = _run(
            "bun_event_loop", "as_completed", db_s=200 * MS
        )
        notice = _only(sinks[SINK_COMPLETED].notices)
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 24.0, places=9)
        self.assertEqual(dispatcher.late_responses, 1)


class TestSla(unittest.TestCase):
    def test_slow_branch_times_out_at_deadline(self) -> None:
        _, admission, dispatcher, sinks = _run("bun_event_loop", "gather", db_s=400 * MS)
        notice = _only(sinks[SINK_TIMEOUT].notices)
        self.assertEqual(notice.outcome, Outcome.TIMEOUT)
        self.assertAlmostEqual((notice.t_complete - notice.t_arrive) / MS, 300.0, places=9)
        self.assertEqual(dispatcher.timed_out, 1)
        self.assertEqual(admission.n_in_system, 0)

    def test_late_response_after_timeout_is_ignored(self) -> None:
        _, _, dispatcher, sinks = _run("bun_event_loop", "gather", db_s=400 * MS)
        self.assertEqual(dispatcher.late_responses, 1)
        self.assertEqual(len(sinks[SINK_TIMEOUT].notices), 1)
        self.assertEqual(len(sinks[SINK_COMPLETED].notices), 0)

    def test_no_duplicate_notice_for_timed_out_request(self) -> None:
        _, _, _, sinks = _run("blocking_baseline", "gather", db_s=500 * MS)
        total = sum(len(s.notices) for s in sinks.values())
        self.assertEqual(total, 1)


class TestSlotPool(unittest.TestCase):
    def test_single_channel_serializes_ingress(self) -> None:
        """Node/Bun có 1 kênh: request thứ hai chỉ được dispatch sau khi request đầu xong ingress."""
        _, _, dispatcher, sinks = _run(
            "bun_event_loop", "gather", arrivals=(0.0, 0.0)
        )
        notices = sorted(sinks[SINK_COMPLETED].notices, key=lambda n: n.request_id)
        self.assertEqual(len(notices), 2)
        self.assertEqual(dispatcher.admitted, 2)
        # Không thể chạy ingress song song trên 1 kênh.
        second = notices[1]
        first = notices[0]
        self.assertGreaterEqual(
            second.t_dispatch - first.t_dispatch, dispatcher.ingress_s - 1e-12
        )
        # Nhưng tổng thời gian vẫn dưới SLA.
        for notice in notices:
            self.assertLessEqual(notice.t_complete - notice.t_arrive, 0.3)

    def test_blocking_holds_slot_through_io(self) -> None:
        """Blocking giữ OS thread suốt 63.5 ms ⇒ 13 request cùng lúc phải xếp hàng."""
        arrivals = tuple(0.0 for _ in range(13))
        _, admission, dispatcher, sinks = _run(
            "blocking_baseline", "gather", arrivals=arrivals
        )
        self.assertEqual(admission.admitted, 13)
        notices = sorted(sinks[SINK_COMPLETED].notices, key=lambda n: n.request_id)
        dispatches = sorted(n.t_dispatch for n in notices)
        # 12 kênh: 12 request đầu vào gần như đồng thời, request 13 phải chờ ≥ 63.5 ms.
        self.assertAlmostEqual((dispatches[-1] - dispatches[0]) / MS, 63.5, places=6)

    def test_snapshot_reports_branch_ratio(self) -> None:
        _, _, dispatcher, _ = _run("spring_loom_vt", "gather", arrivals=(0.0, 0.1))
        snap = dispatcher.snapshot()
        self.assertEqual(snap["branch_ratio"], 3.0)
        self.assertEqual(snap["in_system_at_end"], 0)
        self.assertEqual(snap["completed"], 2)


class TestFullStack(unittest.TestCase):
    """Ghép **tất cả LP thật** lần đầu: source → admission → link → 3 service → dispatcher → sink."""

    def _run_full(self, runtime_model: str, join_mode: str, *, lam: float, t_sim: float, seed: int):
        from spe_sim.core.rng import SeedStreams
        from spe_sim.lp.infra.auth_cluster import AuthClusterLP
        from spe_sim.lp.infra.catalog_service import CatalogServiceLP
        from spe_sim.lp.infra.db_pool import DbPoolLP
        from spe_sim.lp.infra.network_link import NetworkLinkLP
        from spe_sim.lp.infra.source import SourceLP

        streams = SeedStreams(seed)
        source = SourceLP(lam, streams.stream("source"), t_sim)
        admission = AdmissionControlLP(runtime_model, join_mode)
        link = NetworkLinkLP(streams.stream("network"))
        auth = AuthClusterLP(streams.stream("lp", "auth"))
        catalog = CatalogServiceLP(streams.stream("lp", "catalog"))
        db = DbPoolLP(streams.stream("lp", "db"))
        dispatcher = RuntimeDispatcherLP(get_profile(runtime_model), join_mode)
        sinks = build_sinks()

        kernel = SequentialKernel(end_time=t_sim + 1.0)
        kernel.add(
            source, admission, dispatcher, link, auth, catalog, db, *sinks.values()
        )
        kernel.run()
        return source, admission, dispatcher, sinks

    def _assert_invariants(self, source, admission, dispatcher, sinks) -> list:
        notices = sorted(
            (n for sink in sinks.values() for n in sink.notices),
            key=lambda n: n.request_id,
        )
        self.assertEqual(admission.n_in_system, 0)  # ΔN = 0
        self.assertEqual(admission.admitted + admission.dropped, source.emitted)
        self.assertEqual(dispatcher.admitted, admission.admitted)
        self.assertEqual(len(notices), admission.admitted)
        self.assertEqual({n.request_id for n in notices}, set(range(admission.admitted)))
        self.assertEqual(dispatcher.branch_requests, 3 * admission.admitted)
        self.assertEqual(dispatcher.n_in_system, 0)
        for notice in notices:
            self.assertGreaterEqual(notice.t_complete, notice.t_dispatch)
            self.assertGreaterEqual(notice.t_dispatch, notice.t_arrive)
            self.assertLessEqual(notice.t_complete, source.t_sim + 1.0)
        return notices

    def test_blocking_gather_all_real_lps(self) -> None:
        source, admission, dispatcher, sinks = self._run_full(
            "blocking_baseline", "gather", lam=50.0, t_sim=5.0, seed=101
        )
        notices = self._assert_invariants(source, admission, dispatcher, sinks)
        completed = sum(n.outcome is Outcome.COMPLETED for n in notices)
        self.assertGreater(completed / len(notices), 0.95)
        r = [(n.t_complete - n.t_arrive) / MS for n in notices]
        # Kỳ vọng giải tích 63.5 ms khi không nghẽn (λ = 50 ≪ trần 188.98 req/s).
        self.assertAlmostEqual(sum(r) / len(r), 63.5, delta=3.0)

    def test_loom_quorum_all_real_lps(self) -> None:
        source, admission, dispatcher, sinks = self._run_full(
            "spring_loom_vt", "as_completed", lam=100.0, t_sim=5.0, seed=202
        )
        notices = self._assert_invariants(source, admission, dispatcher, sinks)
        r = [(n.t_complete - n.t_arrive) / MS for n in notices]
        # Lưu ý quan trọng: 24.50 ms là trace **tất định** (max(12, 22) = 22).
        # Với service mũ, kỳ vọng là 1.5 + E[max(R_auth, R_catalog)] + 1.0
        # = 1.5 + 16.13 (K9) + 1.0 ≈ 18.6 ms ⇒ giá trị đo phải **thấp hơn** 24.5.
        mean_r = sum(r) / len(r)
        self.assertAlmostEqual(mean_r, 18.63, delta=1.5)
        self.assertLess(mean_r, 24.5)
        self.assertEqual(admission.dropped, 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
