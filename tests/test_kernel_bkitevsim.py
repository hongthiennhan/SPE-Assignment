"""Chạy tầng LP hạ tầng trên kernel **thật** (`vendor/bkitevsim`) — T2 + tích hợp.

Khác `test_infra_lps.py` (kernel `heapq` tự viết chỉ để kiểm logic), file này kiểm phần
phiên dịch `Emission → Event`: gán id số cho LP, ghép `Event.payload` ngược lại LP thuần,
và chạy trên `SequentialEventDispatcher` thật.

Không sửa gì trong `vendor/bkitevsim`; mọi điều chỉnh nằm ở `spe_sim/core/kernel.py`.
"""

from __future__ import annotations

import unittest

from spe_sim.core import kernel
from spe_sim.core.config import LP_DISPATCHER
from spe_sim.core.events import (
    Admitted,
    Branch,
    Emission,
    Outcome,
    OutcomeNotice,
    SubReq,
    SubResp,
)
from spe_sim.core.rng import SeedStreams
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.auth_cluster import AuthClusterLP
from spe_sim.lp.infra.network_link import NetworkLinkLP
from spe_sim.lp.infra.sinks import SINK_COMPLETED, build_sinks
from spe_sim.lp.infra.source import SourceLP
from tests.test_infra_lps import _PassthroughDispatcher, _notices


class _GhostLP:
    """LP cố tình phát event tới tên không tồn tại (để test thông điệp lỗi)."""

    name = "ghost"

    def start(self) -> list[Emission]:
        # `initialize()` của adapter gọi `start()` ⇒ lỗi bắn ra ngay khi dispatcher chạy.
        return [Emission(timestamp=0.0, destination="khong_ton_tai", payload=SubReq(0, Branch.DB))]

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        return []


class _IdleLP:
    name = "idle"

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        return []


@unittest.skipUnless(kernel.is_available(), kernel.unavailable_reason() or "")
class TestKernelBindings(unittest.TestCase):
    def test_vendor_path_exists(self) -> None:
        self.assertTrue(kernel.vendor_path().is_dir(), kernel.vendor_path())

    def test_event_type_uses_event_types_names(self) -> None:
        self.assertEqual(kernel.event_type_for(SubReq(0, Branch.AUTH)), "SUB_REQ")
        self.assertEqual(
            kernel.event_type_for(
                SubResp(0, Branch.AUTH, t_sent=0.0, t_service_start=0.0, t_service_end=0.0)
            ),
            "SUB_RESP",
        )
        self.assertEqual(kernel.event_type_for(Admitted(0, 0.0)), "Admitted")

    def test_ladder_bucket_config_is_safe_for_t_sim(self) -> None:
        from spe_sim.core.config import T_SIM_S

        span = kernel.LADDER_NUM_BUCKETS * kernel.LADDER_BUCKET_WIDTH
        self.assertGreater(span, T_SIM_S + kernel.END_TIME_PADDING_S)

    def test_kernel_rejects_bucket_config_that_wraps(self) -> None:
        with self.assertRaises(ValueError):
            kernel.SequentialKernel(end_time=100.0, bucket_width=1.0, num_buckets=64)


@unittest.skipUnless(kernel.is_available(), kernel.unavailable_reason() or "")
class TestSequentialKernel(unittest.TestCase):
    def _build(self, *, seed: int = 31, lam: float = 100.0, t_sim: float = 20.0):
        streams = SeedStreams(seed)
        source = SourceLP(lam, streams.stream("source"), t_sim)
        admission = AdmissionControlLP("blocking_baseline", "gather")
        link = NetworkLinkLP(streams.stream("network"))
        auth = AuthClusterLP(streams.stream("lp", "auth"))
        sinks = build_sinks()
        dispatcher = _PassthroughDispatcher(admission)

        kern = kernel.SequentialKernel(end_time=t_sim + kernel.END_TIME_PADDING_S)
        kern.add(source, admission, link, auth, dispatcher, *sinks.values())
        return kern, source, admission, link, sinks

    def test_end_to_end_on_real_kernel(self) -> None:
        kern, source, admission, link, sinks = self._build()
        stats = kern.run()
        notices = _notices(sinks)

        self.assertGreater(stats["events_processed"], 0)
        self.assertEqual(admission.n_in_system, 0)  # ΔN = 0
        self.assertEqual(admission.admitted + admission.dropped, source.emitted)
        self.assertEqual(len(notices), admission.admitted)
        self.assertEqual(link.sent, admission.admitted)
        self.assertEqual(link.returned, admission.admitted)
        self.assertEqual({n.request_id for n in notices}, set(range(admission.admitted)))

    def test_mean_response_time_matches_spec_branch(self) -> None:
        kern, _, _, _, sinks = self._build(seed=32)
        kern.run()
        rs = [(n.t_complete - n.t_arrive) * 1000.0 for n in _notices(sinks)]
        mean_r = sum(rs) / len(rs)
        self.assertAlmostEqual(mean_r, 12.0, delta=0.8)  # Auth 10 ms + 2 ms mạng

    def test_events_processed_in_chronological_order(self) -> None:
        kern, _, _, _, sinks = self._build(seed=33)
        kern.run()
        notices = _notices(sinks)
        completes = [n.t_complete for n in notices]
        self.assertEqual(completes, sorted(completes))
        self.assertLessEqual(kern.current_time, kern.end_time)
        for notice in notices:
            self.assertGreaterEqual(notice.t_complete, notice.t_dispatch)
            self.assertGreaterEqual(notice.t_dispatch, notice.t_arrive)

    def test_lp_ids_are_assigned_in_add_order(self) -> None:
        kern, source, admission, _, _ = self._build()
        ids = kern.lp_ids
        self.assertEqual(ids["source"], 0)
        self.assertEqual(ids["admission"], 1)
        self.assertEqual(ids["dispatcher"], 4)
        self.assertEqual(len(set(ids.values())), len(ids))
        self.assertEqual(kern.adapters["source"].inner, source)
        self.assertEqual(kern.adapters["admission"].inner, admission)

    def test_unknown_destination_raises_with_hint(self) -> None:
        kern = kernel.SequentialKernel(end_time=5.0)
        kern.add(_GhostLP(), _IdleLP())
        with self.assertRaises(KeyError):
            kern.run()

    def test_duplicate_name_rejected(self) -> None:
        kern = kernel.SequentialKernel(end_time=5.0)
        kern.add(_IdleLP())
        with self.assertRaises(ValueError):
            kern.add(_IdleLP())

    def test_payload_round_trips_unchanged(self) -> None:
        """`Event.payload` là `Any` ⇒ LP nhận đúng object Python, không bị serialize."""
        seen: list[object] = []

        class _Recorder:
            name = "recorder"

            def handle_event(self, payload: object, now: float) -> list[Emission]:
                seen.append(payload)
                return []

        kern = kernel.SequentialKernel(end_time=5.0)
        kern.add(_Recorder())
        kern._adapters["recorder"].emit(  # noqa: SLF001 - cần đăng ký trước khi run()
            Emission(timestamp=0.5, destination="recorder", payload=SubReq(7, Branch.DB))
        )
        kern.run()
        self.assertEqual(len(seen), 1)
        self.assertIsInstance(seen[0], SubReq)
        self.assertEqual(seen[0].request_id, 7)
        self.assertIs(seen[0].branch, Branch.DB)

    def test_timeout_padding_keeps_all_outcomes(self) -> None:
        """Bug #2: với `end_time = T_sim` dispatcher bỏ event cuối; đệm 1 s thì không."""
        kern, _, admission, _, sinks = self._build(seed=34, lam=150.0)
        kern.run()
        self.assertEqual(admission.n_in_system, 0)
        self.assertEqual(
            sum(n.outcome is Outcome.COMPLETED for n in _notices(sinks)), admission.admitted
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
