"""Test tầng LP hạ tầng (T6–T11) — KHÔNG cần `bkitevsim`.

Vì mỗi LP chỉ trả về `Emission`, ta chạy được cả chuỗi bằng một vòng lặp `heapq`
~20 dòng (`_Kernel` bên dưới). Đây là bằng chứng cho milestone M2 (infra end-to-end
với dispatcher passthrough) mà không phụ thuộc kernel của nhóm.
"""

from __future__ import annotations

import heapq
import random
import unittest
from collections import Counter

from spe_sim.core.config import (
    ADMISSION_CAPACITY_K,
    LP_ADMISSION,
    LP_DISPATCHER,
    LP_NETWORK_LINK,
    MS,
    NET_LINK_ONE_WAY_S,
)
from spe_sim.core.events import (
    Admitted,
    Branch,
    Emission,
    JobDone,
    Outcome,
    OutcomeNotice,
    ReqArrival,
    SubReq,
    SubResp,
)
from spe_sim.core.rng import SeedStreams
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.auth_cluster import AuthClusterLP
from spe_sim.lp.infra.catalog_service import CatalogServiceLP
from spe_sim.lp.infra.db_pool import DbPoolLP
from spe_sim.lp.infra.metrics import MetricsRecorder
from spe_sim.lp.infra.network_link import NetworkLinkLP
from spe_sim.lp.infra.sinks import (
    SINK_COMPLETED,
    SINK_DROPPED,
    SINK_TIMEOUT,
    SinkLP,
    all_notices,
    build_sinks,
)
from spe_sim.lp.infra.source import SourceLP

# --------------------------------------------------------------------------------------
# Kernel tối giản cho test — KHÔNG phải kernel của sản phẩm (T12+ sẽ dùng `bkitevsim`)
# --------------------------------------------------------------------------------------
class _Kernel:
    def __init__(self) -> None:
        self.lps: dict[str, object] = {}
        self._queue: list[tuple[float, int, Emission]] = []
        self._seq = 0
        self.now = 0.0
        self.processed = 0

    def add(self, *lps: object) -> "_Kernel":
        for lp in lps:
            self.lps[lp.name] = lp
        return self

    def push(self, emission: Emission) -> None:
        if emission.timestamp < self.now - 1e-12:
            raise AssertionError(
                f"event đi ngược thời gian: {emission.timestamp} < {self.now}"
            )
        self._seq += 1
        heapq.heappush(self._queue, (emission.timestamp, self._seq, emission))

    def run(self, until: float | None = None) -> None:
        while self._queue:
            t, _, emission = self._queue[0]
            if until is not None and t > until:
                return
            heapq.heappop(self._queue)
            self.now = t
            self.processed += 1
            lp = self.lps[emission.destination]
            for out in lp.handle_event(emission.payload, t):  # type: ignore[attr-defined]
                self.push(out)


def _notices(sinks) -> list[OutcomeNotice]:
    return all_notices(sinks.values())


def _arrival(request_id: int, t: float) -> ReqArrival:
    return ReqArrival(request_id=request_id, t_arrive=t)


def _notice(request_id: int, outcome: Outcome, t_arrive: float, t_complete: float, n: int = 1):
    return OutcomeNotice(
        request_id=request_id,
        outcome=outcome,
        t_arrive=t_arrive,
        t_dispatch=t_arrive,
        t_complete=t_complete,
        n_in_system=n,
        runtime_model="blocking_baseline",
        join_mode="gather",
    )


# --------------------------------------------------------------------------------------
# T6 — SourceLP
# --------------------------------------------------------------------------------------
class TestSourceLP(unittest.TestCase):
    def test_ids_sequential_and_interarrival_mean(self) -> None:
        rng = SeedStreams(1).stream("source")
        source = SourceLP(lam=200.0, rng=rng, t_sim=100.0)
        times: list[float] = []
        ids: list[int] = []
        while (t := source.peek()) is not None:
            emission = source.take()
            times.append(emission.timestamp)
            ids.append(emission.payload.request_id)
        self.assertEqual(ids, list(range(len(ids))))
        self.assertEqual(emission.destination, LP_ADMISSION)
        gaps = [b - a for a, b in zip(times, times[1:])]
        mean_gap = sum(gaps) / len(gaps)
        self.assertAlmostEqual(mean_gap, 1.0 / 200.0, delta=0.25 / 200.0)

    def test_stops_at_t_sim(self) -> None:
        source = SourceLP(lam=100.0, rng=random.Random(0), t_sim=1.0)
        last = None
        while (t := source.peek()) is not None:
            last = t
            source.take()
        self.assertIsNotNone(last)
        self.assertLessEqual(last, 1.0)

    def test_same_seed_same_arrivals(self) -> None:
        def run() -> list[float]:
            src = SourceLP(lam=150.0, rng=SeedStreams(42).stream("source"), t_sim=5.0)
            out = []
            while (t := src.peek()) is not None:
                out.append(src.take().payload.t_arrive)
            return out

        self.assertEqual(run(), run())

    def test_rejects_bad_parameters(self) -> None:
        with self.assertRaises(ValueError):
            SourceLP(lam=0.0, rng=random.Random(0), t_sim=1.0)
        with self.assertRaises(ValueError):
            SourceLP(lam=10.0, rng=random.Random(0), t_sim=0.0)
        empty = SourceLP(lam=10.0, rng=random.Random(0), t_sim=1e-12)
        with self.assertRaises(LookupError):
            empty.take()


# --------------------------------------------------------------------------------------
# T7 — AdmissionControlLP
# --------------------------------------------------------------------------------------
class TestAdmissionControlLP(unittest.TestCase):
    def _lp(self, k: int = 2) -> AdmissionControlLP:
        return AdmissionControlLP("blocking_baseline", "gather", capacity_k=k)

    def test_admits_below_capacity_and_rejects_at_capacity(self) -> None:
        lp = self._lp(k=2)
        first = lp.handle_event(_arrival(0, 0.0), 0.0)
        self.assertIsInstance(first[0].payload, Admitted)
        self.assertEqual(first[0].destination, LP_DISPATCHER)
        self.assertEqual(lp.n_in_system, 1)

        second = lp.handle_event(_arrival(1, 0.1), 0.1)
        self.assertIsInstance(second[0].payload, Admitted)
        self.assertEqual(lp.n_in_system, 2)

        third = lp.handle_event(_arrival(2, 0.2), 0.2)

        self.assertEqual(third[0].destination, SINK_DROPPED)
        dropped = third[0].payload
        self.assertIs(third[0].payload.outcome, Outcome.DROPPED)
        self.assertEqual(dropped.n_in_system, 2)
        self.assertEqual(dropped.reason, "admission_full")
        self.assertEqual(lp.dropped, 1)
        self.assertEqual(lp.admitted, 2)

    def test_never_exceeds_capacity(self) -> None:
        lp = self._lp(k=ADMISSION_CAPACITY_K)
        for i in range(ADMISSION_CAPACITY_K * 5):
            lp.handle_event(_arrival(i, float(i)), float(i))
            self.assertLessEqual(lp.n_in_system, ADMISSION_CAPACITY_K)
        self.assertEqual(lp.n_in_system, ADMISSION_CAPACITY_K)
        self.assertEqual(lp.dropped, ADMISSION_CAPACITY_K * 4)

    def test_outcome_notice_releases_slot(self) -> None:
        lp = self._lp(k=1)
        lp.handle_event(_arrival(0, 0.0), 0.0)
        self.assertEqual(lp.handle_event(_arrival(1, 0.001), 0.001)[0].destination, SINK_DROPPED)
        lp.handle_event(_notice(0, Outcome.COMPLETED, 0.0, 0.02), 0.02)
        self.assertEqual(lp.n_in_system, 0)
        admitted = lp.handle_event(_arrival(2, 0.03), 0.03)
        self.assertIsInstance(admitted[0].payload, Admitted)

    def test_double_release_raises(self) -> None:
        lp = self._lp(k=1)
        lp.handle_event(_arrival(0, 0.0), 0.0)
        notice = _notice(0, Outcome.COMPLETED, 0.0, 0.01)
        lp.handle_event(notice, 0.01)
        with self.assertRaises(ValueError):
            lp.handle_event(notice, 0.02)

    def test_wrong_payload_type_raises(self) -> None:
        with self.assertRaises(TypeError):
            self._lp().handle_event(SubReq(0, Branch.AUTH), 0.0)


# --------------------------------------------------------------------------------------
# T8 — NetworkLinkLP
# --------------------------------------------------------------------------------------
class TestNetworkLinkLP(unittest.TestCase):
    def test_request_leg_goes_to_correct_service(self) -> None:
        link = NetworkLinkLP(random.Random(7))
        out = link.handle_event(
            SubReq(request_id=5, branch=Branch.CATALOG, t_sent=1.0), 1.0
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].destination, "catalog_service")
        self.assertGreater(out[0].timestamp, 1.0)
        self.assertEqual(link.sent, 1)

    def test_response_leg_returns_to_dispatcher(self) -> None:
        link = NetworkLinkLP(random.Random(7))
        resp = SubResp(
            request_id=5,
            branch=Branch.AUTH,
            t_sent=1.0,
            t_service_start=1.001,
            t_service_end=1.011,
        )
        out = link.handle_event(resp, 1.011)
        self.assertEqual(out[0].destination, LP_DISPATCHER)
        self.assertGreater(out[0].timestamp, 1.011)
        self.assertEqual(link.returned, 1)

    def test_round_trip_mean_is_two_ms(self) -> None:
        link = NetworkLinkLP(SeedStreams(3).stream("network"))
        delays = []
        for i in range(4000):
            out = link.handle_event(SubReq(i, Branch.AUTH, t_sent=0.0), 0.0)
            delays.append(out[0].timestamp)
        mean_leg = sum(delays) / len(delays)
        self.assertAlmostEqual(mean_leg, NET_LINK_ONE_WAY_S, delta=0.05 * NET_LINK_ONE_WAY_S)
        self.assertAlmostEqual(link.mean_round_trip_s, 2.0 * MS)


# --------------------------------------------------------------------------------------
# T9 — 3 downstream service LP
# --------------------------------------------------------------------------------------
class TestServiceLPs(unittest.TestCase):
    def test_auth_uses_eight_channels_then_queues(self) -> None:
        auth = AuthClusterLP(SeedStreams(1).stream("lp", "auth"))
        for i in range(8):
            auth.handle_event(SubReq(i, Branch.AUTH, t_sent=0.0), 0.0)
        self.assertEqual(auth.core.busy, 8)
        self.assertEqual(auth.core.waiting, 0)
        auth.handle_event(SubReq(99, Branch.AUTH, t_sent=0.0), 0.0)
        self.assertEqual(auth.core.busy, 8)
        self.assertEqual(auth.core.waiting, 1)

    def test_catalog_mean_service_time(self) -> None:
        catalog = CatalogServiceLP(SeedStreams(2).stream("lp", "catalog"))
        rng = SeedStreams(2).stream("lp", "catalog")
        samples = []
        for i in range(2000):
            now = float(i)
            # Trạm mới cho mỗi mẫu ⇒ luôn còn kênh trống, không phụ thuộc JobDone.
            catalog = CatalogServiceLP(rng)
            out = catalog.handle_event(SubReq(i, Branch.CATALOG, t_sent=now), now)
            self.assertEqual(len(out), 1)
            emission = out[0]
            self.assertIsInstance(emission.payload, JobDone)
            self.assertEqual(emission.destination, catalog.name)
            samples.append(emission.timestamp - now)
        mean_service = sum(samples) / len(samples)
        self.assertAlmostEqual(mean_service, 20.0 * MS, delta=1.0 * MS)

    def test_db_pool_never_queues_and_rejects_beyond_n_db(self) -> None:
        pool = DbPoolLP(SeedStreams(3).stream("lp", "db"), n_db=10)
        for i in range(10):
            out = pool.handle_event(SubReq(i, Branch.DB, t_sent=0.0), 0.0)
            self.assertIsInstance(out[0].payload, JobDone)
        self.assertEqual(pool.core.waiting, 0)
        self.assertEqual(pool.core.busy, 10)

        rejected = pool.handle_event(SubReq(10, Branch.DB, t_sent=0.0), 0.0)
        resp = rejected[0].payload
        self.assertIsInstance(resp, SubResp)
        self.assertFalse(resp.ok)
        self.assertEqual(resp.reason, "no_connection")
        self.assertEqual(rejected[0].destination, LP_NETWORK_LINK)
        self.assertEqual(pool.core.waiting, 0)

    def test_channel_freed_and_queue_fifo(self) -> None:
        auth = AuthClusterLP(SeedStreams(4).stream("lp", "auth"), channels=1)
        first = auth.handle_event(SubReq(0, Branch.AUTH, t_sent=0.0), 0.0)[0]
        second = auth.handle_event(SubReq(1, Branch.AUTH, t_sent=0.0), 0.001)
        self.assertEqual(second, [])  # xếp hàng, chưa phát gì
        self.assertEqual(auth.core.waiting, 1)

        out = auth.handle_event(first.payload, first.timestamp)
        payloads = [e.payload for e in out]
        self.assertIsInstance(payloads[0], SubResp)
        self.assertEqual(payloads[0].request_id, 0)
        self.assertTrue(payloads[0].ok)
        self.assertAlmostEqual(payloads[0].t_service_end, first.timestamp)
        self.assertIsInstance(payloads[1], JobDone)
        self.assertEqual(payloads[1].request_id, 1)
        self.assertEqual(auth.core.waiting, 0)
        self.assertEqual(auth.core.busy, 1)

    def test_job_done_for_unknown_request_raises(self) -> None:
        auth = AuthClusterLP(SeedStreams(5).stream("lp", "auth"))
        with self.assertRaises(ValueError):
            auth.handle_event(JobDone(999, Branch.AUTH, 0.0, 0.0), 0.01)


# --------------------------------------------------------------------------------------
# T10 — Sinks
# --------------------------------------------------------------------------------------
class TestSinks(unittest.TestCase):
    def test_three_sinks_partition_outcomes(self) -> None:
        sinks = build_sinks()
        self.assertEqual(set(sinks), {SINK_COMPLETED, SINK_TIMEOUT, SINK_DROPPED})
        sinks[SINK_COMPLETED].handle_event(_notice(0, Outcome.COMPLETED, 0.0, 0.01), 0.01)
        sinks[SINK_TIMEOUT].handle_event(_notice(1, Outcome.TIMEOUT, 0.0, 0.3), 0.3)
        sinks[SINK_TIMEOUT].handle_event(_notice(2, Outcome.DB_DROP, 0.0, 0.3), 0.3)
        sinks[SINK_DROPPED].handle_event(_notice(3, Outcome.DROPPED, 0.0, 0.0), 0.0)
        counts = Counter(n.outcome for n in _notices(sinks))
        self.assertEqual(
            counts,
            {Outcome.COMPLETED: 1, Outcome.TIMEOUT: 1, Outcome.DB_DROP: 1, Outcome.DROPPED: 1},
        )
        self.assertEqual(len(_notices(sinks)), 4)

    def test_wrong_outcome_for_sink_raises(self) -> None:
        sink = SinkLP(SINK_COMPLETED, frozenset({Outcome.COMPLETED}))
        with self.assertRaises(ValueError):
            sink.handle_event(_notice(0, Outcome.TIMEOUT, 0.0, 0.3), 0.3)

    def test_records_match_schema(self) -> None:
        sink = SinkLP(SINK_COMPLETED)
        sink.handle_event(_notice(0, Outcome.COMPLETED, 1.0, 1.013), 1.013)
        records = sink.records(
            seed=7, runtime_model="blocking_baseline", join_mode="gather", lam=150.0
        )
        row = records[0].to_row()
        self.assertEqual(row["r_ms"], "13.000")
        self.assertEqual(row["outcome"], "completed")
        self.assertEqual(row["seed"], "7")


# --------------------------------------------------------------------------------------
# T11 — MetricsRecorder
# --------------------------------------------------------------------------------------
class TestMetricsRecorder(unittest.TestCase):
    def test_known_values(self) -> None:
        m = MetricsRecorder(t_warmup=10.0, t_end=20.0)
        m.record(_notice(0, Outcome.COMPLETED, 10.0, 10.010))  # r = 10 ms
        m.record(_notice(1, Outcome.COMPLETED, 10.0, 10.030))  # r = 30 ms
        m.record(_notice(2, Outcome.TIMEOUT, 10.0, 10.300))
        m.record(_notice(3, Outcome.COMPLETED, 5.0, 5.010))  # warm-up: bị loại
        s = m.summary()
        self.assertEqual(s["n_total"], 4)
        self.assertEqual(s["n_window"], 3)
        self.assertEqual(s["n_completed"], 2)
        self.assertEqual(s["n_timeout"], 1)
        self.assertAlmostEqual(s["e_r_ms"], 20.0)
        self.assertAlmostEqual(s["x_completed_rps"], 2 / 10.0)
        self.assertAlmostEqual(s["p_loss"], 1 / 3)
        self.assertAlmostEqual(s["e_n_little"], 0.2 * 0.020)
        self.assertAlmostEqual(s["p95_ms"], 29.0)  # linear interpolation giữa 10 và 30

    def test_empty_window(self) -> None:
        s = MetricsRecorder(0.0, 10.0).summary()
        self.assertEqual(s["n_window"], 0)
        self.assertIsNone(s["e_r_ms"])
        self.assertIsNone(s["p95_ms"])
        self.assertIsNone(s["p_loss"])

    def test_rejects_bad_window(self) -> None:
        with self.assertRaises(ValueError):
            MetricsRecorder(10.0, 10.0)


# --------------------------------------------------------------------------------------
# M2 — Infra end-to-end với dispatcher passthrough (không có SLA, không có join)
# --------------------------------------------------------------------------------------
class _PassthroughDispatcher:
    """Dispatcher giả cho test hạ tầng: 1 nhánh Auth, không SLA, không join."""

    name = "dispatcher"

    def __init__(self, admission: AdmissionControlLP) -> None:
        self._admission = admission
        self._t_arrive: dict[int, float] = {}
        self._t_dispatch: dict[int, float] = {}

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, Admitted):
            self._t_arrive[payload.request_id] = payload.t_arrive
            self._t_dispatch[payload.request_id] = now
            return [
                Emission(
                    now,
                    LP_NETWORK_LINK,
                    SubReq(payload.request_id, Branch.AUTH, t_sent=now),
                )
            ]
        if isinstance(payload, SubResp):
            notice = OutcomeNotice(
                request_id=payload.request_id,
                outcome=Outcome.COMPLETED if payload.ok else Outcome.DB_DROP,
                t_arrive=self._t_arrive[payload.request_id],
                t_dispatch=self._t_dispatch[payload.request_id],
                t_complete=now,
                n_in_system=self._admission.n_in_system,
                runtime_model="blocking_baseline",
                join_mode="gather",
            )
            return [
                Emission(now, SINK_COMPLETED, notice),
                Emission(now, LP_ADMISSION, notice),
            ]
        raise TypeError(type(payload).__name__)


def _run_auth_only(seed: int, *, lam: float = 100.0, t_sim: float = 20.0):
    streams = SeedStreams(seed)
    source = SourceLP(lam, streams.stream("source"), t_sim)
    admission = AdmissionControlLP("blocking_baseline", "gather")
    link = NetworkLinkLP(streams.stream("network"))
    auth = AuthClusterLP(streams.stream("lp", "auth"))
    sinks = build_sinks()
    dispatcher = _PassthroughDispatcher(admission)

    kernel = _Kernel().add(source, admission, link, auth, dispatcher, *sinks.values())
    while (t := source.peek()) is not None:
        kernel.push(source.take())
    kernel.run()
    return kernel, source, admission, link, auth, sinks


class TestInfraEndToEnd(unittest.TestCase):
    def test_delta_n_zero_and_one_response_per_request(self) -> None:
        _, source, admission, link, _, sinks = _run_auth_only(seed=11)
        notices = _notices(sinks)
        self.assertEqual(admission.n_in_system, 0)
        self.assertEqual(admission.admitted + admission.dropped, source.emitted)
        self.assertEqual(len(notices), admission.admitted)
        self.assertEqual(link.sent, admission.admitted)
        self.assertEqual(link.returned, admission.admitted)
        self.assertEqual({n.request_id for n in notices}, set(range(admission.admitted)))

    def test_mean_response_time_is_about_12ms(self) -> None:
        _, _, _, _, _, sinks = _run_auth_only(seed=12)
        rs = [(n.t_complete - n.t_arrive) * 1000.0 for n in _notices(sinks)]
        mean_r = sum(rs) / len(rs)
        # Auth 10 ms + 2 ms khứ hồi mạng (spec §3.3).
        # Lưu ý: cả 3 thành phần đều là biến mũ ⇒ KHÔNG có cận dưới dương cho r,
        # chỉ kỳ vọng mới so được với 12 ms.
        self.assertAlmostEqual(mean_r, 12.0, delta=0.8)
        self.assertGreater(min(rs), 0.0)

    def test_deterministic_with_same_seed(self) -> None:
        def rows(seed: int):
            _, _, _, _, _, sinks = _run_auth_only(seed=seed)
            return [
                (n.request_id, n.t_arrive, n.t_complete, n.outcome.value)
                for n in _notices(sinks)
            ]

        self.assertEqual(rows(21), rows(21))
        self.assertNotEqual(rows(21), rows(22))

    def test_event_time_never_goes_backwards(self) -> None:
        kernel, *_ = _run_auth_only(seed=13)
        self.assertGreater(kernel.processed, 0)

    def test_timestamps_are_monotonic_in_notices(self) -> None:
        _, _, _, _, _, sinks = _run_auth_only(seed=14)
        notices = _notices(sinks)
        completes = [n.t_complete for n in notices]
        self.assertEqual(completes, sorted(completes))
        for n in notices:
            self.assertGreaterEqual(n.t_complete, n.t_dispatch)
            self.assertGreaterEqual(n.t_dispatch, n.t_arrive)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
