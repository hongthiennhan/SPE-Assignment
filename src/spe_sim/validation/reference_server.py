"""Bản cài đặt **tham chiếu độc lập** của 6 hàng đợi đơn — dùng để đối chứng chéo (A2).

Chuyển gần nguyên từ `XX-004679A-sim/bkitevsim/models/single_queues.py` (repo hàng đợi đơn):
`ArrivalEvent` (coroutine sinh Poisson bằng `advance_time()`), `Server` (LP thuần sự kiện:
`arrival()/schedule()/beginService()/endService()`), `ProcessingEvent`, `QueueStats`.

Khác bản gốc đúng ba điểm, không đổi logic hàng đợi:
1. Seed dùng `SeedStreams(seed).stream("arrival" | "service")` thay cho
   `random.Random(1_000_003·seed + k)` (C3) — cùng quy ước với phần còn lại của `spe_sim`.
2. `bkitevsim` được nạp qua `spe_sim.core.kernel` (thêm `vendor/` vào `sys.path`).
3. Trả cùng bộ khoá với `spe_sim.core.analytics` (`L, Lq, W, Wq, X, P_block, U`).

File này **được phép** import `bkitevsim` trực tiếp vì nó không phải LP của mạng — nó là một
mô phỏng thứ hai, viết theo phong cách khác, để bắt lỗi mà `StationCore` có thể che giấu.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Any

from spe_sim.core import kernel as _kernel
from spe_sim.core.rng import SeedStreams

_kernel._bind()  # đưa vendor/bkitevsim vào sys.path và kiểm đủ thành phần

from bkitevsim.core import BkSysCoEventLoop, BkSysCoLogicalProcess, Event  # noqa: E402

__all__ = ["run_reference"]

ARRIVAL = "ARRIVAL"
END_SERVICE = "END_SERVICE"
ARRIVAL_LP_ID = 0
SERVER_LP_ID = 1

#: `LadderQueue` có 1024 bucket: chọn bucket_width = T_sim/256 ⇒ chỉ dùng 1/4 vòng.
LADDER_RING_FRACTION = 256


class Customer:
    __slots__ = ("cid", "t_arrive", "t_start", "t_done")

    def __init__(self, cid: int, t_arrive: float) -> None:
        self.cid = cid
        self.t_arrive = t_arrive
        self.t_start: float | None = None
        self.t_done: float | None = None


class ProcessingEvent:
    """Ghi nhận ARRIVAL, BLOCKED, BEGIN, END (tuỳ chọn lưu vết)."""

    def __init__(self, trace: bool = False) -> None:
        self.trace = trace
        self.log: list[tuple[float, str, int]] = []
        self.counts = {"ARRIVAL": 0, "BLOCKED": 0, "BEGIN": 0, "END": 0}

    def create(self, kind: str, t: float, customer: Customer) -> None:
        self.counts[kind] += 1
        if self.trace:
            self.log.append((t, kind, customer.cid))


class QueueStats:
    """Thống kê có trọng số thời gian, chỉ cộng sau warm-up."""

    def __init__(self, warmup: float) -> None:
        self.warmup = warmup
        self.last_t = 0.0
        self.area_n = 0.0
        self.area_busy = 0.0
        self.area_q = 0.0
        self.arrivals = 0
        self.blocked = 0
        self.completed = 0
        self.resp_sum = 0.0
        self.wait_sum = 0.0

    def advance(self, now: float, n: int, busy: int) -> None:
        if now > self.warmup:
            dt = now - max(self.last_t, self.warmup)
            self.area_n += n * dt
            self.area_busy += busy * dt
            self.area_q += (n - busy) * dt
        self.last_t = now


class Server(BkSysCoLogicalProcess):
    """Trạm FCFS nhiều kênh, sức chứa K tuỳ chọn (LP thuần sự kiện)."""

    def __init__(self, lp_id, mean_servicetime, maxServer=1, capacity=None, rng=None,
                 stats=None, processing=None):
        super().__init__(lp_id)
        self.mean_servicetime = mean_servicetime
        self.maxServer = maxServer  # math.inf cho M/M/∞
        self.capacity = capacity  # None = vô hạn
        self.rng = rng
        self.stats = stats
        self.processing = processing or ProcessingEvent()
        self.queue: deque[Customer] = deque()
        self.busy = 0
        self.error: Exception | None = None

    @property
    def now(self) -> float:
        return self.local_virt_time

    @property
    def n_in_system(self) -> int:
        return self.busy + len(self.queue)

    def _touch(self) -> None:
        self.stats.advance(self.now, self.n_in_system, self.busy)

    async def coroutine_run(self) -> None:
        self.run()

    def handle_event(self, event: Any) -> None:
        # BkSysCoEventLoop chỉ log lỗi ở đây; giữ lỗi đầu tiên để run_reference() báo to.
        try:
            if event.event_type == ARRIVAL:
                self.arrival(event.payload)
            elif event.event_type == END_SERVICE:
                self.endService(event.payload)
            else:
                super().handle_event(event)
        except Exception as exc:
            if self.error is None:
                self.error = exc
            raise

    def arrival(self, customer: Customer) -> None:
        now = self.now
        self._touch()
        counted = customer.t_arrive >= self.stats.warmup
        if counted:
            self.stats.arrivals += 1
        self.processing.create("ARRIVAL", now, customer)
        if self.capacity is not None and self.n_in_system >= self.capacity:
            if counted:
                self.stats.blocked += 1
            self.processing.create("BLOCKED", now, customer)
            return
        self.queue.append(customer)
        self.schedule()

    def schedule(self) -> None:
        while self.queue and self.busy < self.maxServer:
            self.beginService(self.queue.popleft())

    def run(self) -> None:
        self.schedule()

    def beginService(self, customer: Customer) -> None:
        self.busy += 1
        customer.t_start = self.now
        self.processing.create("BEGIN", self.now, customer)
        st = self.rng.expovariate(1.0 / self.mean_servicetime)
        self.send_event(Event(timestamp=self.now + st, sender_id=self.lp_id,
                              receiver_id=self.lp_id, event_type=END_SERVICE, payload=customer))

    def endService(self, customer: Customer) -> None:
        self._touch()
        self.busy -= 1
        customer.t_done = self.now
        self.processing.create("END", self.now, customer)
        if customer.t_arrive >= self.stats.warmup:
            self.stats.completed += 1
            self.stats.resp_sum += customer.t_done - customer.t_arrive
            self.stats.wait_sum += customer.t_start - customer.t_arrive
        self.schedule()


class ArrivalEvent(BkSysCoLogicalProcess):
    """Nguồn Poisson: khoảng đến mũ, tiến thời gian ảo bằng `advance_time()`."""

    def __init__(self, lp_id, mean_interarrivaltime, server_lp_id, rng, end_time):
        super().__init__(lp_id)
        self.mean_interarrivaltime = mean_interarrivaltime
        self.server_lp_id = server_lp_id
        self.rng = rng
        self.end_time = end_time
        self.generated = 0

    async def coroutine_run(self) -> None:
        rate = 1.0 / self.mean_interarrivaltime
        loop = self._event_loop
        while True:
            dt = self.rng.expovariate(rate)
            if loop.current_time + dt > self.end_time:
                return
            await self.advance_time(dt)
            now = loop.current_time
            self.generated += 1
            self.send_event(Event(timestamp=now, sender_id=self.lp_id,
                                  receiver_id=self.server_lp_id, event_type=ARRIVAL,
                                  payload=Customer(self.generated, now)))

    def handle_event(self, event: Any) -> None:
        pass  # nguồn không nhận gì


#: Kendall → (số kênh, sức chứa) theo tham số (c, K).
def _shape(kendall: str, c: int | None, capacity: int | None) -> tuple[float, int | None]:
    if kendall == "M/M/1":
        return 1, None
    if kendall == "M/M/1/K":
        return 1, capacity
    if kendall == "M/M/inf":
        return math.inf, None
    if kendall == "M/M/n/n":
        return c, c  # type: ignore[return-value]
    if kendall == "M/M/c":
        return c, None  # type: ignore[return-value]
    if kendall == "M/M/c/K":
        return c, capacity  # type: ignore[return-value]
    raise ValueError(f"kendall không hợp lệ: {kendall!r}")


def run_reference(
    kendall: str,
    lam: float,
    mu: float,
    *,
    c: int | None = None,
    capacity: int | None = None,
    t_sim: float,
    t_warmup: float,
    seed: int,
    trace: bool = False,
) -> dict[str, Any]:
    """Chạy bản tham chiếu; cùng chữ ký và cùng khoá kết quả với `run_station`."""
    servers, cap = _shape(kendall, c, capacity)
    loop = BkSysCoEventLoop(end_time=t_sim, bucket_width=t_sim / LADDER_RING_FRACTION)
    stats = QueueStats(t_warmup)
    processing = ProcessingEvent(trace)
    streams = SeedStreams(seed)
    server = Server(SERVER_LP_ID, 1.0 / mu, servers, cap, streams.stream("service"), stats, processing)
    source = ArrivalEvent(ARRIVAL_LP_ID, 1.0 / lam, SERVER_LP_ID, streams.stream("arrival"), t_sim)
    for lp in (source, server):
        loop.register_lp(lp)
        lp.set_event_loop(loop)
    loop.run()
    if server.error is not None:
        raise RuntimeError(f"{kendall}: server LP lỗi") from server.error
    stats.advance(t_sim, server.n_in_system, server.busy)
    span = t_sim - t_warmup
    n_arr = max(stats.arrivals, 1)
    out: dict[str, Any] = {
        "L": stats.area_n / span,
        "Lq": stats.area_q / span,
        "W": stats.resp_sum / max(stats.completed, 1),
        "Wq": stats.wait_sum / max(stats.completed, 1),
        "X": stats.completed / span,
        "P_block": stats.blocked / n_arr,
        "U": (stats.area_busy / span / servers) if math.isfinite(servers) else None,
        "arrivals": float(stats.arrivals),
    }
    if trace:
        out["trace"] = processing.log
    return out
