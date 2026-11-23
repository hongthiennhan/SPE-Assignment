"""Six single-queue models (Kendall notation) on the BKsyspeEvsim core.

Built from ``BkSysCoLogicalProcess`` LPs running on ``BkSysCoEventLoop`` and
following the Queuing System Reference Design:
  * ArrivalEvent    - coroutine LP generating Poisson arrivals
                      (mean_interarrivaltime) with Virtual Time advance_time()
  * Server          - LP with mean_servicetime, maxServer, capacity K;
                      arrival() / schedule() / run(), beginService() / endService().
                      Driven purely by events (ARRIVAL, END_SERVICE) via handle_event().
  * ProcessingEvent - create() emits notifications / trace entries

Models: M/M/1, M/M/1/K, M/M/inf, M/M/n/n (Erlang-B), M/M/c, M/M/c/K.
Units: seconds and req/s everywhere (convert to ms only for reports).
"""
import math
import random
from collections import deque

from bkitevsim.core import Event, BkSysCoEventLoop, BkSysCoLogicalProcess

# Event types exchanged between the LPs
ARRIVAL = "ARRIVAL"
END_SERVICE = "END_SERVICE"

ARRIVAL_LP_ID = 0
SERVER_LP_ID = 1

# bkitevsim.queues.LadderQueue maps timestamps to buckets with
# ``int(t / bucket_width) % num_buckets`` (num_buckets = 1024): events at
# t >= 1024 * bucket_width wrap around and come out of order.  We size the
# bucket so the horizon uses only 1/4 of the ring (4x headroom for service
# completions scheduled after T_sim).
LADDER_RING_FRACTION = 256


class Customer:
    __slots__ = ("cid", "t_arrive", "t_start", "t_done")

    def __init__(self, cid, t_arrive):
        self.cid = cid
        self.t_arrive = t_arrive
        self.t_start = None
        self.t_done = None


class ProcessingEvent:
    """Notification / log sink for ARRIVAL, BLOCKED, BEGIN, END events."""

    def __init__(self, trace=False):
        self.trace = trace
        self.log = []
        self.counts = {"ARRIVAL": 0, "BLOCKED": 0, "BEGIN": 0, "END": 0}

    def create(self, kind, t, customer):
        self.counts[kind] += 1
        if self.trace:
            self.log.append((t, kind, customer.cid))


class QueueStats:
    """Time-weighted statistics collected after the warm-up period."""

    def __init__(self, warmup):
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

    def advance(self, now, n, busy):
        if now > self.warmup:
            dt = now - max(self.last_t, self.warmup)
            self.area_n += n * dt
            self.area_busy += busy * dt
            self.area_q += (n - busy) * dt
        self.last_t = now


class Server(BkSysCoLogicalProcess):
    """Multi-server FCFS station with optional finite capacity K (event-driven LP)."""

    def __init__(self, lp_id, mean_servicetime, maxServer=1, capacity=None,
                 rng=None, stats=None, processing=None):
        super().__init__(lp_id)
        self.mean_servicetime = mean_servicetime
        self.maxServer = maxServer          # math.inf for M/M/inf
        self.capacity = capacity            # None = infinite room
        self.rng = rng or random.Random(0)
        self.stats = stats
        self.processing = processing or ProcessingEvent()
        self.queue = deque()
        self.busy = 0
        self.error = None                   # handle_event errors are re-raised by the model

    @property
    def now(self):
        return self.local_virt_time         # set to event.timestamp by the event loop

    @property
    def n_in_system(self):
        return self.busy + len(self.queue)

    def _touch(self):
        self.stats.advance(self.now, self.n_in_system, self.busy)

    # --- LP interface ---------------------------------------------------
    async def coroutine_run(self):
        """Server is purely event-driven: all work happens in handle_event()."""
        self.run()

    def handle_event(self, event):
        # BkSysCoEventLoop only logs exceptions raised here; keep the first
        # one so SingleQueueModel.run() can fail loudly.
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

    # --- system operators ---------------------------------------------
    def arrival(self, customer):
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

    def schedule(self):
        while self.queue and self.busy < self.maxServer:
            self.beginService(self.queue.popleft())

    def run(self):
        """(Re)start service of waiting customers."""
        self.schedule()

    # --- process operators --------------------------------------------
    def beginService(self, customer):
        self.busy += 1
        customer.t_start = self.now
        self.processing.create("BEGIN", self.now, customer)
        st = self.rng.expovariate(1.0 / self.mean_servicetime)
        self.send_event(Event(timestamp=self.now + st, sender_id=self.lp_id,
                              receiver_id=self.lp_id, event_type=END_SERVICE,
                              payload=customer))

    def endService(self, customer):
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
    """Poisson source: exponential inter-arrival times, Virtual Time advance_time()."""

    def __init__(self, lp_id, mean_interarrivaltime, server_lp_id, rng, end_time):
        super().__init__(lp_id)
        self.mean_interarrivaltime = mean_interarrivaltime
        self.server_lp_id = server_lp_id
        self.rng = rng
        self.end_time = end_time
        self.generated = 0

    async def coroutine_run(self):
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

    def handle_event(self, event):
        pass  # the source receives nothing


class SingleQueueModel:
    """Base class: lam [req/s], mu [req/s per server], c servers, K capacity."""

    kendall = "?"

    def __init__(self, lam, mu, c=1, K=None):
        self.lam, self.mu, self.c, self.K = lam, mu, c, K

    def run(self, T_sim, T_warmup, seed, trace=False):
        loop = BkSysCoEventLoop(end_time=T_sim, bucket_width=T_sim / LADDER_RING_FRACTION)
        stats = QueueStats(T_warmup)
        processing = ProcessingEvent(trace)
        # independent, reproducible streams per seed
        rng_arr = random.Random(1_000_003 * seed + 1)
        rng_srv = random.Random(1_000_003 * seed + 2)
        server = Server(SERVER_LP_ID, 1.0 / self.mu, self.c, self.K, rng_srv, stats, processing)
        source = ArrivalEvent(ARRIVAL_LP_ID, 1.0 / self.lam, SERVER_LP_ID, rng_arr, T_sim)
        for lp in (source, server):
            loop.register_lp(lp)
            lp.set_event_loop(loop)
        loop_stats = loop.run()
        if server.error is not None:
            raise RuntimeError(f"{self.kendall}: server LP failed") from server.error
        stats.advance(T_sim, server.n_in_system, server.busy)
        span = T_sim - T_warmup
        n_arr = max(stats.arrivals, 1)
        res = {
            "L": stats.area_n / span,
            "Lq": stats.area_q / span,
            "W": stats.resp_sum / max(stats.completed, 1),
            "Wq": stats.wait_sum / max(stats.completed, 1),
            "X": stats.completed / span,
            "P_block": stats.blocked / n_arr,
            "U": (stats.area_busy / span / self.c) if math.isfinite(self.c) else None,
            "events": loop_stats["events_processed"],
        }
        if trace:
            res["trace"] = processing.log
        return res


class MM1(SingleQueueModel):
    kendall = "M/M/1"

    def __init__(self, lam, mu):
        super().__init__(lam, mu, 1, None)


class MM1K(SingleQueueModel):
    kendall = "M/M/1/K"

    def __init__(self, lam, mu, K):
        super().__init__(lam, mu, 1, K)


class MMInf(SingleQueueModel):
    kendall = "M/M/inf"

    def __init__(self, lam, mu):
        super().__init__(lam, mu, math.inf, None)


class MMnn(SingleQueueModel):
    kendall = "M/M/n/n"

    def __init__(self, lam, mu, n):
        super().__init__(lam, mu, n, n)


class MMc(SingleQueueModel):
    kendall = "M/M/c"

    def __init__(self, lam, mu, c):
        super().__init__(lam, mu, c, None)


class MMcK(SingleQueueModel):
    kendall = "M/M/c/K"

    def __init__(self, lam, mu, c, K):
        super().__init__(lam, mu, c, K)


MODELS = {m.kendall: m for m in (MM1, MM1K, MMInf, MMnn, MMc, MMcK)}
