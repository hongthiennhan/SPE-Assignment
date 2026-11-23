"""`CoroutineDispatcherLP` — Dispatcher chạy bằng **asyncio** đúng như spec §1.1.2.

Vì sao có file này (finding F1, `report.md` §11.3): spec mô tả mô phỏng dùng
`asyncio.Future` + `await`, fan-out/join bằng `asyncio.gather` (All-Join 3/3),
`asyncio.as_completed` (Quorum 2/3) và hẹn giờ huỷ tác vụ. `RuntimeDispatcherLP` là bản
state-machine đồng bộ (đúng kết quả nhưng khác cơ chế chữ). Bản này hiện thực đúng cơ chế:

    mỗi request là một coroutine
    ├── await slot (Future xếp hàng FIFO)
    ├── await advance_time(ingress)          ← CPU pha 1 trên virtual time
    ├── fan-out 3 nhánh: mỗi nhánh là một asyncio.Future
    ├── gather 3/3   hoặc   as_completed 2/3
    ├── await advance_time(callback)         ← CPU pha 2
    └── chốt kết cục

SLA được hiện thực bằng **cuộc đua** giữa coroutine xử lý và một `Future` do
`TIMEOUT_ALARM` đánh thức: `asyncio.wait({work, timeout}, FIRST_COMPLETED)` — không có
`asyncio.wait_for(timeout=wall_clock)` nào (DEV.md §2).

LP không import `bkitevsim`: kernel tiêm `emit()` và `loop` qua `bind()`; mọi giao tiếp
vẫn qua `Emission`.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Final

from spe_sim.core.config import (
    LP_ADMISSION,
    LP_NETWORK_LINK,
    N_BRANCHES,
    T_TIMEOUT_S,
    JoinMode,
    RuntimeProfile,
)
from spe_sim.core.events import (
    Admitted,
    Branch,
    CpuTimer,
    Emission,
    Outcome,
    OutcomeNotice,
    SubReq,
    SubResp,
    TimeoutAlarm,
    UvDone,
    UvTask,
)
from spe_sim.lp.infra.sinks import SINK_COMPLETED, SINK_TIMEOUT
from spe_sim.lp.runtime.dispatcher import PHASE_CALLBACK, PHASE_INGRESS

__all__ = ["CoroutineDispatcherLP"]

QUORUM_NEEDED: Final[int] = 2

#: Alarm SLA đặt trễ một hạt vô cùng nhỏ (giây ảo) để response đúng mốc deadline — vốn đã
#: nằm trong queue trước — được xử lý trước alarm. Đây là cách hiện thực K11 cho bản
#: coroutine mà không cần cơ chế "deferred" của bản đồng bộ.
ALARM_EPSILON_S: Final[float] = 1e-9


@dataclass
class _Req:
    request_id: int
    t_arrive: float
    t_deadline: float
    t_dispatch: float | None = None
    futures: dict[Branch, asyncio.Future] = field(default_factory=dict)
    successes: int = 0
    failures: int = 0
    cpu_futures: dict[str, asyncio.Future] = field(default_factory=dict)
    db_rejected: bool = False
    pinned: bool = False
    holding_slot: bool = False
    retired: bool = False
    uv_future: asyncio.Future | None = None


class CoroutineDispatcherLP:
    """Dispatcher dạng coroutine; cùng tham số với `RuntimeDispatcherLP`."""

    def __init__(
        self,
        profile: RuntimeProfile,
        join_mode: JoinMode,
        *,
        t_timeout: float = T_TIMEOUT_S,
        sub_request_target: str = LP_NETWORK_LINK,
        pin_probability: float = 0.0,
        uv_probability: float = 0.0,
        rng: random.Random | None = None,
        uv_pool_name: str = "uv_pool",
        name: str = "dispatcher",
    ) -> None:
        if join_mode not in ("gather", "as_completed"):
            raise ValueError(f"join_mode không hợp lệ: {join_mode!r}")
        if t_timeout <= 0.0:
            raise ValueError(f"t_timeout phải > 0, nhận {t_timeout}")
        for label, value in (("pin_probability", pin_probability), ("uv_probability", uv_probability)):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{label} phải trong [0, 1], nhận {value}")

        self.name = name
        self.profile = profile
        self.join_mode = join_mode
        self.t_timeout = t_timeout
        self.sub_request_target = sub_request_target
        self.pin_probability = pin_probability
        self.uv_probability = uv_probability
        self.uv_pool_name = uv_pool_name
        self._rng = rng

        self.channels = profile.channels
        self.ingress_s = profile.ingress_s
        self.callback_s = profile.callback_s
        self.sequential = profile.sequential_branches
        self.holds_slot_during_io = profile.holds_slot_during_io
        self.needs_all = join_mode == "gather"

        self._loop: Any = None
        self._emit: Callable[[Emission], None] | None = None
        self.lp_id = -1
        self.local_virt_time = 0.0

        self._active: dict[int, _Req] = {}
        self._admitted: list[Admitted] = []
        self._admit_waiter: asyncio.Future | None = None
        self._timeouts: dict[int, asyncio.Future] = {}
        self._busy = 0
        self._slot_waiters: list[asyncio.Future] = []
        self._tasks: set[asyncio.Task] = set()

        # Tích phân N(t) để kiểm Little's Law (E[N] = area / span), giống bản đồng bộ.
        self._area = 0.0
        self._last_t = 0.0

        self.admitted = 0
        self.branch_requests = 0
        self.completed = 0
        self.timed_out = 0
        self.db_dropped = 0
        self.pinned_requests = 0
        self.uv_requests = 0
        self.insufficient = 0

    # ------------------------------------------------------------------ kernel tiêm
    def bind(self, *, lp_id: int, emit: Callable[[Emission], None], loop: Any) -> None:
        self.lp_id = lp_id
        self._emit = emit
        self._loop = loop

    def set_event_loop(self, loop: Any) -> None:  # framework có thể gọi
        self._loop = loop

    def set_local_virt_time(self, value: float) -> None:  # pragma: no cover - duck typing
        self.local_virt_time = value

    def _account(self) -> None:
        now = self._now()
        self._area += len(self._active) * max(0.0, now - self._last_t)
        self._last_t = now

    def mean_in_system(self, span_s: float) -> float:
        """E[N] trên cửa sổ `span_s` (dùng `N(t)` tích lũy, không phải trung bình mẫu)."""
        if span_s <= 0.0:
            return 0.0
        return self._area / span_s

    @property
    def last_time(self) -> float:
        return self._now() if self._loop is not None else self._last_t

    def get_statistics(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "admitted": self.admitted,
            "branch_requests": self.branch_requests,
            "completed": self.completed,
            "timed_out": self.timed_out,
            "db_dropped": self.db_dropped,
            "pinned": self.pinned_requests,
            "uv": self.uv_requests,
        }

    # ------------------------------------------------------------------ tiện ích
    def _now(self) -> float:
        return float(self._loop.current_time)

    def _send(self, emission: Emission) -> None:
        assert self._emit is not None, "CoroutineDispatcherLP chưa được kernel bind()"
        self._emit(emission)

    async def _delay(self, req: _Req, kind: str, delta: float) -> None:
        """Hai pha CPU: chờ `CpuTimer` ở `now + delta` (virtual time, không wall-clock).

        Dùng event thay vì `advance_time` để loop không phụ thuộc trạng thái READY của
        coroutine — cùng cơ chế với `RuntimeDispatcherLP` bản đồng bộ.
        """
        if delta <= 0.0:
            return  # không yield: giữ chuỗi resume ngắn (loop chỉ bơm 3 vòng mỗi batch event)
        future = self._new_future()
        req.cpu_futures[kind] = future
        self._send(
            Emission(
                timestamp=self._now() + delta,
                destination=self.name,
                payload=CpuTimer(req.request_id, kind),
            )
        )
        await future
        req.cpu_futures.pop(kind, None)

    def _new_future(self) -> asyncio.Future:
        return self._loop.create_future()

    # ------------------------------------------------------------------ event vào
    def handle_event(self, payload: object, now: float | None = None) -> list[Any]:
        """Nhận event từ framework; đánh thức Future tương ứng (không trả Emission)."""
        event = payload
        inner = getattr(event, "payload", event)
        if isinstance(inner, Admitted):
            self._on_admitted(inner)
        elif isinstance(inner, SubResp):
            self._on_sub_response(inner)
        elif isinstance(inner, TimeoutAlarm):
            self._on_alarm(inner)
        elif isinstance(inner, UvDone):
            self._on_uv_done(inner)
        elif isinstance(inner, CpuTimer):
            self._on_cpu_timer(inner)
        return []

    def _on_cpu_timer(self, timer: CpuTimer) -> None:
        req = self._active.get(timer.request_id)
        if req is None or req.retired:
            return
        future = req.cpu_futures.get(timer.kind)
        if future is not None and not future.done():
            future.set_result(None)

    def _on_admitted(self, admitted: Admitted) -> None:
        req = _Req(
            request_id=admitted.request_id,
            t_arrive=admitted.t_arrive,
            t_deadline=admitted.t_arrive + self.t_timeout,
        )
        self._account()
        self._active[req.request_id] = req
        self.admitted += 1
        self._admitted.append(admitted)
        if self._admit_waiter is not None and not self._admit_waiter.done():
            self._admit_waiter.set_result(None)

    def _on_sub_response(self, resp: SubResp) -> None:
        req = self._active.get(resp.request_id)
        if req is None or req.retired:
            return
        future = req.futures.get(resp.branch)
        if future is not None and not future.done():
            future.set_result(resp)

    def _on_uv_done(self, done: UvDone) -> None:
        req = self._active.get(done.request_id)
        if req is None or req.retired or req.uv_future is None:
            return
        if not req.uv_future.done():
            req.uv_future.set_result(None)

    def _on_alarm(self, alarm: TimeoutAlarm) -> None:
        req = self._active.get(alarm.request_id)
        if req is None or req.retired:
            return
        if self._join_ready(req):
            return  # response đúng mốc deadline đã đủ ⇒ coi như kịp SLA (K11)
        future = self._timeouts.get(req.request_id)
        if future is not None and not future.done():
            future.set_result(None)

    def _join_ready(self, req: _Req) -> bool:
        if not req.futures:
            return False
        if self.needs_all:
            return all(f.done() for f in req.futures.values())
        ok = sum(
            1 for f in req.futures.values() if f.done() and not f.cancelled() and f.result().ok
        )
        return ok >= QUORUM_NEEDED

    # ------------------------------------------------------------------ vòng chạy
    async def coroutine_run(self) -> None:
        while True:
            if not self._admitted:
                if self._now() >= float(self._loop.end_time):
                    return
                # `wait_for_event` của framework: đặt trạng thái WAITING_EVENT đúng cách,
                # nếu tự await một Future trần thì loop thấy LP luôn READY và quay vô hạn.
                await self._loop.wait_for_event(self.lp_id, "Admitted")
                continue
            admitted = self._admitted.pop(0)
            task = asyncio.ensure_future(self._serve(admitted))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _serve(self, admitted: Admitted) -> None:
        req = self._active[admitted.request_id]
        timeout_fut = self._new_future()
        self._timeouts[req.request_id] = timeout_fut
        # Hẹn giờ SLA bằng event ảo (không dùng wall-clock). Đặt trễ ALARM_EPSILON_S để
        # response đúng mốc deadline (nằm trong queue trước) được xử lý trước alarm.
        self._send(
            Emission(
                timestamp=req.t_deadline + ALARM_EPSILON_S,
                destination=self.name,
                payload=TimeoutAlarm(req.request_id, req.t_deadline),
            )
        )

        work = asyncio.ensure_future(self._pipeline(req))
        try:
            done, _pending = await asyncio.wait(
                {work, timeout_fut}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:  # pragma: no cover - loop bị huỷ cuối mô phỏng
            work.cancel()
            raise

        if work in done:
            try:
                await work
            except asyncio.CancelledError:  # pragma: no cover
                return
            return  # kết cục đã được `_pipeline` chốt

        work.cancel()
        self._release(req)
        self._finish_timeout(req)

    async def _pipeline(self, req: _Req) -> None:
        """Toàn bộ vòng đời một request, chạy song song với các request khác."""
        if self.uv_probability > 0.0 and self._rng is not None and self._rng.random() < self.uv_probability:
            req.uv_future = self._new_future()
            self.uv_requests += 1
            self._send(
                Emission(
                    timestamp=self._now(),
                    destination=self.uv_pool_name,
                    payload=UvTask(request_id=req.request_id, t_submit=self._now()),
                )
            )
            await req.uv_future

        await self._acquire(req)
        req.t_dispatch = self._now()
        await self._delay(req, PHASE_INGRESS, self.ingress_s)

        req.pinned = self.holds_slot_during_io or self._sample_pin()
        if req.pinned:
            self.pinned_requests += 1
        else:
            self._release(req)

        await self._branches(req)

        if not req.holding_slot:
            await self._acquire(req)
        await self._delay(req, PHASE_CALLBACK, self.callback_s)
        self._release(req)
        # Chốt kết cục ngay trong coroutine của request: tránh phụ thuộc số vòng bơm của
        # `BkSysCoEventLoop` sau khi event cuối cùng được xử lý xong.
        self._finish_req(req, self._now())

    async def _branches(self, req: _Req) -> None:
        if self.sequential:
            for branch in Branch:
                resp = await self._ask(req, branch)
                if not resp.ok:
                    req.failures += 1
                    if branch is Branch.DB and resp.reason == "no_connection":
                        req.db_rejected = True
                    return
                req.successes += 1
            return

        futures = [self._ask_future(req, branch) for branch in Branch]
        if self.needs_all:
            responses: list[SubResp] = await asyncio.gather(*futures)
            for resp in responses:
                if resp.ok:
                    req.successes += 1
                else:
                    req.failures += 1
                    if resp.branch is Branch.DB and resp.reason == "no_connection":
                        req.db_rejected = True
            return

        # Quorum 2/3 — đúng `asyncio.as_completed` của spec §1.1.2.
        for pending in asyncio.as_completed(futures):
            resp = await pending
            if resp.ok:
                req.successes += 1
                if req.successes >= QUORUM_NEEDED:
                    return
            else:
                req.failures += 1
                if resp.branch is Branch.DB and resp.reason == "no_connection":
                    req.db_rejected = True

    def _ask_future(self, req: _Req, branch: Branch) -> asyncio.Future:
        future = self._new_future()
        req.futures[branch] = future
        self.branch_requests += 1
        self._send(
            Emission(
                timestamp=self._now(),
                destination=self.sub_request_target,
                payload=SubReq(request_id=req.request_id, branch=branch, t_sent=self._now()),
            )
        )
        return future

    async def _ask(self, req: _Req, branch: Branch) -> SubResp:
        return await self._ask_future(req, branch)

    # ------------------------------------------------------------------ slot pool
    async def _acquire(self, req: _Req) -> None:
        if self._busy < self.channels:
            self._busy += 1
            req.holding_slot = True
            return
        waiter = self._new_future()
        self._slot_waiters.append(waiter)
        try:
            await waiter
        except asyncio.CancelledError:
            if waiter in self._slot_waiters:
                self._slot_waiters.remove(waiter)
            raise
        req.holding_slot = True

    def _release(self, req: _Req) -> None:
        if not req.holding_slot:
            return
        req.holding_slot = False
        self._busy -= 1
        while self._slot_waiters:
            waiter = self._slot_waiters.pop(0)
            if not waiter.done():
                self._busy += 1
                waiter.set_result(None)
                return

    def _sample_pin(self) -> bool:
        if self.pin_probability <= 0.0 or self._rng is None:
            return False
        return self._rng.random() < self.pin_probability

    # ------------------------------------------------------------------ kết cục
    def _finish_req(self, req: _Req, now: float) -> None:
        if req.retired:
            return
        r_s = now - req.t_arrive
        if self.needs_all and req.failures > 0:
            outcome, reason = Outcome.DB_DROP, ("db_full" if req.db_rejected else None)
        elif req.successes < (N_BRANCHES if self.needs_all else QUORUM_NEEDED):
            outcome, reason = Outcome.TIMEOUT, "insufficient_branches"
        elif r_s > self.t_timeout:
            outcome, reason = Outcome.TIMEOUT, None
        else:
            outcome, reason = Outcome.COMPLETED, None
        self._emit_notice(req, outcome, now, reason=reason)

    def _finish_timeout(self, req: _Req) -> None:
        if req.retired:
            return
        self._emit_notice(req, Outcome.TIMEOUT, req.t_deadline)

    def _emit_notice(
        self,
        req: _Req,
        outcome: Outcome,
        t_complete: float,
        *,
        reason: str | None = None,
    ) -> None:
        req.retired = True
        self._account()
        self._active.pop(req.request_id, None)
        self._timeouts.pop(req.request_id, None)
        if outcome is Outcome.COMPLETED:
            self.completed += 1
        elif outcome is Outcome.DB_DROP:
            self.db_dropped += 1
        else:
            self.timed_out += 1

        notice = OutcomeNotice(
            request_id=req.request_id,
            outcome=outcome,
            t_arrive=req.t_arrive,
            t_dispatch=req.t_dispatch if req.t_dispatch is not None else req.t_arrive,
            t_complete=t_complete,
            n_in_system=max(len(self._active), 1),
            runtime_model=self.profile.name,
            join_mode=self.join_mode,
            reason=reason,
        )
        sink = SINK_COMPLETED if outcome is Outcome.COMPLETED else SINK_TIMEOUT
        now = self._now()
        self._send(Emission(timestamp=now, destination=sink, payload=notice))
        self._send(Emission(timestamp=now, destination=LP_ADMISSION, payload=notice))
