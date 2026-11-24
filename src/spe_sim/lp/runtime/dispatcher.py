"""`RuntimeDispatcherLP` — trái tim của mô phỏng: buffer `K`, SLA, fan-out 3 nhánh, join.

Vòng đời request (spec Hình 3, project.md §5):

    ADMITTED → [chờ slot] → CPU ingress → fan-out 3 nhánh → chờ I/O
             → CPU callback (join) → COMPLETED / TIMEOUT / DB_DROP

LP này **thuần logic**, không import `bkitevsim`: `handle_event(payload, now)` trả về
`list[Emission]`, kernel lo phần đặt event (xem `spe_sim/core/kernel.py`).

BỐN QUYẾT ĐỊNH CẦN BIẾT
-----------------------
1. **Kênh (slot)** do `RuntimeProfile.channels` quyết định. Khi `holds_slot_during_io`
   (Java Blocking, Loom bị pinning) request giữ slot suốt pha I/O; ngược lại nhả slot ngay
   sau ingress và **phải xếp hàng lại** để chạy callback — đây chính là hiệu ứng
   Head-of-Line của event loop.
2. **Join**: `gather` = đủ 3/3 (`needs_all`), `as_completed` = 2/3 thành công (Quorum).
   DB từ chối (`ok=False`) → `DB_DROP` chỉ ở chế độ 3/3; ở Quorum 2/3 nó là nhánh hỏng
   và bị bỏ qua, đúng spec §1.3.
3. **SLA**: `r` được chốt ở cuối callback (`r > T_timeout` ⇒ 504). Alarm chỉ là lưới an toàn
   cho request không bao giờ xong; khi nổ đúng mốc deadline nó tự phát lại một lần
   (`TimeoutAlarm.deferred`) để mọi response cùng timestamp được xử lý trước — đây là
   cách hiện thực đề xuất K11.
4. **Huỷ nhánh**: response về sau khi request đã chốt (timeout / quorum đủ 2) bị **bỏ qua**
   (`late_responses`), không double count. Service vẫn chạy xong job của nó (đúng vật lý).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Final

from spe_sim.core.config import (
    ADMISSION_CAPACITY_K,
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
from spe_sim.lp.infra.sinks import SINK_COMPLETED, SINK_DROPPED, SINK_TIMEOUT

__all__ = ["RuntimeDispatcherLP", "QUORUM_NEEDED"]

#: Số nhánh thành công cần có ở chế độ Quorum 2/3 (spec §1.3).
QUORUM_NEEDED: Final[int] = 2

PHASE_INGRESS: Final[str] = "ingress"
PHASE_CALLBACK: Final[str] = "callback"

REASON_DB_FULL: Final[str] = "db_full"


def sample_cpu_phases(
    ingress_s: float, callback_s: float, cpu_rng: random.Random | None
) -> tuple[float, float]:
    """Thời gian hai pha CPU của **một** request (spec §1.5.1, §3.1).

    - `cpu_rng is None` (Deterministic Workload): trả nguyên `(ingress_s, callback_s)`.
    - Ngược lại: sample tổng `S ~ Exp(mean = ingress_s + callback_s)` **một lần** rồi chia
      đúng tỉ lệ của profile (60/40 với Node.js/Bun, 1.5/1.0 với Loom, 100/0 với Blocking).
      Nhờ đó kỳ vọng mỗi pha giữ nguyên và tổng CPU một request là biến mũ, khớp giả định
      M/M/1/K và M/M/c/K của Bảng 1.

    Dùng chung cho `RuntimeDispatcherLP` và `CoroutineDispatcherLP` để hai engine rút số
    giống hệt nhau (cùng thời điểm: lúc admit request).
    """
    if cpu_rng is None:
        return ingress_s, callback_s
    factor = cpu_rng.expovariate(1.0)  # Exp(1) × mean = Exp(mean)
    return ingress_s * factor, callback_s * factor


@dataclass(slots=True)
class _Active:
    """Trạng thái một request đang trong tầng điều phối."""

    request_id: int
    t_arrive: float
    t_deadline: float
    t_dispatch: float | None = None
    sent: list[Branch] = field(default_factory=list)
    successes: int = 0
    failures: int = 0
    db_rejected: bool = False
    joined: bool = False
    retired: bool = False
    forced_reason: str | None = None
    holding_slot: bool = False
    pending_phase: str | None = None
    pinned: bool = False
    uv_offloaded: bool = False
    ingress_s: float = 0.0
    callback_s: float = 0.0

    @property
    def branches_left(self) -> int:
        return N_BRANCHES - len(self.sent)


class RuntimeDispatcherLP:
    """Điều phối request theo một `RuntimeProfile` và một `join_mode`."""

    name: Final[str] = "dispatcher"

    def __init__(
        self,
        profile: RuntimeProfile,
        join_mode: JoinMode,
        *,
        capacity_k: int = ADMISSION_CAPACITY_K,
        t_timeout: float = T_TIMEOUT_S,
        sub_request_target: str = LP_NETWORK_LINK,
        pin_probability: float = 0.0,
        uv_probability: float = 0.0,
        rng: random.Random | None = None,
        cpu_rng: random.Random | None = None,
        uv_pool_name: str = "uv_pool",
        name: str = "dispatcher",
    ) -> None:
        if join_mode not in ("gather", "as_completed"):
            raise ValueError(f"join_mode không hợp lệ: {join_mode!r}")
        for label, value in (("pin_probability", pin_probability), ("uv_probability", uv_probability)):
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{label} phải trong [0, 1], nhận {value}")
            if value > 0.0 and rng is None:
                raise ValueError(f"{label} > 0 cần `rng` để tái lập (DEV.md §3)")
        # cpu_rng: None ⇒ CPU tất định; có ⇒ CPU ~ Exp (A4). Tách khỏi `rng` để thêm/bớt
        # pinning, offload không làm xê dịch dãy số CPU (và ngược lại).
        self._cpu_rng = cpu_rng
        if capacity_k < 1:
            raise ValueError(f"K phải ≥ 1, nhận {capacity_k}")
        if t_timeout <= 0.0:
            raise ValueError(f"t_timeout phải > 0, nhận {t_timeout}")
        if profile.channels < 1:
            raise ValueError(f"profile.channels phải ≥ 1, nhận {profile.channels}")

        self.name = name  # type: ignore[misc]
        self.profile = profile
        self.join_mode = join_mode
        self.capacity_k = capacity_k
        self.t_timeout = t_timeout
        self.sub_request_target = sub_request_target
        self.pin_probability = pin_probability
        self.uv_probability = uv_probability
        self.uv_pool_name = uv_pool_name
        self._rng = rng

        self.channels = profile.channels
        self.ingress_s = profile.ingress_s
        self.callback_s = profile.callback_s
        self.sequential_branches = profile.sequential_branches
        self.holds_slot_during_io = profile.holds_slot_during_io
        self.needs_all = join_mode == "gather"

        self._active: dict[int, _Active] = {}
        self._waiting: list[tuple[int, str]] = []  # FIFO (request_id, phase)
        self._busy = 0

        # Thống kê
        self.admitted = 0
        self.branch_requests = 0
        self.late_responses = 0
        self.completed = 0
        self.timed_out = 0
        self.db_dropped = 0
        self.pinned_requests = 0
        self.uv_requests = 0

        # Tích phân N(t) theo thời gian để kiểm Little's Law (spec §1.5.2).
        self._area_in_system = 0.0
        self._last_t = 0.0

    # ------------------------------------------------------------------ trạng thái
    @property
    def n_in_system(self) -> int:
        return len(self._active)

    @property
    def busy_channels(self) -> int:
        return self._busy

    @property
    def queued(self) -> int:
        return sum(1 for rid, _ in self._waiting if rid in self._active)

    def _observe(self, now: float) -> None:
        """Cộng diện tích `N(t)` giữa hai mốc — dùng cho `mean_in_system`."""
        if now < self._last_t:
            raise ValueError(f"{self.name}: thời gian lùi ({now} < {self._last_t})")
        self._area_in_system += len(self._active) * (now - self._last_t)
        self._last_t = now

    @property
    def last_time(self) -> float:
        """Mốc thời gian của event gần nhất đã xử lý (đuôi drain sau `T_sim`)."""
        return self._last_t

    def mean_in_system(self, t_end: float) -> float:
        """`E[N]` trung bình theo thời gian trên `[0, t_end]`."""
        if t_end <= 0.0:
            raise ValueError(f"t_end phải > 0, nhận {t_end}")
        self._observe(t_end)
        return self._area_in_system / t_end

    # ------------------------------------------------------------------ sự kiện
    def handle_event(self, payload: object, now: float) -> list[Emission]:
        self._observe(now)
        if isinstance(payload, Admitted):
            return self._on_admitted(payload, now)
        if isinstance(payload, SubResp):
            return self._on_sub_response(payload, now)
        if isinstance(payload, TimeoutAlarm):
            return self._on_alarm(payload, now)
        if isinstance(payload, CpuTimer):
            return self._on_cpu_timer(payload, now)
        if isinstance(payload, UvDone):
            return self._on_uv_done(payload, now)
        raise TypeError(f"{self.name}: không nhận payload {type(payload).__name__}")

    # ------------------------------------------------------------------ nhận request
    def _on_admitted(self, admitted: Admitted, now: float) -> list[Emission]:
        if admitted.request_id in self._active:
            raise ValueError(f"{self.name}: request {admitted.request_id} đã ở trong hệ thống")
        if self.n_in_system >= self.capacity_k:
            raise ValueError(
                f"{self.name}: nhận request khi đã đủ K={self.capacity_k} — "
                "Admission Control phải chặn trước"
            )

        req = _Active(
            request_id=admitted.request_id,
            t_arrive=admitted.t_arrive,
            t_deadline=admitted.t_arrive + self.t_timeout,
        )
        req.ingress_s, req.callback_s = sample_cpu_phases(
            self.ingress_s, self.callback_s, self._cpu_rng
        )
        self._active[req.request_id] = req
        self.admitted += 1

        out = [
            Emission(
                timestamp=req.t_deadline,
                destination=self.name,
                payload=TimeoutAlarm(req.request_id, req.t_deadline),
            )
        ]
        if self.uv_probability > 0.0 and self._rng is not None and self._rng.random() < self.uv_probability:
            # Offload pha ingress sang libuv: loop **không** bị giữ trong 12 ms.
            req.uv_offloaded = True
            self.uv_requests += 1
            return out + [
                Emission(
                    timestamp=now,
                    destination=self.uv_pool_name,
                    payload=UvTask(request_id=req.request_id, t_submit=now),
                )
            ]

        out.extend(self._enqueue(req, PHASE_INGRESS, now))
        return out

    def _on_uv_done(self, done: UvDone, now: float) -> list[Emission]:
        """Job offload xong → xin kênh loop để chạy fan-out như bình thường."""
        req = self._active.get(done.request_id)
        if req is None or req.retired:
            self.late_responses += 1
            return []
        return self._enqueue(req, PHASE_INGRESS, now)

    # ------------------------------------------------------------------ slot pool
    def _enqueue(self, req: _Active, phase: str, now: float) -> list[Emission]:
        """Xin một kênh cho `phase`; không có thì xếp FIFO."""
        if self._busy < self.channels:
            self._busy += 1
            req.holding_slot = True
            return [self._start_cpu(req, phase, now)]
        req.pending_phase = phase
        self._waiting.append((req.request_id, phase))
        return []

    def _start_cpu(self, req: _Active, phase: str, now: float) -> Emission:
        if phase == PHASE_INGRESS:
            req.t_dispatch = now
            return Emission(
                timestamp=now + req.ingress_s,
                destination=self.name,
                payload=CpuTimer(req.request_id, PHASE_INGRESS),
            )
        return Emission(
            timestamp=now + req.callback_s,
            destination=self.name,
            payload=CpuTimer(req.request_id, PHASE_CALLBACK),
        )

    def _release(self, req: _Active, now: float) -> list[Emission]:
        """Nhả kênh và trao ngay cho request kế tiếp trong FIFO (nếu có)."""
        if not req.holding_slot:
            return []
        req.holding_slot = False
        self._busy -= 1

        while self._waiting:
            request_id, phase = self._waiting.pop(0)
            nxt = self._active.get(request_id)
            if nxt is None or nxt.retired or nxt.pending_phase != phase:
                continue  # request đã chốt trong lúc chờ
            nxt.pending_phase = None
            self._busy += 1
            nxt.holding_slot = True
            if phase == PHASE_INGRESS:
                nxt.t_dispatch = now
            return [
                self._start_cpu(nxt, phase, now),
                # Pha callback chạy xong là chốt kết cục; pha ingress thì fan-out.
            ]
        return []

    # ------------------------------------------------------------------ hai pha CPU
    def _on_cpu_timer(self, timer: CpuTimer, now: float) -> list[Emission]:
        req = self._active.get(timer.request_id)
        if req is None or req.retired:
            return []

        if timer.kind == PHASE_INGRESS:
            # Pinning: quyết định theo từng request (K12). Blocking thì luôn giữ.
            req.pinned = self.holds_slot_during_io or self._sample_pin()
            if req.pinned:
                self.pinned_requests += 1
            out: list[Emission] = []
            if not req.pinned:
                out.extend(self._release(req, now))
            out.extend(self._fan_out(req, now))
            return out

        if timer.kind == PHASE_CALLBACK:
            out = self._release(req, now)
            out.extend(self._finish(req, now))
            return out

        raise ValueError(f"{self.name}: CpuTimer.kind lạ {timer.kind!r}")

    # ------------------------------------------------------------------ fan-out
    def _fan_out(self, req: _Active, now: float) -> list[Emission]:
        """Phát truy vấn nhánh. Sequential (Blocking) chỉ phát nhánh kế tiếp."""
        if self.sequential_branches:
            pending = [b for b in Branch if b not in req.sent]
            if not pending:
                return []
            return [self._send_branch(req, pending[0], now)]
        return [self._send_branch(req, branch, now) for branch in Branch if branch not in req.sent]

    def _send_branch(self, req: _Active, branch: Branch, now: float) -> Emission:
        req.sent.append(branch)
        self.branch_requests += 1
        return Emission(
            timestamp=now,
            destination=self.sub_request_target,
            payload=SubReq(request_id=req.request_id, branch=branch, t_sent=now),
        )

    # ------------------------------------------------------------------ phản hồi nhánh
    def _on_sub_response(self, resp: SubResp, now: float) -> list[Emission]:
        req = self._active.get(resp.request_id)
        if req is None or req.retired or req.joined:
            self.late_responses += 1  # response về sau khi đã chốt / huỷ
            return []
        if not resp.ok:
            return self._on_branch_failure(req, resp, now)

        req.successes += 1
        return self._after_branch(req, now)

    def _on_branch_failure(self, req: _Active, resp: SubResp, now: float) -> list[Emission]:
        req.failures += 1
        if resp.branch is Branch.DB and resp.reason == "no_connection":
            req.db_rejected = True

        # All-Join 3/3: DB đầy là 504 ngay, không có nhánh nào thay thế.
        if self.needs_all:
            return self._begin_callback(req, now)

        # Quorum 2/3: còn khả năng đạt 2 thành công hay không?
        # `branches_left` là số nhánh *chưa gửi*; ở đây phải tính số nhánh *chưa trả lời*.
        unanswered = N_BRANCHES - (req.successes + req.failures)
        if req.successes + unanswered < QUORUM_NEEDED:
            return self._begin_callback(req, now, forced_reason="insufficient_branches")
        return []

    def _after_branch(self, req: _Active, now: float) -> list[Emission]:
        if self.sequential_branches:
            out = self._fan_out(req, now)  # gửi nhánh kế tiếp
            if out:
                return out
        if self.needs_all:
            if req.successes == N_BRANCHES:
                return self._begin_callback(req, now)
            return []
        if req.successes >= QUORUM_NEEDED:
            return self._begin_callback(req, now)
        return []

    # ------------------------------------------------------------------ join → callback
    def _begin_callback(
        self, req: _Active, now: float, *, forced_reason: str | None = None
    ) -> list[Emission]:
        """Chốt phần join; xin kênh để chạy pha callback (có thể phải xếp hàng)."""
        req.joined = True
        if forced_reason is not None:
            req.forced_reason = forced_reason
        if req.holding_slot:
            # Blocking / pinning: kênh vẫn đang giữ ⇒ chạy callback ngay.
            return [
                Emission(
                    timestamp=now + req.callback_s,
                    destination=self.name,
                    payload=CpuTimer(req.request_id, PHASE_CALLBACK),
                )
            ]
        return self._enqueue(req, PHASE_CALLBACK, now)

    # ------------------------------------------------------------------ SLA
    def _on_alarm(self, alarm: TimeoutAlarm, now: float) -> list[Emission]:
        req = self._active.get(alarm.request_id)
        if req is None or req.retired:
            return []  # đã chốt: alarm là no-op, không double count

        if now < req.t_deadline:
            return [
                Emission(
                    timestamp=req.t_deadline,
                    destination=self.name,
                    payload=TimeoutAlarm(req.request_id, req.t_deadline, alarm.deferred),
                )
            ]

        # K11: nhường mọi response cùng mốc deadline xử lý trước.
        if not alarm.deferred:
            return [
                Emission(
                    timestamp=now,
                    destination=self.name,
                    payload=TimeoutAlarm(req.request_id, req.t_deadline, deferred=True),
                )
            ]

        out = self._release(req, now)
        req.retired = True
        self.timed_out += 1
        del self._active[req.request_id]
        out.append(
            Emission(
                timestamp=now,
                destination=SINK_TIMEOUT,
                payload=self._notice(req, Outcome.TIMEOUT, req.t_deadline),
            )
        )
        out.append(
            Emission(
                timestamp=now,
                destination=LP_ADMISSION,
                payload=self._notice(req, Outcome.TIMEOUT, req.t_deadline),
            )
        )
        return out

    # ------------------------------------------------------------------ chốt kết cục
    def _finish(self, req: _Active, now: float) -> list[Emission]:
        req.retired = True
        r_s = now - req.t_arrive

        if self.needs_all and req.failures > 0:
            outcome = Outcome.DB_DROP
            reason = REASON_DB_FULL if req.db_rejected else None
        elif req.successes < (N_BRANCHES if self.needs_all else QUORUM_NEEDED):
            outcome = Outcome.TIMEOUT
            reason = req.forced_reason or "insufficient_branches"
        elif r_s > self.t_timeout:
            outcome = Outcome.TIMEOUT
            reason = req.forced_reason
        else:
            outcome = Outcome.COMPLETED
            reason = None

        if outcome is Outcome.COMPLETED:
            self.completed += 1
        elif outcome is Outcome.DB_DROP:
            self.db_dropped += 1
        else:
            self.timed_out += 1

        req_dict = self._active.pop(req.request_id)
        notice = self._notice(req_dict, outcome, now, reason=reason)
        sink = {
            Outcome.COMPLETED: SINK_COMPLETED,
            Outcome.DB_DROP: SINK_TIMEOUT,
            Outcome.TIMEOUT: SINK_TIMEOUT,
        }[outcome]
        return [
            Emission(timestamp=now, destination=sink, payload=notice),
            Emission(timestamp=now, destination=LP_ADMISSION, payload=notice),
        ]

    def _notice(
        self,
        req: _Active,
        outcome: Outcome,
        t_complete: float,
        *,
        reason: str | None = None,
    ) -> OutcomeNotice:
        return OutcomeNotice(
            request_id=req.request_id,
            outcome=outcome,
            t_arrive=req.t_arrive,
            t_dispatch=req.t_dispatch if req.t_dispatch is not None else req.t_arrive,
            t_complete=t_complete,
            n_in_system=self.n_in_system + 1,
            runtime_model=self.profile.name,
            join_mode=self.join_mode,
            reason=reason,
        )

    def _sample_pin(self) -> bool:
        """Bernoulli `p_pin` cho Thread Pinning (Loom). Không có rng ⇒ không bao giờ pin."""
        if self.pin_probability <= 0.0 or self._rng is None:
            return False
        return self._rng.random() < self.pin_probability

    # ------------------------------------------------------------------ thống kê
    def snapshot(self) -> dict[str, float | int]:
        return {
            "runtime_model": self.profile.name,
            "join_mode": self.join_mode,
            "channels": self.channels,
            "admitted": self.admitted,
            "branch_requests": self.branch_requests,
            "branch_ratio": (self.branch_requests / self.admitted) if self.admitted else 0.0,
            "completed": self.completed,
            "timed_out": self.timed_out,
            "db_dropped": self.db_dropped,
            "late_responses": self.late_responses,
            "pinned_requests": self.pinned_requests,
            "uv_requests": self.uv_requests,
            "in_system_at_end": self.n_in_system,
            "busy_channels": self._busy,
            "queued": self.queued,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"RuntimeDispatcherLP({self.profile.name}, {self.join_mode}, "
            f"channels={self.channels}, in_system={self.n_in_system})"
        )
