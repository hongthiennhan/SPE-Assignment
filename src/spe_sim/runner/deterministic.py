"""Harness **tất định** để kiểm trace: service trả lời sau đúng một khoảng cố định.

Service thật (`lp/infra/*`) sample thời gian mũ nên chỉ so được *kỳ vọng*, không so được
một con số chính xác. Với harness này, mỗi kết cục là một đẳng thức:

    r = thời_gian_offload + ingress + (tổng hoặc max các nhánh) + callback

`network_link` được thay bằng `ExactLink` (không trễ) và các service bằng `FixedService`
(12 / 22 / 27 ms — đã bao gồm 2 ms khứ hồi), nên trace 63.50 / 29.50 / 24.50 ms kiểm được
tuyệt đối. Đây là nền của `tests/verify_trace.py` (T18).
"""

from __future__ import annotations

import random
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Final

from spe_sim.core.config import (
    AUTH_SERVICE_S,
    CATALOG_SERVICE_S,
    DB_SERVICE_S,
    LP_DISPATCHER,
    MS,
    NET_LINK_RTT_S,
    SERVICE_LP_NAMES,
    UV_POOL_CHANNELS,
    UV_POOL_SERVICE_S,
    JoinMode,
)
from spe_sim.core.events import (
    Admitted,
    Emission,
    OutcomeNotice,
    ReqArrival,
    SubReq,
    SubResp,
    UvDone,
    UvTask,
)
from spe_sim.core.kernel import CoroutineKernel, SequentialKernel
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.sinks import all_notices, build_sinks
from spe_sim.lp.infra.station import StationCore
from spe_sim.lp.runtime.models.base import build_model

__all__ = [
    "ExactLink",
    "FixedService",
    "ScenarioResult",
    "branch_delay_s",
    "run_deterministic",
    "run_deterministic_coroutine",
]

#: Độ trễ nhánh mặc định = service + 2 ms khứ hồi (spec §3.3: 12 / 22 / 27 ms).
def branch_delay_s() -> dict[str, float]:
    return {
        "auth": AUTH_SERVICE_S + NET_LINK_RTT_S,
        "catalog": CATALOG_SERVICE_S + NET_LINK_RTT_S,
        "db": DB_SERVICE_S + NET_LINK_RTT_S,
    }


class ExactLink:
    """Thay `NetworkLinkLP`: định tuyến `SUB_REQ` sang service, không thêm độ trễ."""

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
        raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")


class FixedUvPool:
    """Bản tất định của `UvWorkerPoolLP`: 4 kênh, mỗi job đúng `delay_s`.

    `UvWorkerPoolLP` thật sample thời gian mũ (M/M/c_uv), nên dùng nó ở harness trace sẽ làm
    phép so trở thành *kỳ vọng* chứ không còn là đẳng thức. Ở đây giữ nguyên cấu trúc
    kênh/hàng đợi, chỉ bỏ phần ngẫu nhiên.
    """

    name = "uv_pool"

    def __init__(self, delay_s: float, *, channels: int = 4) -> None:
        if delay_s <= 0.0:
            raise ValueError(f"delay_s phải > 0, nhận {delay_s}")
        self._delay = delay_s
        self.core = StationCore(self.name, channels)
        self._pending: deque[UvTask] = deque()
        self._running: set[int] = set()

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, UvTask):
            admission = self.core.on_arrival(now, self._delay)
            if admission.rejected:  # pragma: no cover - pool không giới hạn sức chứa
                raise RuntimeError(f"{self.name}: từ chối job {payload.request_id}")
            if admission.queued:
                self._pending.append(payload)
                return []
            self._running.add(payload.request_id)
            return [
                Emission(
                    timestamp=admission.finish_s if admission.finish_s is not None else now,
                    destination=self.name,
                    payload=UvDone(payload.request_id, payload.t_submit, now),
                )
            ]
        if isinstance(payload, UvDone):
            self._running.discard(payload.request_id)
            out = [
                Emission(
                    timestamp=now,
                    destination=LP_DISPATCHER,
                    payload=UvDone(payload.request_id, payload.t_submit, payload.t_start),
                )
            ]
            if self._pending:
                nxt = self._pending.popleft()
                admission = self.core.on_departure(now, self._delay)
                assert admission is not None and admission.finish_s is not None
                self._running.add(nxt.request_id)
                out.append(
                    Emission(
                        timestamp=admission.finish_s,
                        destination=self.name,
                        payload=UvDone(nxt.request_id, nxt.t_submit, now),
                    )
                )
            else:
                self.core.on_departure(now)
            return out
        raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")


class FixedService:
    """Service trả lời sau đúng `delay_s`; `ok=False` là từ chối ngay (DB đầy)."""

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


def run_deterministic_coroutine(
    runtime_model: str,
    join_mode: JoinMode,
    *,
    auth_s: float | None = None,
    catalog_s: float | None = None,
    db_s: float | None = None,
    db_ok: bool = True,
    arrivals: tuple[float, ...] = (0.0,),
    p_pin: float = 0.0,
    p_uv: float = 0.0,
    seed: int = 0,
    end_time: float | None = None,
) -> ScenarioResult:
    """Như `run_deterministic` nhưng trên `CoroutineKernel` + `CoroutineDispatcherLP`
    (asyncio, `asyncio.gather`/`as_completed`) — dùng để chứng minh hai cơ chế cho cùng
    một kết quả số.
    """
    delays = branch_delay_s()
    auth_delay = delays["auth"] if auth_s is None else auth_s
    catalog_delay = delays["catalog"] if catalog_s is None else catalog_s
    db_delay = delays["db"] if db_s is None else db_s

    model = build_model(runtime_model, p_pin=p_pin, p_uv=p_uv)
    rng = random.Random(seed) if (p_pin > 0.0 or p_uv > 0.0) else None
    dispatcher = model.build_coroutine_dispatcher(join_mode, rng=rng)
    admission = AdmissionControlLP(runtime_model, join_mode)  # type: ignore[arg-type]

    sinks = build_sinks()
    kernel = CoroutineKernel(
        end_time=end_time if end_time is not None else max(arrivals, default=0.0) + 1.0
    )
    kernel.add(
        admission,
        dispatcher,
        ExactLink(),
        FixedService("auth_cluster", auth_delay),
        FixedService("catalog_service", catalog_delay),
        FixedService("db_pool", db_delay, ok=db_ok),
        FixedUvPool(UV_POOL_SERVICE_S, channels=UV_POOL_CHANNELS),
        *sinks.values(),
    )
    emit = kernel.make_emit(kernel.lp_ids["admission"])
    for request_id, t in enumerate(arrivals):
        emit(
            Emission(
                timestamp=t,
                destination="admission",
                payload=ReqArrival(request_id=request_id, t_arrive=t),
            )
        )
    stats = kernel.run()
    return ScenarioResult(
        notices=all_notices(sinks.values()),
        dispatcher=dispatcher,
        admission=admission,
        kernel=kernel,  # type: ignore[arg-type]
        stats=stats,
    )


@dataclass
class ScenarioResult:
    """Kết quả một lần chạy tất định."""

    notices: list[OutcomeNotice]
    dispatcher: Any
    admission: Any
    kernel: SequentialKernel
    stats: dict[str, Any] = field(default_factory=dict)

    def r_ms(self) -> list[float]:
        return [(n.t_complete - n.t_arrive) / MS for n in self.notices]


def run_deterministic(
    runtime_model: str,
    join_mode: JoinMode,
    *,
    auth_s: float | None = None,
    catalog_s: float | None = None,
    db_s: float | None = None,
    db_ok: bool = True,
    arrivals: tuple[float, ...] = (0.0,),
    p_pin: float = 0.0,
    p_uv: float = 0.0,
    seed: int = 0,
    end_time: float | None = None,
) -> ScenarioResult:
    """Chạy một kịch bản tất định (service cố định) trên kernel thật của `bkitevsim`."""
    delays = branch_delay_s()
    auth_delay = delays["auth"] if auth_s is None else auth_s
    catalog_delay = delays["catalog"] if catalog_s is None else catalog_s
    db_delay = delays["db"] if db_s is None else db_s

    model = build_model(runtime_model, p_pin=p_pin, p_uv=p_uv)
    rng = random.Random(seed) if (p_pin > 0.0 or p_uv > 0.0) else None
    dispatcher = model.build_dispatcher(join_mode, rng=rng)
    admission = AdmissionControlLP(runtime_model, join_mode)  # type: ignore[arg-type]

    sinks = build_sinks()
    kernel = SequentialKernel(
        end_time=end_time if end_time is not None else max(arrivals, default=0.0) + 1.0
    )
    kernel.add(
        admission,
        dispatcher,
        ExactLink(),
        FixedService("auth_cluster", auth_delay),
        FixedService("catalog_service", catalog_delay),
        FixedService("db_pool", db_delay, ok=db_ok),
        FixedUvPool(UV_POOL_SERVICE_S, channels=UV_POOL_CHANNELS),
        *sinks.values(),
    )
    for request_id, t in enumerate(arrivals):
        kernel.adapters["admission"].emit(
            Emission(
                timestamp=t,
                destination="admission",
                payload=ReqArrival(request_id=request_id, t_arrive=t),
            )
        )
    stats = kernel.run()
    return ScenarioResult(
        notices=all_notices(sinks.values()),
        dispatcher=dispatcher,
        admission=admission,
        kernel=kernel,
        stats=stats,
    )
