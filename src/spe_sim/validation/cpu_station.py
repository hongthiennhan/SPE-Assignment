"""A3 — kiểm chứng riêng **trạm CPU** của 4 runtime (chế độ cô lập).

Hàng chờ CPU của runtime là slot pool bên trong dispatcher (`_acquire`/`_release`, hoặc
`_enqueue`/`_release` ở bản đồng bộ), không đi qua `StationCore`, nên A2 không phủ được.
Ở đây dùng **chính dispatcher thật** cùng Admission Control thật, chỉ thay phần còn lại:

    SourceLP → AdmissionControlLP(K=64) → dispatcher → _NullBackend("network_link")
                                              └──────→ sinks (OutcomeNotice)

- `_NullBackend` trả lời mọi `SubReq` **ngay lập tức** (trễ 0) ⇒ không còn pha I/O.
- Profile được dẫn xuất cho chế độ kiểm chứng: toàn bộ CPU dồn vào pha ingress
  (`callback = 0`) và giữ slot suốt vòng đời (`holds_slot_during_io = True`). Nếu vẫn chia
  hai pha thì request phải xếp hàng lại cho callback và trạm không còn là FIFO một lần.
- CPU ~ Exp(S) (A4), SLA rất lớn (10 s) để không request nào bị huỷ.

Khi đó (spec §1.4 Bảng 1, §3.3):

    Node.js : M/M/1/64,  µ = 1/3.2 ms = 312.5 req/s
    Bun     : M/M/1/64,  µ = 1/2.0 ms = 500   req/s
    Loom    : M/M/4/64,  µ = 1/2.5 ms = 400   req/s mỗi carrier
    Blocking: M/M/12/64, µ = 1/2.5 ms = 400   req/s mỗi OS thread (chỉ phần CPU)
"""

from __future__ import annotations

import dataclasses
from typing import Final

from spe_sim.core.config import ADMISSION_CAPACITY_K, LP_DISPATCHER, LP_NETWORK_LINK, get_profile
from spe_sim.core.events import Emission, Outcome, SubReq, SubResp
from spe_sim.core.rng import SeedStreams
from spe_sim.lp.infra.admission import AdmissionControlLP
from spe_sim.lp.infra.sinks import all_notices, build_sinks
from spe_sim.lp.infra.source import SourceLP
from spe_sim.lp.runtime.coroutine_dispatcher import CoroutineDispatcherLP
from spe_sim.lp.runtime.dispatcher import RuntimeDispatcherLP
from spe_sim.validation.measure import Visit, WindowStats
from spe_sim.validation.single_queue import kernel_for

__all__ = ["CPU_STATION_MODELS", "cpu_station_queue", "run_cpu_station"]

#: SLA đủ lớn để không request nào timeout (K = 64 ⇒ thời gian lưu tối đa cỡ 64·S ≪ 10 s).
VALIDATION_TIMEOUT_S: Final[float] = 10.0

CPU_STATION_MODELS: tuple[str, ...] = (
    "nodejs_event_loop",
    "bun_event_loop",
    "spring_loom_vt",
    "blocking_baseline",
)


def cpu_station_queue(runtime_model: str) -> tuple[str, float, int, int]:
    """`(kendall, µ mỗi kênh, c, K)` mà trạm CPU của runtime phải khớp."""
    profile = get_profile(runtime_model)
    mu = 1.0 / profile.cpu_total_s
    kendall = "M/M/1/K" if profile.channels == 1 else "M/M/c/K"
    return kendall, mu, profile.channels, ADMISSION_CAPACITY_K


class _NullBackend:
    """Đứng ở tên `network_link`: trả `SubResp(ok=True)` ngay, không trễ."""

    name = LP_NETWORK_LINK

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if not isinstance(payload, SubReq):
            raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")
        return [
            Emission(
                timestamp=now,
                destination=LP_DISPATCHER,
                payload=SubResp(
                    request_id=payload.request_id,
                    branch=payload.branch,
                    t_sent=payload.t_sent,
                    t_service_start=now,
                    t_service_end=now,
                ),
            )
        ]


def run_cpu_station(
    runtime_model: str,
    lam: float,
    *,
    t_sim: float,
    t_warmup: float,
    seed: int,
    kernel: str = "coroutine",
) -> dict[str, float | None]:
    """Chạy trạm CPU cô lập của một runtime; trả `L, Lq, W, Wq, X, P_block, U`."""
    if runtime_model not in CPU_STATION_MODELS:
        raise ValueError(f"runtime_model phải thuộc {CPU_STATION_MODELS}, nhận {runtime_model!r}")
    base = get_profile(runtime_model)
    profile = dataclasses.replace(
        base,
        ingress_s=base.cpu_total_s,
        callback_s=0.0,
        holds_slot_during_io=True,
    )
    streams = SeedStreams(seed)
    source = SourceLP(lam, streams.stream("arrival"), t_sim)
    admission = AdmissionControlLP(profile.name, "gather", capacity_k=ADMISSION_CAPACITY_K)
    dispatcher_cls = CoroutineDispatcherLP if kernel == "coroutine" else RuntimeDispatcherLP
    dispatcher = dispatcher_cls(
        profile,
        "gather",
        t_timeout=VALIDATION_TIMEOUT_S,
        cpu_rng=streams.stream("cpu"),
    )
    sinks = build_sinks()
    end_time = t_sim + VALIDATION_TIMEOUT_S + 1.0
    kern = kernel_for(kernel, end_time)
    kern.add(source, admission, dispatcher, _NullBackend(), *sinks.values())
    kern.run()

    notices = all_notices(sinks.values())
    if len(notices) != source.emitted:
        raise RuntimeError(f"{runtime_model}: {source.emitted} đến nhưng {len(notices)} kết cục")
    stats = WindowStats(t_warmup, t_sim, float(profile.channels))
    for notice in notices:
        if notice.outcome is Outcome.DROPPED:
            stats.add(Visit(notice.t_arrive))
        elif notice.outcome is Outcome.COMPLETED:
            # Chế độ cô lập: slot giữ từ t_dispatch (vào CPU) tới t_complete.
            stats.add(Visit(notice.t_arrive, notice.t_dispatch, notice.t_complete))
        else:
            raise RuntimeError(
                f"{runtime_model}: kết cục {notice.outcome.value} không được phép ở chế độ cô lập"
            )
    return stats.summary()
