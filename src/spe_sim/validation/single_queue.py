"""A2 — chạy 6 hàng đợi đơn trên **chính các LP của mạng** (không phải bản cài riêng).

Ánh xạ (spec §1.4 Bảng 1 → thành phần thật của `spe_sim`):

    M/M/1, M/M/1/K, M/M/c, M/M/c/K, M/M/n/n → `ServiceLP` (bọc `StationCore`)
                                             — cùng lớp với Auth / Catalog / DB pool
    M/M/∞                                    → `NetworkLinkLP` (một chặng)
                                             — chính là Internal Network Link

Mạng tối giản, chạy trên `CoroutineKernel` (asyncio) như mạng thật:

    SourceLP ──ReqArrival──▶ _Feeder("admission") ──SubReq──▶ trạm
    trạm ──SubResp──▶ _Collector("network_link")           (ServiceLP)
    trạm ──SubReq (đã trễ)──▶ _Collector("auth_cluster")    (NetworkLinkLP)

`_Feeder` và `_Collector` mượn đúng tên LP mà trạm thật gửi tới, nên `ServiceLP` /
`NetworkLinkLP` chạy **nguyên vẹn**, không sửa dòng nào.
"""

from __future__ import annotations

import math
from typing import Any

from spe_sim.core.config import LP_ADMISSION, LP_NETWORK_LINK, SERVICE_LP_NAMES
from spe_sim.core.events import Branch, Emission, ReqArrival, SubReq, SubResp
from spe_sim.core.kernel import CoroutineKernel, SequentialKernel
from spe_sim.core.rng import SeedStreams
from spe_sim.lp.infra.network_link import NetworkLinkLP
from spe_sim.lp.infra.service_lp import ServiceLP
from spe_sim.lp.infra.source import SourceLP
from spe_sim.validation.measure import Visit, WindowStats

__all__ = ["STATION_KENDALLS", "kernel_for", "run_station"]

STATION_KENDALLS: tuple[str, ...] = ("M/M/1", "M/M/1/K", "M/M/inf", "M/M/n/n", "M/M/c", "M/M/c/K")

_STATION = "station"
_INF_TARGET = SERVICE_LP_NAMES[Branch.AUTH.value]  # NetworkLinkLP chuyển SubReq tới đây


def kernel_for(kind: str, end_time: float) -> Any:
    """Kernel có bucket đủ rộng cho `end_time` bất kỳ.

    `LadderQueue` chọn bucket bằng `int(t / width) % num_buckets` (1024 bucket ở
    `BkSysCoEventLoop`): nếu `end_time` vượt `1024 × width` thì thứ tự event bị đảo âm thầm.
    Lấy `width = end_time / 256` ⇒ chỉ dùng 1/4 vòng bucket.
    """
    width = max(end_time / 256.0, 1e-3)
    if kind == "coroutine":
        return CoroutineKernel(end_time=end_time, bucket_width=width)
    if kind == "sequential":
        return SequentialKernel(end_time=end_time, bucket_width=width)
    raise ValueError(f"kernel phải là 'coroutine' hoặc 'sequential', nhận {kind!r}")


class _Feeder:
    """Đứng ở tên `admission`: đổi `ReqArrival` thành `SubReq` gửi tới trạm."""

    name = LP_ADMISSION

    def __init__(self, target: str) -> None:
        self.target = target
        self.arrivals: dict[int, float] = {}

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if not isinstance(payload, ReqArrival):
            raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")
        self.arrivals[payload.request_id] = now
        return [
            Emission(
                timestamp=now,
                destination=self.target,
                payload=SubReq(request_id=payload.request_id, branch=Branch.AUTH, t_sent=now),
            )
        ]


class _Collector:
    """Thu phản hồi của trạm; ghi một `Visit` cho mỗi khách."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.visits: dict[int, Visit] = {}

    def handle_event(self, payload: object, now: float) -> list[Emission]:
        if isinstance(payload, SubResp):  # ServiceLP: có mốc bắt đầu/kết thúc phục vụ
            if payload.ok:
                visit = Visit(payload.t_sent, payload.t_service_start, payload.t_service_end)
            else:
                visit = Visit(payload.t_sent)  # bị từ chối (M/M/c/K đầy, M/M/n/n hết kênh)
        elif isinstance(payload, SubReq):  # NetworkLinkLP (M/M/∞): phục vụ ngay khi đến
            visit = Visit(payload.t_sent, payload.t_sent, now)
        else:
            raise TypeError(f"{self.name}: không nhận {type(payload).__name__}")
        if payload.request_id in self.visits:
            raise ValueError(f"{self.name}: request {payload.request_id} về hai lần")
        self.visits[payload.request_id] = visit
        return []


def run_station(
    kendall: str,
    lam: float,
    mu: float,
    *,
    c: int | None = None,
    capacity: int | None = None,
    t_sim: float,
    t_warmup: float,
    seed: int,
    kernel: str = "coroutine",
) -> dict[str, float | None]:
    """Chạy một hàng đợi đơn; trả `L, Lq, W, Wq, X, P_block, U` trên cửa sổ đo.

    `c` là số kênh (M/M/c, M/M/c/K, và `n` của M/M/n/n); `capacity` là K.
    """
    if kendall not in STATION_KENDALLS:
        raise ValueError(f"kendall phải thuộc {STATION_KENDALLS}, nhận {kendall!r}")
    streams = SeedStreams(seed)
    channels: float
    if kendall == "M/M/inf":
        station: Any = NetworkLinkLP(streams.stream("service"), mean_one_way_s=1.0 / mu, name=_STATION)
        collector = _Collector(_INF_TARGET)
        channels = math.inf
    else:
        servers = 1 if kendall in ("M/M/1", "M/M/1/K") else c
        if servers is None or servers < 1:
            raise ValueError(f"{kendall}: cần c ≥ 1")
        k = {
            "M/M/1": None,
            "M/M/1/K": capacity,
            "M/M/c": None,
            "M/M/c/K": capacity,
            "M/M/n/n": servers,
        }[kendall]
        if kendall in ("M/M/1/K", "M/M/c/K") and k is None:
            raise ValueError(f"{kendall}: cần capacity K")
        station = ServiceLP(
            _STATION,
            servers,
            1.0 / mu,
            streams.stream("service"),
            capacity=k,
            loss=kendall == "M/M/n/n",
        )
        collector = _Collector(LP_NETWORK_LINK)
        channels = float(servers)

    source = SourceLP(lam, streams.stream("arrival"), t_sim)
    feeder = _Feeder(_STATION)
    # Đệm đủ dài để khách cuối kịp xong (hàng chờ còn lại lúc T_sim) — kiểm ở dưới.
    end_time = t_sim + max(2.0, 0.05 * t_sim)
    kern = kernel_for(kernel, end_time)
    kern.add(source, feeder, station, collector)
    kern.run()

    unfinished = len(feeder.arrivals) - len(collector.visits)
    if unfinished:
        raise RuntimeError(
            f"{kendall}: còn {unfinished} khách chưa xong ở end_time={end_time:.1f} s — "
            "tăng đệm drain"
        )
    stats = WindowStats(t_warmup, t_sim, channels)
    for visit in collector.visits.values():
        stats.add(visit)
    return stats.summary()
