"""Adapter duy nhất tới framework `bkitevsim` — đã đối chiếu với source thật trong `vendor/`.

VÌ SAO CÓ FILE NÀY
------------------
Phần còn lại của `spe_sim` là code thuần: mỗi LP có `handle_event(payload, now)` trả về
`list[Emission]` (xem `spe_sim.core.events`). File này là chỗ **duy nhất** biết tới
`bkitevsim`: nó import kernel, ghim hằng số cấu hình an toàn, và bọc từng LP thành một
"logical process" mà dispatcher của framework điều khiển được.

API THẬT (đọc từ `vendor/bkitevsim`, KHÔNG sửa một dòng nào)
------------------------------------------------------------
    bkitevsim.core.Event(timestamp, sender_id:int, receiver_id:int, event_type:str, payload)
    bkitevsim.queues.LadderQueue(bucket_width=1.0, num_buckets=1024)
    bkitevsim.queues.PriorityQueue(bucket_width=1.0)
    bkitevsim.core.BkSysCoEventLoop(end_time, bucket_width).run(max_iterations=None)
        ↳ dùng bởi `CoroutineKernel`; LP coroutine nhận event qua `handle_event`, chờ qua
          `wait_for_event`/`wait_until_time` (loop chỉ tiến thời gian khi không LP nào READY)
    bkitevsim.core.BkSysCoLogicalProcess      (coroutine)
    bkitevsim.dispatchers.SequentialEventDispatcher(end_time, bucket_width).run()

So với giả định cũ (`Event(source=..., destination=...)`), khác biệt quyết định là LP được
định danh bằng **số nguyên** `sender_id`/`receiver_id`, còn `Emission.destination` của ta là
**tên**. `SequentialKernel` giữ bảng `tên → id` và làm phần phiên dịch đó.

BA QUYẾT ĐỊNH CẤU HÌNH (xem `report.md` §9.4 để biết lý do)
-----------------------------------------------------------
1. Hai engine, **cùng một bộ LP**: `CoroutineKernel` (`BkSysCoEventLoop`, asyncio — đúng spec
   §1.1.2, mặc định của runner) và `SequentialKernel` (`SequentialEventDispatcher`, chạy nhanh
   ~3×, dùng để đối chứng). Cả hai cho kết quả số trùng khớp; xem `report.md` §11.5.
2. Ghim `bucket_width = 1.0` và `num_buckets = 2048`. Lý do: `LadderQueue` chọn bucket theo
   `int(t / bucket_width) % num_buckets`, nên nếu `num_buckets * bucket_width` nhỏ hơn thời
   điểm lớn nhất thì **thứ tự event bị sai âm thầm**. Với `T_sim = 200 s` và 2048 bucket × 1 s
   ta còn dư 10× biên an toàn.
3. `SequentialEventDispatcher.run()` **bỏ** event đầu tiên có `timestamp > end_time`, nên
   `end_time` phải lớn hơn `T_sim` (mặc định đệm `END_TIME_PADDING_S = 1.0`).
   Thống kê vẫn cắt ở `T_sim`, chỉ có điều kiện dừng của kernel là rộng hơn.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from spe_sim.core.events import (
    Emission,
    ReqArrival,
    SubReq,
    SubResp,
    TimeoutAlarm,
)

BKITEVSIM_MODULE: Final[str] = "bkitevsim"

#: Thư mục vendor tương đối so với gốc repo (kernel.py → core → spe_sim → src → repo).
_VENDOR_RELATIVE: Final[str] = Path("vendor") / "bkitevsim"

#: Tên thật + module thật của từng thành phần (khác hẳn giả định ban đầu: không có top-level).
EXPECTED_LAYOUT: Final[dict[str, str]] = {
    "Event": "bkitevsim.core",
    "BkSysLogicalProcess": "bkitevsim.core",
    "BkSysCoEventLoop": "bkitevsim.core",
    "BkSysCoLogicalProcess": "bkitevsim.core.logical_coProcess",
    "LadderQueue": "bkitevsim.queues",
    "PriorityQueue": "bkitevsim.queues",
    "SequentialEventDispatcher": "bkitevsim.dispatchers",
}

#: Cấu hình an toàn cho LadderQueue (xem docstring đầu file, quyết định 2).
LADDER_BUCKET_WIDTH: Final[float] = 1.0
LADDER_NUM_BUCKETS: Final[int] = 2048

#: Đệm cho điều kiện dừng của dispatcher (quyết định 3).
END_TIME_PADDING_S: Final[float] = 1.0

#: Tên `event_type` theo `docs/team/Thống nhất DEV.md` §4 — dùng khi payload là 1 trong 4 loại.
EVENT_TYPE_BY_PAYLOAD: Final[dict[type, str]] = {
    ReqArrival: "REQ_ARRIVAL",
    SubReq: "SUB_REQ",
    SubResp: "SUB_RESP",
    TimeoutAlarm: "TIMEOUT_ALARM",
}


class KernelUnavailable(RuntimeError):
    """Kernel `bkitevsim` chưa sẵn sàng (không import được hoặc thiếu thành phần)."""


@dataclass(frozen=True, slots=True)
class KernelBindings:
    """Các thành phần kernel đã bind."""

    Event: type
    LadderQueue: type
    PriorityQueue: type
    BkSysLogicalProcess: type
    BkSysCoEventLoop: type
    BkSysCoLogicalProcess: type
    SequentialEventDispatcher: type


_bindings: KernelBindings | None = None
_error: str | None = None


def vendor_path() -> Path:
    """Đường dẫn thư mục `vendor/bkitevsim` của repo này."""
    return Path(__file__).resolve().parents[3] / _VENDOR_RELATIVE


def _add_vendor_to_sys_path() -> bool:
    """Thêm `vendor/bkitevsim` vào `sys.path` nếu chưa có. Trả về True nếu đã thêm."""
    candidate = vendor_path()
    if not (candidate / BKITEVSIM_MODULE).is_dir():
        return False
    entry = str(candidate)
    if entry in sys.path:
        return False
    sys.path.insert(0, entry)
    return True


def _bind() -> KernelBindings:
    """Import và kiểm tra `bkitevsim`. Cache kết quả (kể cả lỗi)."""
    global _bindings, _error
    if _bindings is not None:
        return _bindings
    if _error is not None:
        raise KernelUnavailable(_error)

    if _add_vendor_to_sys_path():
        # `bkitevsim` là namespace package (không có __init__.py ở gốc) nên phải có trên sys.path.
        pass

    try:
        import importlib

        resolved: dict[str, Any] = {}
        missing: list[str] = []
        for name, module_name in EXPECTED_LAYOUT.items():
            try:
                module = importlib.import_module(module_name)
            except ModuleNotFoundError:
                missing.append(f"{module_name}.{name}")
                continue
            if not hasattr(module, name):
                missing.append(f"{module_name}.{name}")
                continue
            resolved[name] = getattr(module, name)
    except Exception as exc:  # pragma: no cover - lỗi môi trường
        _error = f"Không nạp được {BKITEVSIM_MODULE!r}: {exc}"
        raise KernelUnavailable(_error) from exc

    if missing:
        _error = (
            f"Package {BKITEVSIM_MODULE!r} thiếu thành phần: {missing}. "
            f"Kỳ vọng theo source trong {vendor_path()} — xem report.md §9.2."
        )
        raise KernelUnavailable(_error)

    try:
        _bindings = KernelBindings(**resolved)  # type: ignore[arg-type]
    except TypeError as exc:  # pragma: no cover - lỗi lập trình
        _error = f"Bindings không khớp KernelBindings: {exc}"
        raise KernelUnavailable(_error) from exc
    return _bindings


def is_available() -> bool:
    """True nếu kernel dùng được (import + đủ thành phần)."""
    try:
        _bind()
    except KernelUnavailable:
        return False
    return True


def unavailable_reason() -> str | None:
    """Thông điệp lỗi nếu thiếu kernel, ngược lại None."""
    try:
        _bind()
    except KernelUnavailable as exc:
        return str(exc)
    return None


def make_event(
    *,
    timestamp: float,
    sender_id: int,
    receiver_id: int,
    event_type: str,
    payload: Any,
) -> Any:
    """Tạo `Event` của kernel — điểm duy nhất được phép dựng event."""
    return _bind().Event(
        timestamp=timestamp,
        sender_id=sender_id,
        receiver_id=receiver_id,
        event_type=event_type,
        payload=payload,
    )


def make_ladder_queue(
    *,
    bucket_width: float = LADDER_BUCKET_WIDTH,
    num_buckets: int = LADDER_NUM_BUCKETS,
) -> Any:
    """Tạo `LadderQueue` với cấu hình an toàn (không để wrap bucket index)."""
    return _bind().LadderQueue(bucket_width=bucket_width, num_buckets=num_buckets)


def event_type_for(payload: object) -> str:
    """Tên `event_type` cho payload: 4 loại chuẩn dùng đúng tên DEV.md §4."""
    return EVENT_TYPE_BY_PAYLOAD.get(type(payload), type(payload).__name__)


class LogicalProcessAdapter:
    """Bọc một LP thuần (`handle_event(payload, now) -> list[Emission]`) thành LP của kernel.

    Duck-typing thay vì kế thừa `BkSysLogicalProcess`: dispatcher chỉ cần
    `lp_id`, `initialize()`, `handle_event(event)`, `send_event`, `set_local_virt_time`,
    `finalize()` — tất cả đều có ở đây, nên không phụ thuộc class cụ thể nào của framework.
    """

    def __init__(self, lp_id: int, inner: Any, registry: dict[str, int]) -> None:
        self.lp_id = lp_id
        self.inner = inner
        self.name: str = inner.name
        self.local_virt_time = 0.0
        self._registry = registry
        self.emitted = 0
        # Được `SequentialEventDispatcher.register_lp()` ghi đè thành `schedule_event`.
        self.send_event: Any = None

    # ------------------------------------------------------------------ vòng đời
    def initialize(self) -> None:
        """Hook của framework: nạp trước event cho LP nguồn (`stream()`) nếu có."""
        stream = getattr(self.inner, "stream", None)
        if stream is not None:
            for emission in stream():
                self.emit(emission)
            return
        start = getattr(self.inner, "start", None)
        if start is not None:
            for emission in start():
                self.emit(emission)

    def handle_event(self, event: Any) -> None:
        """Nhận `Event` của kernel, gọi logic thuần, rồi phát các `Emission` trả về."""
        self.set_local_virt_time(event.timestamp)
        for emission in self.inner.handle_event(event.payload, event.timestamp):
            self.emit(emission)

    def set_local_virt_time(self, local_virt_time: float) -> None:
        self.local_virt_time = local_virt_time

    def finalize(self) -> None:
        finalize = getattr(self.inner, "finalize", None)
        if finalize is not None:
            finalize()

    # ------------------------------------------------------------------ phát event
    def emit(self, emission: Emission) -> None:
        """Đổi `Emission` (tên LP đích) thành `Event` (id số) và gửi vào queue."""
        receiver_id = self._registry.get(emission.destination)
        if receiver_id is None:
            raise KeyError(
                f"{self.name}: không có LP tên {emission.destination!r}. "
                f"Đã đăng ký: {sorted(self._registry)}"
            )
        if self.send_event is None:
            raise RuntimeError(
                f"{self.name}: chưa được dispatcher đăng ký nên không có send_event. "
                "Hãy thêm LP vào SequentialKernel trước khi chạy."
            )
        self.send_event(
            make_event(
                timestamp=emission.timestamp,
                sender_id=self.lp_id,
                receiver_id=receiver_id,
                event_type=event_type_for(emission.payload),
                payload=emission.payload,
            )
        )
        self.emitted += 1

    def get_statistics(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "lp_id": self.lp_id,
            "local_virt_time": self.local_virt_time,
            "events_emitted": self.emitted,
        }


class CoroutineAdapter(LogicalProcessAdapter):
    """Bọc LP thuần thành **coroutine LP** cho `BkSysCoEventLoop` (spec §1.1.2).

    LP thuần vẫn giữ nguyên `handle_event(payload, now) -> list[Emission]`: event được
    framework giao đồng bộ, còn `coroutine_run` chỉ có nhiệm vụ giữ LP sống và nạp trước
    event cho nguồn. Vì vậy chạy được **cùng một bộ LP** trên cả hai kernel.
    """

    def __init__(self, lp_id: int, inner: Any, registry: dict[str, int], loop: Any) -> None:
        super().__init__(lp_id, inner, registry)
        self._loop = loop
        self.send_event = loop.schedule_event

    async def coroutine_run(self) -> None:
        import asyncio

        self.initialize()  # nạp event đầu cho LP nguồn (stream())
        # Chờ một mốc ngoài `end_time`: loop bỏ qua waiter quá hạn, nên LP không đẩy thời gian
        # ảo đi và không tranh chấp với event thật. Task bị huỷ khi mô phỏng kết thúc.
        await self._loop.wait_until_time(self.lp_id, float(self._loop.end_time) + 1.0)
        await asyncio.sleep(0)  # pragma: no cover - chỉ để coroutine là async thật

    def handle_event(self, event: Any) -> None:  # noqa: D401 - cùng chữ ký adapter đồng bộ
        self.set_local_virt_time(event.timestamp)
        for emission in self.inner.handle_event(event.payload, event.timestamp):
            self.emit(emission)

    def __repr__(self) -> str:  # pragma: no cover
        return f"LogicalProcessAdapter(id={self.lp_id}, name={self.name!r})"


class SequentialKernel:
    """Chạy một tập LP thuần trên `SequentialEventDispatcher` của `bkitevsim`.

    Cách dùng:

        kernel = SequentialKernel(end_time=20.0 + END_TIME_PADDING_S)
        kernel.add(source)         # LP có `stream()`
        kernel.add(admission)
        kernel.add(link, auth, catalog, db, *sinks.values(), dispatcher)
        stats = kernel.run()

    Lưu ý: phải `add()` **tất cả** LP trước khi `run()`, vì `Emission.destination` được
    phiên dịch sang id lúc phát event.
    """

    def __init__(
        self,
        *,
        end_time: float,
        bucket_width: float = LADDER_BUCKET_WIDTH,
        num_buckets: int = LADDER_NUM_BUCKETS,
    ) -> None:
        if end_time <= 0.0:
            raise ValueError(f"end_time phải > 0, nhận {end_time}")
        if num_buckets < 2:
            raise ValueError(f"num_buckets phải ≥ 2, nhận {num_buckets}")
        if bucket_width <= 0.0:
            raise ValueError(f"bucket_width phải > 0, nhận {bucket_width}")
        if num_buckets * bucket_width <= end_time:
            raise ValueError(
                f"num_buckets × bucket_width = {num_buckets * bucket_width} phải > end_time "
                f"= {end_time}, nếu không LadderQueue quay vòng bucket và đảo thứ tự event "
                "(report.md §9.4 #1)."
            )

        bindings = _bind()
        self.end_time = end_time
        self.bucket_width = bucket_width
        self.num_buckets = num_buckets
        self.dispatcher = bindings.SequentialEventDispatcher(
            end_time=end_time, bucket_width=bucket_width
        )
        # Framework không cho truyền num_buckets qua dispatcher; thay queue để tránh bug wrap.
        self.dispatcher.event_queue = bindings.LadderQueue(
            bucket_width=bucket_width, num_buckets=num_buckets
        )
        self._ids: dict[str, int] = {}
        self._adapters: dict[str, LogicalProcessAdapter] = {}

    # ------------------------------------------------------------------ đăng ký
    def add(self, *lps: Any) -> None:
        """Đăng ký LP thuần; id gán theo thứ tự thêm."""
        for lp in lps:
            name = getattr(lp, "name", None)
            if not isinstance(name, str) or not name:
                raise ValueError(f"{lp!r}: LP phải có thuộc tính `name` kiểu str")
            if name in self._ids:
                raise ValueError(f"LP trùng tên {name!r}")
            lp_id = len(self._ids)
            self._ids[name] = lp_id
            adapter = LogicalProcessAdapter(lp_id, lp, self._ids)
            self._adapters[name] = adapter
            self.dispatcher.register_lp(adapter)

    @property
    def lp_ids(self) -> dict[str, int]:
        return dict(self._ids)

    @property
    def adapters(self) -> dict[str, LogicalProcessAdapter]:
        return dict(self._adapters)

    # ------------------------------------------------------------------ chạy
    def run(self) -> dict[str, Any]:
        """Chạy tới khi hết event hoặc vượt `end_time`; trả `stats` của dispatcher."""
        return self.dispatcher.run()

    @property
    def current_time(self) -> float:
        return float(self.dispatcher.current_time)


class CoroutineKernel:
    """Chạy cùng một tập LP trên `BkSysCoEventLoop` — **asyncio**, đúng spec §1.1.2.

    Khác `SequentialKernel` ở chỗ: event loop là coroutine (`asyncio.run`), mỗi LP là một
    `coroutine_run`, và dispatcher dùng `asyncio.Future` + `asyncio.gather`/
    `as_completed` (`CoroutineDispatcherLP`). Dùng để đối chứng cơ chế với bản đồng bộ;
    kết quả số phải trùng khớp.

    Cảnh báo: loop này dùng `asyncio.wait_for` khi `wait_for_event(..., timeout=...)` —
    không dùng timeout wall-clock trong mô phỏng của ta (DEV.md §2).
    """

    def __init__(
        self,
        *,
        end_time: float,
        bucket_width: float = LADDER_BUCKET_WIDTH,
    ) -> None:
        if end_time <= 0.0:
            raise ValueError(f"end_time phải > 0, nhận {end_time}")
        if bucket_width <= 0.0:
            raise ValueError(f"bucket_width phải > 0, nhận {bucket_width}")
        bindings = _bind()
        self.end_time = end_time
        self.loop = bindings.BkSysCoEventLoop(end_time=end_time, bucket_width=bucket_width)
        self._ids: dict[str, int] = {}
        self._adapters: dict[str, Any] = {}

    @property
    def lp_ids(self) -> dict[str, int]:
        return dict(self._ids)

    def add(self, *lps: Any) -> None:
        """Đăng ký LP; LP có `bind()` (dispatcher coroutine) nhận thêm `emit`/`loop`."""
        for lp in lps:
            name = getattr(lp, "name", None)
            if not isinstance(name, str) or not name:
                raise ValueError(f"{lp!r}: LP phải có thuộc tính `name` kiểu str")
            if name in self._ids:
                raise ValueError(f"LP trùng tên {name!r}")
            lp_id = len(self._ids)
            self._ids[name] = lp_id
            if hasattr(lp, "bind"):
                adapter = lp
                adapter.bind(lp_id=lp_id, emit=self._make_emit(lp_id), loop=self.loop)
                setter = getattr(adapter, "set_event_loop", None)
                if setter is not None:
                    setter(self.loop)
            else:
                adapter = CoroutineAdapter(lp_id, lp, self._ids, self.loop)
            self._adapters[name] = adapter
            self.loop.register_lp(adapter)

    def _make_emit(self, sender_id: int):
        def emit(emission: Emission) -> None:
            receiver_id = self._ids.get(emission.destination)
            if receiver_id is None:
                raise KeyError(
                    f"không có LP tên {emission.destination!r}. Đã đăng ký: {sorted(self._ids)}"
                )
            self.loop.schedule_event(
                make_event(
                    timestamp=emission.timestamp,
                    sender_id=sender_id,
                    receiver_id=receiver_id,
                    event_type=event_type_for(emission.payload),
                    payload=emission.payload,
                )
            )

        return emit

    def run(self) -> dict[str, Any]:
        return self.loop.run()

    def make_emit(self, sender_id: int):
        """Hàm phát `Emission` từ ngoài (dùng cho test/harness nạp sự kiện ban đầu)."""
        return self._make_emit(sender_id)

    @property
    def current_time(self) -> float:
        return float(self.loop.current_time)

    @property
    def lp_ids(self) -> dict[str, int]:
        return dict(self._ids)

    @property
    def adapters(self) -> dict[str, Any]:
        return dict(self._adapters)
