"""Adapter duy nhất tới framework `bkitevsim`.

VÌ SAO CÓ FILE NÀY
------------------
`docs/team/Thống nhất DEV.md` §2 bắt buộc mọi độ trễ phải đi qua
`Event(timestamp=...)` trên `LadderQueue` / `BkSysCoEventLoop` / `BkSysCoLogicalProcess`
(thư viện `bkitevsim`). Tuy nhiên package này **chưa có trong workspace và chưa cài**,
và tài liệu nhóm chỉ nêu *tên* thành phần chứ không nêu signature.

Do đó toàn bộ phần còn lại của `spe_sim` **không import `bkitevsim` trực tiếp**.
Chúng import module này. Khi nhóm cung cấp package, chỉ cần sửa `_bind()` bên dưới.

API kỳ vọng (suy ra từ DEV.md §2 và Hình 3–5 của `hk261_dasyn_asspe.pdf`):
    Event(timestamp=float, source=LP, destination=LP, payload=Any)
    LadderQueue(): push(event) / pop() -> Event / __bool__ (còn event)
    BkSysCoEventLoop: run()
    BkSysCoLogicalProcess: initialize(), handle_event(event),
                           schedule_event(event), set_local_virt_time(t)

Nếu signature thực tế khác, chỉ `_bind()` và các hàm wrapper phải sửa — không lan ra LP.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

BKITEVSIM_MODULE: Final[str] = "bkitevsim"

EXPECTED_NAMES: Final[tuple[str, ...]] = (
    "Event",
    "LadderQueue",
    "BkSysCoEventLoop",
    "BkSysCoLogicalProcess",
)


class KernelUnavailable(RuntimeError):
    """Kernel `bkitevsim` chưa sẵn sàng (chưa cài hoặc thiếu thành phần)."""


@dataclass(frozen=True, slots=True)
class KernelBindings:
    """Các thành phần kernel đã bind, dùng nội bộ."""

    Event: type
    LadderQueue: type
    BkSysCoEventLoop: type
    BkSysCoLogicalProcess: type
    module: Any


_bindings: KernelBindings | None = None
_error: str | None = None


def _bind() -> KernelBindings:
    """Import và kiểm tra `bkitevsim`. Cache kết quả (kể cả lỗi)."""
    global _bindings, _error
    if _bindings is not None:
        return _bindings
    if _error is not None:
        raise KernelUnavailable(_error)

    try:
        import importlib

        module = importlib.import_module(BKITEVSIM_MODULE)
    except ModuleNotFoundError:
        _error = (
            f"Không tìm thấy package {BKITEVSIM_MODULE!r}. "
            "Nhóm cần cung cấp source/pip của bkitevsim (xem project.md §9.1, §12-K4). "
            "Trong lúc chờ, các module thuần logic (config, events, rng, records, "
            "analytics, station) vẫn dùng và test được."
        )
        raise KernelUnavailable(_error) from None

    missing = [name for name in EXPECTED_NAMES if not hasattr(module, name)]
    if missing:
        _error = (
            f"Package {BKITEVSIM_MODULE!r} thiếu thành phần {missing}. "
            f"API kỳ vọng: {list(EXPECTED_NAMES)} (xem docstring module này)."
        )
        raise KernelUnavailable(_error)

    _bindings = KernelBindings(
        Event=module.Event,
        LadderQueue=module.LadderQueue,
        BkSysCoEventLoop=module.BkSysCoEventLoop,
        BkSysCoLogicalProcess=module.BkSysCoLogicalProcess,
        module=module,
    )
    return _bindings


def is_available() -> bool:
    """True nếu kernel dùng được (đã cài và đủ thành phần)."""
    try:
        _bind()
    except KernelUnavailable:
        return False
    return True


def unavailable_reason() -> str | None:
    """Thông điệp lỗi nếu kernel chưa sẵn sàng, ngược lại None."""
    try:
        _bind()
    except KernelUnavailable as exc:
        return str(exc)
    return None


def make_event(*, timestamp: float, source: Any, destination: Any, payload: Any) -> Any:
    """Tạo `Event` của kernel (điểm duy nhất được phép dựng event)."""
    bindings = _bind()
    return bindings.Event(
        timestamp=timestamp,
        source=source,
        destination=destination,
        payload=payload,
    )


def make_queue() -> Any:
    """Tạo `LadderQueue` (priority queue theo timestamp, Least-Timestamp-First)."""
    return _bind().LadderQueue()
