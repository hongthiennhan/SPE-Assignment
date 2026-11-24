"""`RuntimeModel` — lớp mỏng cấu hình 4 runtime model quanh `RuntimeDispatcherLP` (T13).

Vì `RuntimeDispatcherLP` đọc thẳng `RuntimeProfile` (kênh, `S_cpu`, hai pha CPU, sequential,
holds-slot), phần khác nhau giữa 4 model **đã** nằm trong `PROFILES`; lớp này chỉ:

1. khai báo model nào dùng `p_pin` (Loom) và `p_uv` (Node/Bun) — tránh truyền nhầm tham số,
2. dựng dispatcher đã cấu hình sẵn, kèm `rng` tất định cho pinning/offload,
3. là **một chỗ duy nhất** để tra cứu model theo tên chuỗi (`DEV.md` §4).

Nhờ vậy T14–T17 không phải viết lại logic điều phối, chỉ khác nhau ở tham số và ngữ nghĩa
đã ghi trong `RuntimeProfile`.
"""

from __future__ import annotations

import random
from typing import Any, Final

from spe_sim.core.config import RUNTIME_MODELS, JoinMode, RuntimeProfile, get_profile
from spe_sim.lp.runtime.dispatcher import RuntimeDispatcherLP

__all__ = [
    "RuntimeModel",
    "MODEL_REGISTRY",
    "build_model",
    "model_names",
]


class RuntimeModel:
    """Cấu hình một runtime model; `build_dispatcher()` trả dispatcher sẵn dùng."""

    model_name: Final[str] = ""
    supports_pin: Final[bool] = False
    supports_uv: Final[bool] = False

    def __init__(self, *, p_pin: float = 0.0, p_uv: float = 0.0, cpu_scale: float = 1.0) -> None:
        if not 0.0 <= p_pin <= 1.0:
            raise ValueError(f"p_pin phải trong [0, 1], nhận {p_pin}")
        if not 0.0 <= p_uv <= 1.0:
            raise ValueError(f"p_uv phải trong [0, 1], nhận {p_uv}")
        if p_pin > 0.0 and not self.supports_pin:
            raise ValueError(
                f"{self.model_name} không có Thread Pinning ⇒ p_pin phải = 0 (nhận {p_pin})"
            )
        if p_uv > 0.0 and not self.supports_uv:
            raise ValueError(
                f"{self.model_name} không có libuv Worker Pool ⇒ p_uv phải = 0 (nhận {p_uv})"
            )
        self.p_pin = p_pin
        self.p_uv = p_uv
        self.cpu_scale = cpu_scale
        self.profile  # noqa: B018 - kiểm cpu_scale hợp lệ với model ngay lúc dựng

    # ------------------------------------------------------------------ cấu hình
    @property
    def profile(self) -> RuntimeProfile:
        return get_profile(
            self.model_name, p_pin=self.p_pin, p_uv=self.p_uv, cpu_scale=self.cpu_scale
        )

    @property
    def label(self) -> str:
        return self.profile.label

    def build_dispatcher(
        self,
        join_mode: JoinMode,
        *,
        rng: random.Random | None = None,
        **kwargs: Any,
    ) -> RuntimeDispatcherLP:
        """Dispatcher đã gắn `p_pin` / `p_uv` đúng model này."""
        if (self.p_pin > 0.0 or self.p_uv > 0.0) and rng is None:
            raise ValueError(
                f"{self.model_name}: cần `rng` khi p_pin/p_uv > 0 để chạy lại cùng seed "
                "cho kết quả trùng (DEV.md §3)"
            )
        return RuntimeDispatcherLP(
            self.profile,
            join_mode,
            pin_probability=self.p_pin if self.supports_pin else 0.0,
            uv_probability=self.p_uv if self.supports_uv else 0.0,
            rng=rng,
            **kwargs,
        )

    def build_coroutine_dispatcher(
        self,
        join_mode: JoinMode,
        *,
        rng: random.Random | None = None,
        **kwargs: Any,
    ) -> Any:
        """Bản **asyncio** của dispatcher (spec §1.1.2) — cùng profile, cùng kết quả số."""
        from spe_sim.lp.runtime.coroutine_dispatcher import CoroutineDispatcherLP

        if (self.p_pin > 0.0 or self.p_uv > 0.0) and rng is None:
            raise ValueError(
                f"{self.model_name}: cần `rng` khi p_pin/p_uv > 0 để chạy lại cùng seed "
                "cho kết quả trùng (DEV.md §3)"
            )
        return CoroutineDispatcherLP(
            self.profile,
            join_mode,
            pin_probability=self.p_pin if self.supports_pin else 0.0,
            uv_probability=self.p_uv if self.supports_uv else 0.0,
            rng=rng,
            **kwargs,
        )

    def describe(self) -> dict[str, Any]:
        profile = self.profile
        return {
            "model_name": self.model_name,
            "label": profile.label,
            "channels": profile.channels,
            "ingress_ms": profile.ingress_s * 1000.0,
            "callback_ms": profile.callback_s * 1000.0,
            "sequential_branches": profile.sequential_branches,
            "holds_slot_during_io": profile.holds_slot_during_io,
            "p_pin": self.p_pin,
            "p_uv": self.p_uv,
            "cpu_scale": self.cpu_scale,
            "theoretical_capacity_rps": profile.theoretical_capacity_rps,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}(p_pin={self.p_pin}, p_uv={self.p_uv})"


def model_names() -> tuple[str, ...]:
    return RUNTIME_MODELS


def build_model(
    name: str, *, p_pin: float = 0.0, p_uv: float = 0.0, cpu_scale: float = 1.0
) -> RuntimeModel:
    """Tra model theo tên chuỗi cố định của `DEV.md` §4."""
    try:
        cls = MODEL_REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"runtime_model không hợp lệ: {name!r}; phải là {RUNTIME_MODELS}") from exc
    return cls(p_pin=p_pin, p_uv=p_uv, cpu_scale=cpu_scale)


# Đăng ký ở cuối module để tránh import vòng (các model import `RuntimeModel`).
def _register() -> None:
    from spe_sim.lp.runtime.models.blocking_baseline import BlockingBaselineModel
    from spe_sim.lp.runtime.models.bun_event_loop import BunEventLoopModel
    from spe_sim.lp.runtime.models.nodejs_event_loop import NodejsEventLoopModel
    from spe_sim.lp.runtime.models.spring_loom_vt import SpringLoomVtModel

    MODEL_REGISTRY.update(
        {
            BlockingBaselineModel.model_name: BlockingBaselineModel,
            NodejsEventLoopModel.model_name: NodejsEventLoopModel,
            BunEventLoopModel.model_name: BunEventLoopModel,
            SpringLoomVtModel.model_name: SpringLoomVtModel,
        }
    )


MODEL_REGISTRY: Final[dict[str, type[RuntimeModel]]] = {}
_register()
