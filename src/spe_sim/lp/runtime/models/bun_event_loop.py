"""`bun_event_loop` — Bun (JavaScriptCore + Zig), spec §1.1.1.

Cùng cấu trúc event loop đơn luồng như Node nhưng `S_loop = 2.0 ms` (1.20 ingress / 0.80
callback); trần lý thuyết `1000 / 2.0 = 500 req/s`.
"""

from __future__ import annotations

from spe_sim.lp.runtime.models.base import RuntimeModel

__all__ = ["BunEventLoopModel"]


class BunEventLoopModel(RuntimeModel):
    model_name = "bun_event_loop"
    supports_pin = False
    supports_uv = True
