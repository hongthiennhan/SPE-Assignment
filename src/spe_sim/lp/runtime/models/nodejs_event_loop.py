"""`nodejs_event_loop` — Node.js (V8 + libuv), spec §1.1.1.

`c_loop = 1`, `S_loop = 3.2 ms` chia hai pha 1.92 (ingress) / 1.28 (callback); nhả loop khi
chờ I/O (trừ khi bị offload sang libuv với `p_uv`, khi đó 12 ms chạy trên Worker Pool
`c_uv = 4` và loop được nhả). Trần lý thuyết `1000 / 3.2 = 312.5 req/s`.
"""

from __future__ import annotations

from spe_sim.lp.runtime.models.base import RuntimeModel

__all__ = ["NodejsEventLoopModel"]


class NodejsEventLoopModel(RuntimeModel):
    model_name = "nodejs_event_loop"
    supports_pin = False
    supports_uv = True
