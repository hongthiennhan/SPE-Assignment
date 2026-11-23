"""`spring_loom_vt` — Java Spring Loom (Virtual Threads M:N), spec §1.1.1.

`c_carrier = 4`, `S_cpu = 2.5 ms` (1.5 trước I/O + 1.0 sau unmount), gọi 3 nhánh song song.
Carrier được nhả khi chờ I/O (**unmount**), trừ khi Thread Pinning xảy ra với xác suất
`p_pin` — khi đó carrier bị giữ suốt pha I/O (K12).

Trần lý thuyết `4 × (1000 / 2.5) = 1600 req/s`; tại `p_pin = 20%` spec cho `389.59 req/s`.
"""

from __future__ import annotations

from spe_sim.lp.runtime.models.base import RuntimeModel

__all__ = ["SpringLoomVtModel"]


class SpringLoomVtModel(RuntimeModel):
    model_name = "spring_loom_vt"
    supports_pin = True
    supports_uv = False
