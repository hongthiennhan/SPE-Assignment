"""`blocking_baseline` — Java Blocking (Thread-per-Request), spec §1.1.1.

`c_os = 12` OS thread; mỗi request giữ **một** thread từ CPU ingress tới hết callback
(`holds_slot_during_io = True`), gọi 3 nhánh **tuần tự** và `S_cpu = 2.5 ms`.

Trace kỳ vọng: `2.5 + 12 + 22 + 27 = 63.50 ms` (Hình 4). Trần lý thuyết:
`12 × (1000 / 63.5) ≈ 188.98 req/s`.
"""

from __future__ import annotations

from spe_sim.lp.runtime.models.base import RuntimeModel

__all__ = ["BlockingBaselineModel"]


class BlockingBaselineModel(RuntimeModel):
    model_name = "blocking_baseline"
    supports_pin = False
    supports_uv = False
