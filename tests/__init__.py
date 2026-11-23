"""Test suite cho spe_sim (unittest, không cần dependency ngoài).

Thêm `src/` vào `sys.path` ngay khi import package `tests` để chạy được cả:
    python -m unittest discover -s tests -t .      (toàn bộ)
    python -m unittest tests.test_kernel_bkitevsim (một module)
mà không phụ thuộc thứ tự import (trước đây chỉ chạy được nhờ may mắn về alphabet).
"""

from __future__ import annotations

import sys
from pathlib import Path

_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))
