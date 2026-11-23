"""Quản lý random seed — tái lập 100% (DEV.md §3).

Nguyên tắc: KHÔNG dùng module `random` toàn cục. Mỗi LP/stream nhận một
`random.Random` riêng, dẫn xuất tất định từ `(seed, đường_dẫn_stream)`.

⚠ Khoảng trống K14 (project.md §12): `DEV.md` nêu nguyên tắc nhưng không nêu thuật
toán tách stream. Module này chọn BLAKE2b-128 của chuỗi `"seed|tag1|tag2|..."`.
Cách làm phải được nhóm xác nhận vì nó ảnh hưởng trực tiếp tới tính tái lập của
dev thống kê (cùng seed ⇒ cùng dãy số ở mọi máy, mọi thứ tự gọi).
"""

from __future__ import annotations

import hashlib
import math
import random
from typing import Final

_DIGEST_SIZE: Final[int] = 16


class SeedStreams:
    """Nhà máy sinh các `random.Random` độc lập, tất định theo `(seed, tags)`."""

    __slots__ = ("_seed",)

    def __init__(self, seed: int) -> None:
        if seed < 0:
            raise ValueError(f"seed phải ≥ 0, nhận {seed}")
        self._seed = seed

    @property
    def seed(self) -> int:
        return self._seed

    def stream(self, *tags: str | int) -> random.Random:
        """Trả về một stream độc lập cho tổ hợp tag (ví dụ `("source",)`, `("lp", i)`)."""
        if not tags:
            raise ValueError("cần ít nhất một tag để định danh stream")
        key = "|".join(str(part) for part in (self._seed, *tags))
        digest = hashlib.blake2b(key.encode("utf-8"), digest_size=_DIGEST_SIZE).digest()
        return random.Random(int.from_bytes(digest, "big"))


def sample_exponential(rng: random.Random, mean_s: float) -> float:
    """Mẫu phân phối mũ với **kỳ vọng** `mean_s` (giây).

    Lưu ý: `random.expovariate` nhận *rate* λ = 1/mean nên phải nghịch đảo.
    """
    if mean_s <= 0.0:
        raise ValueError(f"mean_s phải > 0, nhận {mean_s}")
    return rng.expovariate(1.0 / mean_s)


def sample_interarrival(rng: random.Random, lam_rps: float) -> float:
    """Khoảng giữa hai request liên tiếp τ ~ Exp(λ) (giây)."""
    if lam_rps <= 0.0:
        raise ValueError(f"λ phải > 0, nhận {lam_rps}")
    return rng.expovariate(lam_rps)


def sample_bernoulli(rng: random.Random, p: float) -> bool:
    """Biến cố Bernoulli xác suất `p` (dùng cho p_uv, p_pin)."""
    if not 0.0 <= p <= 1.0:
        raise ValueError(f"p phải trong [0, 1], nhận {p}")
    return rng.random() < p


def sample_percentile(values: list[float], q: float, *, method: str = "linear") -> float:
    """Phân vị `q` (0..1) — ⚠ quy ước K10 chưa được spec chốt.

    Mặc định **linear interpolation** (giống `numpy.percentile`). Truyền
    `method="nearest_rank"` để dùng quy ước nearest-rank (phân vị luôn là một mẫu thật).
    Hai cách lệch nhau rất ít trên mẫu lớn — xem `tests/test_percentile.py`.
    """
    if not values:
        raise ValueError("cần mẫu khác rỗng")
    if not 0.0 <= q <= 1.0:
        raise ValueError(f"q phải trong [0, 1], nhận {q}")
    if method not in ("linear", "nearest_rank"):
        raise ValueError(f"method phải là 'linear' hoặc 'nearest_rank', nhận {method!r}")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    if method == "nearest_rank":
        rank = max(1, math.ceil(q * len(ordered)))
        return ordered[rank - 1]
    pos = q * (len(ordered) - 1)
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    frac = pos - lo
    return ordered[lo] * (1.0 - frac) + ordered[hi] * frac
