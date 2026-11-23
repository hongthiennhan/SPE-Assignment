"""Bản ghi kết quả từng request và xuất CSV — hợp đồng dữ liệu D3 (project.md §7).

Schema cố định (dev thống kê + dev vẽ biểu đồ đọc trực tiếp):

    seed,runtime_model,join_mode,lam,request_id,t_arrive,t_dispatch,t_complete,r_ms,outcome,n_in_system

- `t_*`    : giây (virtual time) — nhân 1000 chỉ ở `r_ms`.
- `outcome`: completed | timeout | db_drop | dropped  (spec §1.3).
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Final, Iterable

from spe_sim.core.events import Outcome

CSV_COLUMNS: Final[tuple[str, ...]] = (
    "seed",
    "runtime_model",
    "join_mode",
    "lam",
    "request_id",
    "t_arrive",
    "t_dispatch",
    "t_complete",
    "r_ms",
    "outcome",
    "n_in_system",
)

_TIME_FIELDS: Final[frozenset[str]] = frozenset({"t_arrive", "t_dispatch", "t_complete"})


@dataclass(frozen=True, slots=True)
class RequestRecord:
    """Một dòng kết quả. `r_ms` là giá trị dẫn xuất, luôn tính từ `t_complete − t_arrive`."""

    seed: int
    runtime_model: str
    join_mode: str
    lam: float
    request_id: int
    t_arrive: float
    t_dispatch: float
    t_complete: float
    outcome: Outcome
    n_in_system: int

    def __post_init__(self) -> None:
        if self.t_complete < self.t_arrive:
            raise ValueError(
                f"vi phạm trật tự nhân quả: t_complete ({self.t_complete}) "
                f"< t_arrive ({self.t_arrive}) cho request {self.request_id}"
            )
        if self.t_dispatch < self.t_arrive:
            raise ValueError(
                f"vi phạm trật tự nhân quả: t_dispatch ({self.t_dispatch}) "
                f"< t_arrive ({self.t_arrive}) cho request {self.request_id}"
            )

    @property
    def r_s(self) -> float:
        """Thời gian đáp ứng (giây)."""
        return self.t_complete - self.t_arrive

    @property
    def r_ms(self) -> float:
        """Thời gian đáp ứng (ms) — chỉ dùng khi xuất/nhìn."""
        return self.r_s * 1000.0

    def to_row(self) -> dict[str, str]:
        """Chuyển sang dict chuỗi theo đúng thứ tự `CSV_COLUMNS`."""
        row: dict[str, str] = {}
        for name, value in asdict(self).items():
            if name == "outcome":
                row[name] = str(Outcome(value).value)
            elif name in _TIME_FIELDS:
                row[name] = f"{float(value):.6f}"
            elif name == "lam":
                row[name] = f"{float(value):.1f}"
            else:
                row[name] = str(value)
        row["r_ms"] = f"{self.r_ms:.3f}"
        return {column: row[column] for column in CSV_COLUMNS}


def records_from_notices(
    notices: Iterable,
    *,
    seed: int,
    runtime_model: str,
    join_mode: str,
    lam: float,
) -> list[RequestRecord]:
    """Dựng danh sách `RequestRecord` từ `OutcomeNotice` của Dispatcher."""
    out: list[RequestRecord] = []
    for notice in notices:
        out.append(
            RequestRecord(
                seed=seed,
                runtime_model=runtime_model,
                join_mode=join_mode,
                lam=lam,
                request_id=notice.request_id,
                t_arrive=notice.t_arrive,
                t_dispatch=notice.t_dispatch,
                t_complete=notice.t_complete,
                outcome=notice.outcome,
                n_in_system=notice.n_in_system,
            )
        )
    return out


def write_csv(path: str | Path, records: Iterable[RequestRecord]) -> Path:
    """Ghi CSV (UTF-8, newline='' theo chuẩn `csv` module)."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for record in records:
            writer.writerow(record.to_row())
    return destination


def read_csv(path: str | Path) -> list[dict[str, str]]:
    """Đọc lại CSV (dùng cho test và cho dev thống kê)."""
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_columns(path: str | Path) -> None:
    """Kiểm tra header khớp `CSV_COLUMNS` — chạy trước khi bàn giao."""
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle), [])
    if tuple(header) != CSV_COLUMNS:
        raise ValueError(f"header CSV không khớp D3: {header} != {list(CSV_COLUMNS)}")


assert tuple(f.name for f in fields(RequestRecord)) == tuple(
    c for c in CSV_COLUMNS if c != "r_ms"
), "RequestRecord lệch so với schema CSV D3"
