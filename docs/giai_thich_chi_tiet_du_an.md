# Giải thích chi tiết dự án `XX-004679A-sim` (bkitevsim – phần Dev 1)

Nhóm XX-004679A · CO3007 Đánh giá hiệu năng hệ thống · Dev 1: lõi DES, 6 mô hình hàng đợi đơn và validation.

Tài liệu này đi qua **từng file, từng lớp, từng hàm** mà Dev 1 dùng hoặc viết. Với mỗi phần, tài liệu trả lời hai
câu hỏi: nó **làm gì**, và **tại sao làm như vậy**. Phần sơ đồ kiến trúc và bảng kết quả tóm tắt nằm ở
[architecture_and_validation.md](architecture_and_validation.md). Bảng số liệu đầy đủ nằm ở
[validation_results.md](validation_results.md).

---

## Mục lục

0. [Bối cảnh và các quyết định chung](#0-bối-cảnh-và-các-quyết-định-chung)
1. [Cấu trúc thư mục](#1-cấu-trúc-thư-mục)
2. [`bkitevsim/core/event.py`: lớp `Event`](#2-bkitevsimcoreeventpy-lớp-event)
3. [`bkitevsim/queues/`: `LadderQueue` và `PriorityQueue`](#3-bkitevsimqueues-ladderqueue-và-priorityqueue)
4. [`bkitevsim/core/logical_coProcess.py`: event loop và logical process](#4-bkitevsimcorelogical_coprocesspy-event-loop-và-logical-process)
5. [`bkitevsim/models/single_queues.py`: 6 mô hình hàng đợi đơn](#5-bkitevsimmodelssingle_queuespy-6-mô-hình-hàng-đợi-đơn)
6. [`scenarios/coroutine_network_sim.py`: demo mạng](#6-scenarioscoroutine_network_simpy-demo-mạng)
7. [`scenarios/validation_single_queues.py`: validation](#7-scenariosvalidation_single_queuespy-validation)
8. [Các file còn lại của framework](#8-các-file-còn-lại-của-framework)
9. [Một lần chạy đi qua hệ thống như thế nào](#9-một-lần-chạy-đi-qua-hệ-thống-như-thế-nào)
10. [Hạn chế đã biết và lưu ý cho Dev 2 / Dev 3](#10-hạn-chế-đã-biết-và-lưu-ý-cho-dev-2--dev-3)

---

## 0. Bối cảnh và các quyết định chung

### 0.1 Nhiệm vụ của Dev 1 (theo `dev1.md`)

1. Vá 4 lỗi nền trong `bkitevsim/core/logical_coProcess.py` và 1 lỗi trong `scenarios/coroutine_network_sim.py`.
2. Cài đặt 6 mô hình hàng đợi đơn: M/M/1, M/M/1/K, M/M/∞, M/M/n/n, M/M/c, M/M/c/K.
3. Viết 6 hàm nghiệm giải tích và script validation: R = 30 seed, CI 95% Student-t, ngưỡng sai số 5% / 8%.
4. Viết tài liệu kiến trúc, sơ đồ lớp và phần Validation cho Phase 2.

### 0.2 Framework gốc và phạm vi chỉnh sửa

Dự án chạy trên **framework gốc của môn học BKsyspeEvsim** (thư mục `hk261_spe_BKsyspeEvsim`, tác giả pdnguyen,
HCMUT). Toàn bộ framework được chép nguyên vào `XX-004679A-sim`. Theo quy định sở hữu file của nhóm, Dev 1 chỉ:

| Loại | File |
|---|---|
| Sửa | `bkitevsim/core/logical_coProcess.py` (FIX-1…4), `scenarios/coroutine_network_sim.py` (FIX-5) |
| Tạo mới | `bkitevsim/models/single_queues.py`, `scenarios/validation_single_queues.py` (+ `bkitevsim/models/__init__.py`) |

Mọi file khác (`event.py`, `logical_process.py`, `simulation.py`, `queues/`, `dispatchers/`, `apps/`) **giống hệt
bản gốc từng byte**. Các chỗ sửa được đánh dấu `[FIX-1]` … `[FIX-5]` trong mã để dễ đối chiếu với bản gốc.

> Lịch sử: trước khi có mã nguồn gốc, dự án từng dùng một bản `bkitevsim` tự viết (stand-in). Sau khi chuyển sang
> framework gốc, 6 mô hình cho **kết quả validation giống hệt bit-by-bit** với bản cũ. Lý do là cùng luồng ngẫu
> nhiên, cùng thứ tự rút số và cả hai đều là DES chính xác. Đây cũng là một phép kiểm chứng chéo cho cả hai bản.

### 0.3 Ba quy ước bắt buộc và cách dự án tuân thủ

| Quy ước | Cách thực hiện trong mã | Lý do |
|---|---|---|
| **Đơn vị SI** (giây, req/s) | Mọi tham số là giây: `1/μ = 0.010`, `T_sim`, `end_time`… Chỉ `fmt()` và các lệnh `print` nhân 1000 để in ra ms. | Ba dev ghép mã với nhau. Nếu một người dùng ms và người kia dùng s, kết quả lệch 1000 lần mà không báo lỗi. |
| **Chỉ dùng Virtual Time** | Mọi trễ đi qua `advance_time` / `wait_until` (timer trên `_time_waiters`) hoặc qua `Event` có `timestamp` tương lai trên `_event_queue`. Timeout dùng `schedule_virtual_timer` (FIX-3). Không có `time.sleep`, `asyncio.sleep(d > 0)`, `asyncio.wait_for/wait(timeout=…)`. Chỉ có `asyncio.sleep(0)` (nhường lượt, không chờ). | Mô phỏng phải chạy nhanh hơn thời gian thực rất nhiều lần. Kết quả không được phụ thuộc vào tốc độ máy. |
| **Seed riêng** | Mỗi lần chạy tạo `random.Random(...)` riêng: một luồng cho dòng đến, một luồng cho thời gian phục vụ. Không gọi hàm của module `random` toàn cục. | Cùng seed thì kết quả giống hệt (đã kiểm tra bằng `assert a == b`). Chạy song song nhiều tiến trình cũng không làm thay đổi kết quả. |

### 0.4 Môi trường

Cần **Python ≥ 3.11** vì FIX-4 dùng `asyncio.Runner`. Trên máy này lệnh `python` mặc định là 3.8, nên phải chạy
bằng `py -3.12`. Dự án không cần thư viện ngoài. Nếu máy có `scipy`, script sẽ dùng nó để tính phân vị t.

---

## 1. Cấu trúc thư mục

```
XX-004679A-sim/
├── bkitevsim/                         # framework BKsyspeEvsim
│   ├── core/
│   │   ├── __init__.py                # xuất API: Event, BkSysCoEventLoop, BkSysCoLogicalProcess, TIMEOUT_ERROR…
│   │   ├── event.py                   # Event (gốc)
│   │   ├── logical_process.py         # BkSysLogicalProcess, BkSysLogicalProcessState (gốc)
│   │   ├── logical_coProcess.py       # BkSysCoEventLoop + BkSysCoLogicalProcess  ← FIX-1…4
│   │   └── simulation.py              # Simulation / SimulationOptions (gốc, Dev 1 không dùng)
│   ├── queues/
│   │   ├── ladder_queue.py            # LadderQueue (gốc)
│   │   └── priority_queue.py          # PriorityQueue: bọc LadderQueue + heapq (gốc)
│   ├── dispatchers/
│   │   └── sequential_eventdispatcher.py  # dispatcher đồng bộ (gốc, Dev 1 không dùng)
│   └── models/                        # MỚI (Dev 1)
│       ├── __init__.py
│       └── single_queues.py           # 6 mô hình hàng đợi đơn
├── apps/                              # 2 ví dụ gốc của framework
├── scenarios/
│   ├── __init__.py                    # (gốc)
│   ├── coroutine_network_sim.py       # demo Socket ⇄ App  ← FIX-5
│   └── validation_single_queues.py    # MỚI: nghiệm giải tích + validation
├── docs/
│   ├── architecture_and_validation.md
│   ├── validation_results.md          # do script sinh tự động
│   └── giai_thich_chi_tiet_du_an.md   # tài liệu này
├── Makefile, README.md, LICENSE       # (gốc)
└── .gitignore
```

**Tại sao đặt mô hình trong `bkitevsim/models` mà không phải `scenarios`?**
- `core` và `queues` không biết gì về hàng đợi. Chúng chỉ biết sự kiện, thời gian ảo và coroutine. Nhờ vậy Dev 2 và
  Dev 3 có thể dựng mô hình khác (fan-out/join, mạng hàng đợi) trên cùng lõi.
- `models` là các mô hình dùng lại được, không in gì ra màn hình, và có thể `import` từ bất kỳ kịch bản nào.
- `scenarios` là các chương trình cụ thể: chọn tham số, chạy, in bảng, assert.

---

## 2. `bkitevsim/core/event.py`: lớp `Event`

Một `Event` là một thông điệp có nhãn thời gian, gửi từ LP này sang LP khác (có thể là chính nó). File này là
**bản gốc**, Dev 1 không sửa.

```python
@dataclass(order=True)
class Event:
    timestamp: float
    sender_id: int
    receiver_id: int
    event_type: str
    payload: Any = field(default=None, compare=False)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
```

| Thuộc tính | Ý nghĩa | Cách mô hình của Dev 1 dùng |
|---|---|---|
| `timestamp` | Thời điểm ảo (giây) mà sự kiện xảy ra | `now` cho `ARRIVAL`, `now + st` cho `END_SERVICE` |
| `sender_id`, `receiver_id` | ID số nguyên của LP gửi và nhận | Nguồn đến có `lp_id = 0`, Server có `lp_id = 1` |
| `event_type` | Loại sự kiện (chuỗi) | `"ARRIVAL"` hoặc `"END_SERVICE"` |
| `payload` | Dữ liệu tùy ý, không tham gia so sánh | Đối tượng `Customer` |
| `event_id` | UUID duy nhất, không tham gia so sánh | Không dùng |

### `@dataclass(order=True)`
Sinh các phép so sánh `<`, `<=`… theo thứ tự các trường có `compare=True`: `timestamp`, rồi `sender_id`,
`receiver_id`, `event_type`. Hàng đợi thực tế không dựa vào phép so sánh này, vì `LadderQueue` tự phá hòa bằng bộ
đếm (xem mục 3). Nhưng nhờ nó, `Event` vẫn sắp xếp được nếu ai đó đưa vào `sorted()` hay `heapq`.

### `__post_init__`
Ném `ValueError` nếu `timestamp < 0` hoặc ID âm. **Hệ quả cho mô hình:** ID của LP phải là số nguyên không âm. Đây
là lý do bản mới dùng `0` và `1` thay cho tên chuỗi như bản stand-in.

### `event_id` dùng `uuid4`
`uuid4` lấy ngẫu nhiên từ hệ điều hành chứ không từ module `random`, và nó `compare=False`. Vì vậy nó **không ảnh
hưởng** tới thứ tự sự kiện hay luồng số ngẫu nhiên của mô hình, và tính lặp lại theo seed vẫn được giữ.

---

## 3. `bkitevsim/queues/`: `LadderQueue` và `PriorityQueue`

Đây là tập sự kiện chờ (pending event set), lấy ra theo nguyên tắc **LTSF (Least TimeStamp First)**. Cả hai file là
**bản gốc**.

### 3.1 `LadderQueue`
Dựa trên Ladder Queue của Tang và cộng sự (2005), ở dạng đơn giản: một tầng bucket.

| Thành phần | Ý nghĩa |
|---|---|
| `bucket_width` | Độ rộng thời gian của một bucket (giây) |
| `num_buckets = 1024` | Số bucket, dùng như vòng tròn |
| `_buckets` | `dict`: chỉ số bucket → danh sách `(timestamp, entry_count, event)` đã sắp xếp |
| `_bucket_boundaries` | Danh sách đã sắp xếp các chỉ số bucket đang có dữ liệu |
| `_entry_count` | Bộ đếm tăng dần dùng để phá hòa |
| `_lock` | `threading.RLock`, cho phép dùng từ nhiều luồng |

- **`push(event)`**: tính `idx = int(t / bucket_width) % num_buckets`, rồi `bisect.insort` bộ ba
  `(timestamp, entry_count, event)` vào bucket đó.
  **Tại sao cần `entry_count`?** Khi hai sự kiện trùng `timestamp`, Python so sánh tiếp phần tử thứ hai. Vì
  `entry_count` luôn khác nhau, Python không bao giờ phải so sánh tới `Event`, và sự kiện vào trước sẽ ra trước (FIFO
  khi trùng giờ). Điều này giúp kết quả **lặp lại được bit-by-bit** với cùng seed.
- **`pop()`**: lấy phần tử đầu của bucket có chỉ số nhỏ nhất; xóa bucket nếu rỗng.
- **`peek()` / `peek_timestamp()`**: xem phần tử đầu mà không lấy ra.

> ⚠️ **Giới hạn quan trọng (có trong bản gốc):** chỉ số bucket lấy `% 1024`. Sự kiện có
> `t ≥ 1024 · bucket_width` sẽ "quấn vòng" về bucket nhỏ và bị lấy ra **sai thứ tự**. Ví dụ với `bucket_width = 1.0`,
> sự kiện tại t = 1025 s được coi như sớm hơn sự kiện tại t = 500 s. Dev 1 không được sửa file này, nên mô hình tự
> tránh lỗi bằng cách chọn `bucket_width = T_sim / 256` (xem mục 5.6).

### 3.2 `PriorityQueue`
Lớp bọc có giao diện giống `heapq`:
- Nếu phần tử là `Event` thì đẩy vào `LadderQueue` bên trong.
- Nếu là thứ khác (ví dụ tuple) thì đẩy vào một `heapq` riêng với khóa `(priority, sequence, item)`.

`BkSysCoEventLoop` dùng **hai** `PriorityQueue` tách biệt: `_event_queue` chỉ chứa `Event` (nên dùng
`LadderQueue`), và `_time_waiters` chỉ chứa tuple `(target_time, lp_id, future)` (nên dùng `heapq`). Vì vậy giới hạn
`% 1024` ở trên chỉ ảnh hưởng tới `_event_queue`, còn các timer luôn được sắp xếp đúng.

---

## 4. `bkitevsim/core/logical_coProcess.py`: event loop và logical process

Đây là file quan trọng nhất. Ý tưởng cốt lõi:

> **asyncio chỉ được dùng để chạy coroutine, không được dùng để quyết định thời gian.**
> Thời gian do `BkSysCoEventLoop` nắm giữ trong `_current_time`. Thời gian chỉ tiến lên trong
> `_advance_time_to_next_event()`, khi không còn sự kiện nào ở thời điểm hiện tại và không còn LP nào sẵn sàng chạy.

Nhờ vậy người viết mô hình được viết code tuần tự, dễ đọc (`await self.advance_time(dt)`), trong khi mô phỏng vẫn
là DES thuần túy.

### 4.1 Các hằng số ở đầu file

| Hằng | Giá trị | Mục đích |
|---|---|---|
| `FIRST_COMPLETED`, `FIRST_EXCEPTION`, `ALL_COMPLETED` | lấy từ `asyncio` | Điều kiện trả về của `asyncio.wait` |
| `CANCELLED_ERROR` | `asyncio.CancelledError` | Tên ngắn |
| `TIMEOUT_ERROR` | `asyncio.TimeoutError` | **Tên duy nhất** cho lỗi hết giờ (xem FIX-1) |

`BkSysFuture(asyncio.Future)` là future riêng của framework, hiện không thêm hành vi gì.

### 4.2 Trạng thái LP: `BkSysCoLPStatus`
`READY`, `WAITING_EVENT`, `WAITING_TIME`, `SUSPENDED`, `TERMINATED`. Vòng lặp dùng trạng thái này để quyết định:
còn LP `READY` thì nhường lượt cho chúng chạy; không còn thì mới được tiến đồng hồ.

### 4.3 Lớp `BkSysCoEventLoop`

#### `__init__(end_time=inf, bucket_width=1.0)`
| Thuộc tính | Ý nghĩa |
|---|---|
| `end_time` | Chân trời mô phỏng. Sự kiện và timer sau mốc này không được xử lý. |
| `_current_time` / `current_time` | Đồng hồ ảo (giây), bắt đầu từ 0 |
| `_event_queue` | `PriorityQueue[Event]` (bên trong là `LadderQueue` với `bucket_width`) |
| `_time_waiters` | `PriorityQueue[tuple]` chứa `(target_time, lp_id, future)` |
| `_lp_coroutines`, `_lp_instances`, `_lp_status` | Task, đối tượng LP và trạng thái của từng LP theo `lp_id` |
| `_event_waiters` | `event_type → [(lp_id, future)]` cho các LP đang `wait_for` |
| `stats` | `events_processed`, `wall_time`… |

#### `register_lp(lp)`
Lưu LP theo `lp_id` (trùng ID thì ném `ValueError`), lưu hàm `lp.coroutine_run` (chưa tạo task, vì lúc này chưa có
event loop asyncio), và đặt trạng thái `READY`. **Lưu ý:** hàm này **không** gán event loop cho LP. Người dùng phải
gọi thêm `lp.set_event_loop(loop)`. Mô hình của Dev 1 làm cả hai bước trong `SingleQueueModel.run()`.

#### `schedule_event(event)`
Đẩy sự kiện vào `_event_queue`. Sự kiện được giao khi đồng hồ chạm `timestamp`.

#### `schedule_virtual_timer(lp_id, delay)`  **[FIX-3, hàm mới]**
```python
timer = self.create_future()
deadline = self._current_time + delay
self._time_waiters.push((deadline, lp_id, timer), priority=deadline)
return timer
```
Hẹn một timer theo thời gian ảo; khi đồng hồ chạm `deadline`, timer được resolve. Đây là cùng một cơ chế mà
`wait_until_time` đang dùng, chỉ khác là trả future ra để người gọi tự quyết định làm gì khi hết giờ.
`delay < 0` bị chặn bằng `ValueError`, vì không được hẹn giờ vào quá khứ.
**Tại sao tách thành hàm riêng?** Để `wait_for_event` và `wait_for_multiple` dùng chung một cách hẹn giờ, và để Dev
2/Dev 3 có thể dùng cho timeout của riêng họ (ví dụ timeout của request trong mô hình fan-out).

#### `async wait_for_event(lp_id, event_type, timeout=None)`  **[FIX-1, FIX-3]**
1. Tạo future, đăng ký vào `_event_waiters[event_type]`, đặt trạng thái `WAITING_EVENT`.
2. Nếu có `timeout`: gọi `schedule_virtual_timer(lp_id, timeout)`, gắn callback `_expire`. Khi timer nổ mà event
   chưa đến, `_expire` đặt `TIMEOUT_ERROR` vào future của event.
3. `await future`:
   - Event đến trước: trả về event.
   - Gặp `TIMEOUT_ERROR`: gỡ LP khỏi danh sách chờ, rồi ném lại **`TIMEOUT_ERROR`** kèm thời điểm ảo.
4. `finally`: nếu timer chưa nổ thì `timer.cancel()`.

- **Lỗi FIX-1 trong mã gốc:** nhánh timeout viết `raise EVENT_TIMEOUT_ERROR(...)`, một tên chưa từng được định
  nghĩa. Vì vậy lẽ ra phải báo timeout thì chương trình lại báo `NameError`.
- **Lỗi FIX-3 trong mã gốc:** `asyncio.wait_for(future, timeout=timeout)` đếm theo **đồng hồ thực**. Một mô phỏng
  10 giây ảo có thể chạy chỉ 30 ms thực, nên timeout 1 giây thực không bao giờ nổ. Ngược lại, máy chậm có thể làm
  timeout nổ sai lúc. Kết quả phụ thuộc vào tốc độ máy và không lặp lại được.
- Điều kiện được đổi từ `if timeout:` sang `if timeout is not None:`, để `timeout = 0` có nghĩa "hết giờ ngay" như
  asyncio, thay vì bị coi là "không có timeout".

#### `async wait_until_time(lp_id, target_time)` và `async advance_time(lp_id, delta)`
Đẩy `(target_time, lp_id, future)` vào `_time_waiters`, đặt trạng thái `WAITING_TIME`, rồi `await future`.
`advance_time` chỉ là `wait_until_time(current_time + delta)`. Đây là "sleep ảo" của framework. Nếu
`target_time <= current_time` thì trả về ngay.
**Lưu ý:** timer có `target_time > end_time` sẽ không bao giờ được resolve. LP đó sẽ bị FIX-2 báo là bị kẹt (xem
mục 6, lỗi App LP).

#### `_advance_time_to_next_event()`  **[FIX-3, phần dọn timer]**
1. Lấy thời điểm của sự kiện sớm nhất trong `_event_queue`.
2. **Mới:** bỏ khỏi đầu `_time_waiters` mọi timer đã xong (đã bị hủy). **Lý do:** khi event đến trước timeout,
   timer bị `cancel()` nhưng vẫn nằm trong heap. Nếu không dọn, vòng lặp sẽ nhảy đồng hồ tới đúng mốc timeout đó dù
   không còn ai chờ. Việc này lãng phí và làm `current_time` lúc kết thúc bị sai.
3. Lấy thời điểm của timer sớm nhất còn hiệu lực.
4. `next_time = min(hai giá trị trên)`, chỉ xét các giá trị `≤ end_time`. Nếu không còn gì thì trả `False`.
5. Resolve mọi timer có `target_time ≤ next_time` (đặt LP về `READY`), rồi đặt `current_time = next_time`.

#### `_process_events_at_current_time()`
Lặp: lấy mọi `Event` có `timestamp ≤ current_time` ra khỏi `_event_queue`. Với mỗi event:
1. Đặt `local_virt_time` của LP nhận bằng `event.timestamp`.
2. Gọi `lp.handle_event(event)`. **Lưu ý:** mọi exception ở đây bị bắt và **chỉ ghi log**, không dừng mô phỏng.
   Vì vậy `Server` của Dev 1 tự giữ lại lỗi để báo sau (mục 5.4).
3. Đánh thức các LP đang `wait_for` đúng loại event này **và** là LP nhận.

#### `run(max_iterations=None)`  **[FIX-4]**
```python
with asyncio.Runner() as runner:
    return runner.run(self._run_async(max_iterations))
```
- **Mã gốc:** `asyncio.run(...)`.
- **Tại sao dùng `Runner`:** `asyncio.Runner` (Python 3.11+) là context manager, đóng event loop và các async
  generator một cách tất định khi ra khỏi khối `with`, và cho phép tái sử dụng loop nếu sau này cần chạy nhiều pha.
  Mỗi lần gọi `run()` tạo một loop mới và sạch, nên các lần chạy liên tiếp (30 seed) không ảnh hưởng nhau.
- Đây là lý do dự án yêu cầu Python ≥ 3.11.

#### `async _run_async(max_iterations)`: vòng lặp chính  **[FIX-2]**
1. `_create_lp_tasks()`: tạo task cho `coroutine_run()` của mọi LP, rồi `await asyncio.sleep(0)` để chúng chạy tới
   lần `await` đầu tiên và lên lịch các sự kiện ban đầu.
2. Lặp:
   - Đánh dấu `TERMINATED` cho các task đã xong (ghi log nếu task lỗi).
   - `_process_events_at_current_time()`. Có xử lý thì nhường lượt 3 lần (`asyncio.sleep(0)`) rồi lặp lại.
   - Còn LP `READY` thì nhường lượt rồi lặp lại. **Lý do:** một coroutine vừa được đánh thức phải chạy tới lần
     `await` kế tiếp (và gửi xong các event mới) **trước khi** đồng hồ được phép tiến. Nếu không, nó có thể lên lịch
     một event ở thời điểm nhỏ hơn thời điểm đồng hồ vừa nhảy tới, tức là sai nhân quả.
   - Không còn gì ở thời điểm hiện tại: `_advance_time_to_next_event()`. Nếu trả `False` thì kết thúc:
     ```python
     alive_lps = [lp_id for lp_id, status in self._lp_status.items()
                  if status != BkSysCoLPStatus.TERMINATED]
     if not alive_lps:   # [FIX-2]
         logger.info("All LPs terminated - simulation complete")
     else:
         logger.warning("No events or timers left before end_time; stranded LPs: {...}")
     ```
3. `finally`: hủy và `gather` các task còn sống, ghi `wall_time`, rồi trả về `stats`.

- **Lỗi FIX-2 trong mã gốc:** viết `if alive_lps:` rồi log "complete", tức logic bị ngược. Khi còn LP bị kẹt,
  chương trình báo đã xong; khi xong thật, chương trình lại báo kẹt. Chạy bản gốc với demo mạng sẽ thấy dòng "All
  LPs terminated" trong khi App LP vẫn đang chờ.
- **Sau khi sửa:** "complete" chỉ được log khi không còn LP nào sống. Nếu còn, cảnh báo kèm `lp_id` và thứ LP đó
  đang chờ (`_lp_waiting_for`). Cảnh báo này giúp phát hiện deadlock trong mô hình.

### 4.4 Lớp `BkSysCoLogicalProcess(BkSysLogicalProcess)`

Mỗi thực thể mô phỏng là một LP, có `lp_id`, `local_virt_time` và `state` (kế thừa từ `BkSysLogicalProcess` gốc).

| Hàm | Ý nghĩa |
|---|---|
| `__init__(lp_id, initial_state=None, lookahead=0.001)` | `lookahead` là độ trễ tối thiểu khi gửi event, dùng trong demo mạng |
| `set_event_loop(loop)` | Gắn LP với event loop; bắt buộc trước khi chạy |
| `send_event(event)` | Đẩy event vào `loop.schedule_event` |
| `handle_event(event)` | Mặc định: cất event vào `_pending_events`. Lớp con thường ghi đè để xử lý trực tiếp |
| `async coroutine_run()` | Hành vi chủ động của LP; lớp con phải ghi đè |
| `async advance_time(dt)`, `async wait_until(t)` | Ngủ theo thời gian ảo |
| `async wait_for(event_type, timeout=None)` | Ủy quyền sang `loop.wait_for_event` (timeout ảo, FIX-3) |
| `async wait_for_multiple(event_types, timeout=None)` | Chờ một trong nhiều loại event |

#### `async wait_for_multiple(event_types, timeout=None)`  **[FIX-3]**
1. Nếu trong `_pending_events` đã có event thuộc loại cần chờ thì lấy ra và trả ngay.
2. Tạo một future cho mỗi loại event và đăng ký vào `_event_waiters`.
3. **Mới:** nếu có `timeout`, gọi `schedule_virtual_timer` và cho timer vào cùng tập chờ.
4. `await asyncio.wait(tập_chờ, return_when=FIRST_COMPLETED)`. Lời gọi này **không có tham số timeout**, nên chỉ có
   nghĩa "ngủ tới khi có ai đó xong". Ai xong là do sự kiện ảo quyết định.
5. Nếu chỉ có timer xong mà chưa có event nào: hủy các future, dọn `_event_waiters`, ném **`TIMEOUT_ERROR`**.
6. Ngược lại: hủy các future còn lại, trả event đầu tiên, cất các event thừa vào `_pending_events`.
7. `finally`: hủy timer nếu chưa nổ.

- **Lỗi trong mã gốc:** `asyncio.wait(..., timeout=timeout)` đếm theo đồng hồ thực. Hơn nữa, `asyncio.wait` khi hết
  giờ **không ném** lỗi mà trả `done` rỗng, nên mã gốc rơi vào `raise RuntimeError("No future completed")` thay vì
  `TIMEOUT_ERROR` như docstring hứa. Bản sửa ném đúng `TIMEOUT_ERROR`.

#### Kiểm tra nhanh các bản vá FIX-3 (thời gian ảo, tổng thời gian thực ≈ 3 ms)
| Tình huống | Kết quả |
|---|---|
| `wait_for("NEVER", timeout=5.0)` từ t = 0 | `TIMEOUT_ERROR` đúng tại t = 5.0 s |
| `wait_for_multiple(["X","Y"], timeout=2.5)` từ t = 5.0 | `TIMEOUT_ERROR` tại t = 7.5 s |
| Event `PING` đến sau 1 s, `timeout=50` | Nhận `PING` tại t = 8.5 s; timer 50 s bị hủy, không làm đồng hồ nhảy |
| Event `Y` đến sau 0.25 s, `wait_for_multiple(["X","Y"], timeout=40)` | Nhận `Y` tại t = 8.75 s |
| `advance_time(0.5)` sau đó | t = 9.25 s, chứng tỏ hai timer đã hủy không đẩy đồng hồ |

---

## 5. `bkitevsim/models/single_queues.py`: 6 mô hình hàng đợi đơn

Thiết kế bám theo **Queuing System Reference Design** của đề:
*ArrivalEvent* (`mean_interarrivaltime`), *Server* (`mean_servicetime`, `maxServer`, `arrival()` / `schedule()` /
`run()`, `beginService()` / `endService()`) và *ProcessingEvent* (`create()`).

**Ý tưởng then chốt:** cả 6 mô hình chỉ khác nhau ở hai tham số của **cùng một lớp `Server`**: số server
`maxServer` (c) và sức chứa `capacity` (K). Vì vậy chỉ có một bản cài đặt logic hàng đợi. Lỗi nào được sửa thì
sửa cho cả 6, và validation 6 mô hình cũng là validation cho chính lớp `Server` đó.

**Hai LP, hai loại event:**

| LP | `lp_id` | Kiểu | Gửi / nhận |
|---|---|---|---|
| `ArrivalEvent` | 0 | coroutine (`coroutine_run`) | Gửi `ARRIVAL` tới LP 1 |
| `Server` | 1 | hướng sự kiện (`handle_event`) | Nhận `ARRIVAL`; tự gửi và nhận `END_SERVICE` |

### 5.1 Hằng số ở đầu file
- `ARRIVAL = "ARRIVAL"`, `END_SERVICE = "END_SERVICE"`: giá trị `event_type`.
- `ARRIVAL_LP_ID = 0`, `SERVER_LP_ID = 1`: ID không âm, theo yêu cầu của `Event.__post_init__`.
- `LADDER_RING_FRACTION = 256`: dùng để chọn `bucket_width` (xem 5.6).

### 5.2 `Customer`
Lưu `cid`, `t_arrive`, `t_start`, `t_done`. Dùng `__slots__` vì mỗi lần chạy có khoảng 50 000 khách. Ba mốc thời
gian đủ để tính thời gian chờ `Wq = t_start − t_arrive` và thời gian lưu hệ thống `W = t_done − t_arrive`. Khách
được đi kèm trong `Event.payload`.

### 5.3 `ProcessingEvent` và `QueueStats`

**`ProcessingEvent`**
- `__init__(trace=False)`: `counts` đếm 4 loại `ARRIVAL`, `BLOCKED`, `BEGIN`, `END`. `log` chỉ được ghi khi
  `trace=True`.
- `create(kind, t, customer)`: tăng bộ đếm, và nếu bật trace thì ghi `(t, kind, cid)`.
- **Tại sao trace mặc định tắt?** 990 lần chạy × khoảng 200 000 bản ghi sẽ tốn rất nhiều bộ nhớ. Khi cần debug,
  bật `run(..., trace=True)` thì kết quả có thêm khóa `"trace"`.

**`QueueStats`: thống kê theo thời gian**
- `__init__(warmup)`: các diện tích tích lũy `area_n` (∫N(t)dt), `area_busy` (∫số server bận dt), `area_q`
  (∫số khách đang chờ dt), cùng các bộ đếm `arrivals`, `blocked`, `completed`, `resp_sum`, `wait_sum`.
- `advance(now, n, busy)`: cộng diện tích hình chữ nhật của **trạng thái cũ** trên đoạn
  `[max(last_t, warmup), now]`, rồi cập nhật `last_t = now`.

**Tại sao phải tính theo diện tích?** L và U là trung bình **theo thời gian**, không phải trung bình theo số sự
kiện. Ví dụ, hệ thống có 5 khách trong 1 ms và 0 khách trong 99 ms thì L = 0.05.

**Tại sao dùng `max(last_t, warmup)`?** Để chỉ tính phần sau warm-up. Đoạn đầu, khi hệ thống còn rỗng và chưa ổn
định, bị loại bỏ; đoạn vắt qua mốc warm-up được cắt đúng tại mốc.

**Thứ tự quan trọng:** `advance` phải được gọi **trước** khi thay đổi trạng thái (xem `_touch` bên dưới).

### 5.4 `Server(BkSysCoLogicalProcess)`

#### `__init__(lp_id, mean_servicetime, maxServer=1, capacity=None, rng, stats, processing)`
- `maxServer = math.inf` biểu diễn M/M/∞. Phép so sánh `busy < inf` luôn đúng, nên không cần nhánh code riêng.
- `capacity = None` nghĩa là phòng chờ vô hạn. **K là tổng số khách trong hệ thống** (gồm cả khách đang được phục
  vụ), đúng theo quy ước Kendall.
- `queue` là một `deque`, dùng FCFS.
- `error = None`: nơi giữ lỗi đầu tiên xảy ra trong `handle_event`.

#### `now` (property)
Trả về `self.local_virt_time`. Event loop gán giá trị này bằng `event.timestamp` ngay trước khi gọi `handle_event`,
nên đây đúng là "bây giờ" của event đang xử lý.

#### `n_in_system` (property) và `_touch()`
`n_in_system = busy + len(queue)`, tức N(t). `_touch()` gọi `stats.advance(now, n_in_system, busy)`; hàm này được gọi
ở đầu `arrival()` và đầu `endService()`, là hai chỗ duy nhất làm thay đổi N(t).

#### `async coroutine_run()`
Chỉ gọi `self.run()` rồi kết thúc. **Tại sao Server không cần coroutine?** Server không có hành vi chủ động; nó chỉ
phản ứng với hai event: khách đến và phục vụ xong. Viết bằng `handle_event` giúp mỗi lần phục vụ chỉ tốn đúng một
event trên `_event_queue`, không cần future hay task cho từng khách. Task kết thúc ngay nhưng event loop **vẫn tiếp
tục giao event** cho LP qua `_lp_instances`, nên Server hoạt động bình thường.

#### `handle_event(event)`
Phân nhánh theo `event.event_type`: `ARRIVAL → arrival(payload)`, `END_SERVICE → endService(payload)`, loại khác thì
gọi bản gốc (cất vào `_pending_events`).
Hàm bọc trong `try/except`, giữ lỗi đầu tiên vào `self.error` rồi ném lại. **Lý do:** `BkSysCoEventLoop` bắt mọi
exception của `handle_event` và chỉ ghi log. Nếu không giữ lại, một bug trong `Server` sẽ cho ra số liệu sai mà mô
phỏng vẫn "chạy xong" bình thường. `SingleQueueModel.run()` kiểm tra `server.error` và ném `RuntimeError`.

#### `arrival(customer)` (system operator)
1. `_touch()`, để chốt diện tích trước khi trạng thái thay đổi.
2. Nếu khách đến sau warm-up thì tăng `stats.arrivals`. Ghi nhận `ARRIVAL`.
3. **Chặn (blocking):** nếu `capacity` hữu hạn và `n_in_system >= capacity`, khách bị từ chối: tăng `blocked`, ghi
   nhận `BLOCKED`, `return`. Đây là cơ chế cho M/M/1/K, M/M/n/n và M/M/c/K.
4. Nếu không bị chặn: đưa khách vào hàng và gọi `schedule()`.

#### `schedule()` và `run()`
`schedule()`: `while queue and busy < maxServer: beginService(queue.popleft())`. Dùng `while` thay vì `if` để bao
quát trường hợp có nhiều server cùng rảnh. `run()` chỉ gọi `schedule()`, giữ đúng giao diện
`arrival()/schedule()/run()` của Reference Design.

#### `beginService(customer)` (process operator)
Tăng `busy`, ghi `t_start`, ghi nhận `BEGIN`, rút thời gian phục vụ `st ~ Exp(1/mean_servicetime)` từ luồng rng phục
vụ, rồi gửi cho chính mình:
```python
Event(timestamp=now + st, sender_id=1, receiver_id=1, event_type="END_SERVICE", payload=customer)
```
Hàm không gọi `_touch()` vì luôn được gọi ngay sau `_touch()` ở cùng thời điểm (từ `arrival` hoặc `endService`).

#### `endService(customer)` (process operator)
1. `_touch()`. Giảm `busy`, ghi `t_done`, ghi nhận `END`.
2. Nếu khách **đến** sau warm-up thì tăng `completed`, cộng `W` vào `resp_sum` và `Wq` vào `wait_sum`.
   **Tại sao lọc theo thời điểm đến?** Khách đến trong warm-up đã trải qua hệ thống chưa ổn định; tính họ vào sẽ làm W
   lệch.
3. `schedule()`, để đưa khách kế tiếp (nếu có) vào server vừa rảnh.

### 5.5 `ArrivalEvent(BkSysCoLogicalProcess)`: nguồn Poisson
- `__init__(lp_id, mean_interarrivaltime, server_lp_id, rng, end_time)`.
- `async coroutine_run()`:
  ```python
  while True:
      dt = rng.expovariate(rate)
      if loop.current_time + dt > end_time: return
      await self.advance_time(dt)
      generated += 1
      self.send_event(Event(now, 0, server_lp_id, "ARRIVAL", Customer(generated, now)))
  ```
  Khoảng cách giữa hai lần đến có phân phối mũ, nên dòng đến là quá trình Poisson với tốc độ λ.
  - **Tại sao dừng trước `end_time`?** Timer có `target > end_time` sẽ không bao giờ được resolve, và LP sẽ bị FIX-2
    báo là kẹt. Dừng sớm giúp LP tự kết thúc đúng lúc và không tạo sự kiện thừa.
  - **Tại sao event `ARRIVAL` có `timestamp = now`?** Khách đến đúng lúc nguồn thức dậy. Event được giao ngay trong
    vòng lặp kế tiếp, cùng thời điểm ảo, trước khi đồng hồ tiến.
- `handle_event`: bỏ qua, vì nguồn không nhận gì.

### 5.6 `SingleQueueModel`: lớp cơ sở
- `__init__(lam, mu, c=1, K=None)`: `lam` là λ (req/s), `mu` là μ (req/s cho **mỗi** server).
- `run(T_sim, T_warmup, seed, trace=False)`:
  1. Tạo `BkSysCoEventLoop(end_time=T_sim, bucket_width=T_sim / 256)`.
     **Tại sao `T_sim / 256`?** Để tránh lỗi quấn vòng `% 1024` của `LadderQueue` (mục 3.1). Với độ rộng này, toàn
     bộ chân trời mô phỏng chỉ chiếm 256/1024 bucket, chừa dư 4 lần cho các `END_SERVICE` được hẹn sau `T_sim`. Số
     event đang chờ cùng lúc rất ít (1 lần đến + tối đa c lần phục vụ), nên bucket rộng không làm chậm.
  2. Tạo `QueueStats(T_warmup)` và `ProcessingEvent`.
  3. **Hai luồng ngẫu nhiên độc lập:** `Random(1_000_003·seed + 1)` cho dòng đến, `Random(1_000_003·seed + 2)` cho
     thời gian phục vụ.
     - **Tại sao tách luồng?** Thay đổi một yếu tố (ví dụ K) không làm xáo trộn dãy thời gian phục vụ. Đây là kỹ
       thuật *common random numbers*, giúp so sánh giữa các cấu hình bớt nhiễu.
     - **Tại sao nhân với 1 000 003?** Để seed s và s+1 không bao giờ cho ra cùng một seed con. Nếu viết
       `seed+1`, `seed+2`, thì luồng phục vụ của seed 1 sẽ trùng với luồng đến của seed 2.
  4. Tạo `Server(1, 1/mu, c, K, …)` và `ArrivalEvent(0, 1/lam, 1, …)`. Với mỗi LP: `loop.register_lp(lp)` và
     `lp.set_event_loop(loop)`.
  5. `loop_stats = loop.run()`. Nếu `server.error` khác `None` thì ném `RuntimeError`.
  6. `stats.advance(T_sim, ...)` để chốt đoạn diện tích cuối cùng tới đúng `T_sim`.
  7. Tính kết quả trên `span = T_sim − T_warmup`:

| Khóa | Công thức | Ý nghĩa |
|---|---|---|
| `L` | `area_n / span` | Số khách trung bình trong hệ thống |
| `Lq` | `area_q / span` | Số khách trung bình đang chờ |
| `W` | `resp_sum / completed` | Thời gian lưu hệ thống trung bình (s) |
| `Wq` | `wait_sum / completed` | Thời gian chờ trung bình (s) |
| `X` | `completed / span` | Thông lượng (req/s) |
| `P_block` | `blocked / arrivals` | Xác suất bị chặn |
| `U` | `area_busy / span / c` | Hiệu suất **mỗi** server; `None` với M/M/∞ vì c = ∞ |
| `events` | `loop_stats["events_processed"]` | Số event `ARRIVAL` + `END_SERVICE` đã giao (không tính timer) |

`max(…, 1)` ở mẫu số giúp tránh chia cho 0 khi chạy cực ngắn.

### 5.7 Sáu lớp con

| Lớp | `kendall` | Gọi `super().__init__(lam, mu, c, K)` với | Giải thích |
|---|---|---|---|
| `MM1` | M/M/1 | `c=1, K=None` | 1 server, hàng vô hạn |
| `MM1K` | M/M/1/K | `c=1, K=K` | 1 server, tối đa K khách |
| `MMInf` | M/M/inf | `c=inf, K=None` | Mọi khách được phục vụ ngay, không ai phải chờ |
| `MMnn` | M/M/n/n | `c=n, K=n` | n server, không có phòng chờ: hệ thống mất khách Erlang-B |
| `MMc` | M/M/c | `c=c, K=None` | c server, hàng vô hạn (Erlang-C) |
| `MMcK` | M/M/c/K | `c=c, K=K` | c server, tối đa K khách |

Mỗi lớp con chỉ khai báo tham số có ý nghĩa với mô hình đó, ví dụ `MMnn(lam, mu, n)`. Nhờ vậy người dùng không thể
lỡ truyền một tổ hợp c, K vô nghĩa.

### 5.8 `MODELS`
Bảng tra `{"M/M/1": MM1, ...}`. Script validation dùng chuỗi Kendall để tạo mô hình, giúp các job gửi sang tiến
trình con chỉ chứa dữ liệu đơn giản (chuỗi, số), dễ pickle.

Ví dụ một lần chạy M/M/1 (λ = 80, μ = 100, T = 500 s, warm-up 50 s, seed 3):
`L ≈ 4.12`, `W ≈ 51.7 ms`, `U ≈ 0.799`, `X ≈ 79.7 req/s`, 79 732 event, khoảng 2.7 s thời gian thực.
Lý thuyết: L = 4, W = 50 ms, U = 0.8. Lần chạy đơn lẻ còn nhiễu; đó là lý do validation lặp lại 30 seed.

---

## 6. `scenarios/coroutine_network_sim.py`: demo mạng

Đây là **demo gốc** của framework, minh họa mẫu "push": LP socket nhận gói mạng và đẩy sang LP ứng dụng qua
`handle_event`, không cần polling. Dev 1 chỉ sửa hai chỗ.

### Các thành phần
- `SocketEventType`: `NETWORK_DATA` (gói mạng tới socket), `APP_DATA` (socket đẩy cho app), `APP_ACK` (app xác nhận).
- `SocketState`, `AppState`: kế thừa `BkSysLogicalProcessState`, chứa buffer và bộ đếm thống kê.
- **`BkSysCoSocketLP`** (`lp_id = 101`):
  - `add_arrival(t, data)`: hẹn trước các gói mạng.
  - `coroutine_run()`: gửi các event `NETWORK_DATA` đã hẹn, rồi lặp `advance_time(0.1)` cho tới khi xử lý hết gói.
  - `handle_event`: `NETWORK_DATA → _handle_network_data` (cất vào buffer và đẩy `APP_DATA` sau `lookahead = 1 ms`;
    buffer đầy thì drop), `APP_ACK → _handle_app_ack` (chỉ ghi log).
- **`BkSysCoAppLP`** (`lp_id = 202`): `handle_event(APP_DATA)` tăng `processed` và gửi `APP_ACK` lại socket.
  `coroutine_run()` lặp `advance_time(0.1)` cho tới khi đủ `max_events = 10` hoặc tới `end_time`.
- `create_bksys_network_sim(...)`: tạo loop với `bucket_width = 0.001`, tạo hai LP, đăng ký, gán event loop.
- `run_network_demo()`: 5 gói HTTP tại t = 0.05, 0.12, 0.20, 0.30, 0.40 s, với `end_time = 1.0`.

### FIX-5: biến đếm `arrivals_processed`
- **Lỗi trong mã gốc:** `arrivals_processed = 0` là **biến cục bộ** trong `coroutine_run`, và không có chỗ nào tăng
  nó. Gói mạng được xử lý trong `handle_event`, là một hàm khác, nên không thể chạm tới biến cục bộ đó. Điều kiện
  `arrivals_processed >= expected_arrivals` vì vậy không bao giờ đúng, và Socket LP chỉ dừng khi chạm `end_time`.
- **Cách sửa:** đổi thành thuộc tính `self.arrivals_processed` (khởi tạo trong `__init__`), và tăng
  `self.arrivals_processed += 1` ở cuối `_handle_network_data`, sau khi gói đã được đẩy đi **hoặc** bị drop. Cả hai
  trường hợp đều tính là "đã xử lý xong", nếu không thì một gói bị drop sẽ làm socket chờ mãi.

### Sửa thêm: App LP bị kẹt do sai số số thực
Sau FIX-2, chạy demo hiện cảnh báo `stranded LPs: {202: 1.0999999999999999}`. **Nguyên nhân:** App LP chờ 10 gói
nhưng chỉ có 5, nên nó lặp `advance_time(0.1)` cho tới `end_time`. Cộng dồn `0.1` mười lần ra `0.9999999999999999`
(nhỏ hơn 1.0), nên vòng lặp chạy thêm một lần và hẹn timer tại 1.0999 > `end_time`. Timer đó không bao giờ nổ.
Lỗi này luôn có trong mã gốc, nhưng bị che vì FIX-2 bị ngược.
**Cách sửa:** bước chờ được kẹp lại `advance_time(min(0.1, end_time − current_time))`, nên timer cuối rơi đúng
vào `end_time`.

### Kết quả khi chạy
```
Socket LP 101: All arrivals processed and buffer empty, terminating
Socket LP 101 finished at time=0.52
App LP 202 finished: {'processed': 5}
All LPs terminated - simulation complete
Coroutine event loop finished. Events: 15, ...
Socket LP (101) stats: {'received': 5, 'delivered': 5, 'dropped': 0, 'pushed': 5}
```
- **15 event** = 5 `NETWORK_DATA` + 5 `APP_DATA` + 5 `APP_ACK`.
- Socket LP kết thúc tại t = 0.52 s nhờ FIX-5 (bản gốc không tự dừng được). App LP kết thúc tại t = 1.0 s.
- Dòng "All LPs terminated" giờ mới **đúng thật** (FIX-2).

---

## 7. `scenarios/validation_single_queues.py`: validation

Mục tiêu: chứng minh 6 mô hình mô phỏng cho kết quả khớp với nghiệm giải tích, trong ngưỡng sai số đề ra.

### 7.1 Các hàm nghiệm giải tích
Mỗi hàm trả về `dict` gồm `L, Lq, W, Wq` (giây), `X` (req/s), `P_block` và `U` (cho mỗi server), cùng khóa với
kết quả mô phỏng để so sánh trực tiếp.

| Hàm | Công thức | Ghi chú / lý do |
|---|---|---|
| `mm1(lam, mu)` | ρ = λ/μ; L = ρ/(1−ρ); W = 1/(μ−λ); Lq = ρL; Wq = ρ/(μ−λ) | `assert rho < 1`: M/M/1 với ρ ≥ 1 không ổn định, không có nghiệm. Vì vậy M/M/1 không được chạy ở ρ = 1.2. |
| `mm1k(lam, mu, K)` | pₙ = p₀ρⁿ, với p₀ = (1−ρ)/(1−ρᴷ⁺¹); λ_eff = λ(1−p_K); U = 1−p₀; W = L/λ_eff (định luật Little) | Riêng trường hợp ρ = 1 thì phân phối đều 1/(K+1), để tránh chia cho 0. W phải tính với **λ_eff**, vì khách bị chặn không vào hệ thống. |
| `mminf(lam, mu)` | L = λ/μ; W = 1/μ; Lq = Wq = 0 | U = `None`, vì "hiệu suất mỗi server" không có nghĩa khi có vô hạn server. |
| `erlang_b(n, a)` | Đệ quy B₀ = 1; Bₖ = a·Bₖ₋₁/(k + a·Bₖ₋₁) | **Tại sao dùng đệ quy?** Công thức trực tiếp (aⁿ/n!)/Σ(aᵏ/k!) bị tràn số hoặc mất chính xác khi n lớn. Dạng đệ quy luôn ổn định về số học. |
| `mmnn(lam, mu, n)` | B = Erlang-B(n, a); λ_eff = λ(1−B); L = λ_eff/μ; W = 1/μ; U = L/n | Không ai phải chờ, nên W đúng bằng thời gian phục vụ. |
| `erlang_c(c, a)` | C = B/(1 − ρ(1−B)), với B = Erlang-B(c, a) | Suy ra từ Erlang-B nên kế thừa tính ổn định số học. |
| `mmc(lam, mu, c)` | Wq = C/(cμ−λ); Lq = λWq; L = Lq + a; W = Wq + 1/μ; U = ρ = a/c | `assert rho < 1` vì lý do ổn định như M/M/1. |
| `mmck(lam, mu, c, K)` | Giải chính xác CTMC sinh-tử trên các trạng thái 0…K: wₙ = wₙ₋₁·λ/(min(n,c)·μ), chuẩn hóa pₙ = wₙ/Σw. Từ đó tính L, Lq = Σ(n−c)pₙ, λ_eff, W = L/λ_eff, Wq = Lq/λ_eff, U = Σmin(n,c)pₙ/c. | **Tại sao giải CTMC thay vì dùng công thức đóng?** Công thức đóng của M/M/c/K dài và dễ gõ sai. Giải chuỗi sinh-tử trực tiếp chỉ khoảng 10 dòng, đúng với mọi c, K. Nó cũng tự kiểm tra chéo được: với c = 1 thì ra M/M/1/K, với K = c thì ra Erlang-B. |

### 7.2 Thiết kế thí nghiệm (hằng số)

| Hằng | Giá trị | Lý do chọn |
|---|---|---|
| `MU` | 100 req/s (1/μ = 10 ms) | Thời gian phục vụ 10 ms là con số điển hình của một dịch vụ web, cho số ms dễ đọc. |
| `C_MULTI` | 4 | Đủ nhiều server để Erlang-C khác hẳn M/M/1, nhưng vẫn chạy nhanh. |
| `K_1` | 10 | Với K = 10, P_block thay đổi rõ khi ρ từ 0.3 lên 1.2, nên kiểm tra được cơ chế chặn. |
| `K_C` | 16 (= 4 server + 12 chỗ chờ) | Như trên, cho M/M/c/K. |
| `N_LOSS` | 10 | n = 10 cho Erlang-B có P_block trải từ khoảng 0.001 đến 0.3. |
| `C_REF_INF` | 10 | M/M/∞ không có khái niệm ρ, nên quy ước λ = ρ·10·μ để vẫn chạy được trên cùng các mức ρ. |
| `RHOS` | 0.3, 0.5, 0.7, 0.8, 0.9 | Theo đề. |
| `RHOS_FINITE` | thêm 1.2 | Hàng hữu hạn vẫn ổn định khi quá tải. Thêm mức này để kiểm tra chế độ chặn nặng. |

#### `CONFIGS`
Mỗi dòng gồm: tên Kendall, hàm `ρ → λ`, hàm `λ → kwargs` của mô hình, hàm `λ → nghiệm giải tích`, và cờ "hữu hạn?".
**Tại sao viết thành bảng dữ liệu?** Thêm hay bớt một mô hình chỉ cần sửa một dòng. Phần chạy, đánh giá và in bảng
không phải đổi.

#### `METRICS = ["W", "L", "U", "P_block"]`
Bốn chỉ số được assert. Lq và Wq vẫn được tính nhưng không assert riêng: chúng suy ra từ W, L và 1/μ, nên nếu W và
L đã đúng thì Lq và Wq cũng đúng.

#### `P_BLOCK_FLOOR = 0.01`
Nếu P_block lý thuyết nhỏ hơn 0.01, dòng đó chỉ được báo cáo mà không assert.
**Lý do:** với P = 0.0005, sai số tuyệt đối 0.00004 (rất nhỏ) đã thành 8% sai số tương đối. Mô phỏng với khoảng
50 000 khách chỉ có khoảng 25 lần bị chặn, nên phương sai tương đối rất lớn. Dùng sai số tương đối cho xác suất
quá nhỏ là thước đo không phù hợp. Các dòng này vẫn hiện trong bảng, kèm cột "Theory in CI".

### 7.3 Thống kê

#### `T_TABLE` và `t_quantile_975(df)`
Hàm trả về phân vị 97.5% của phân phối Student-t với `df` bậc tự do. Nếu có `scipy` thì dùng `t.ppf` cho chính xác.
Nếu không có, hàm tra bảng nhỏ đã nhập sẵn. **Lý do:** dự án không phụ thuộc thư viện ngoài, vì máy của các thành
viên có thể không cài scipy. Với R = 30 thì df = 29 và t = 2.045, đã có sẵn trong bảng.

#### `ci95(xs)`
Trả về `(mean, half_width)`, với half-width h = t₀.₉₇₅,ₙ₋₁ · s/√n.
**Tại sao dùng Student-t mà không dùng z = 1.96?** Mỗi seed cho một giá trị trung bình độc lập. Với n = 30 mẫu và
độ lệch chuẩn ước lượng từ mẫu, phân phối đúng của trung bình chuẩn hóa là t. Dùng z sẽ cho CI hẹp hơn thực tế.
Khi chỉ có 1 mẫu, hàm trả `h = 0` để tránh lỗi của `stdev`.

### 7.4 Chạy các lần mô phỏng

#### `_one_run(job)`
Chạy một mô hình với một seed. Hàm được đặt ở mức module (không phải lambda hay hàm lồng) để `ProcessPoolExecutor`
có thể pickle được. Hàm bỏ khóa `events` khỏi kết quả để dữ liệu trả về nhỏ gọn. Khóa trả về là `(kendall, lam, seed)`.

#### `build_jobs(R, n_arrivals)`
Với mỗi mô hình, mỗi mức ρ:
- `T_sim = n_arrivals / λ`. **Lý do:** mọi cấu hình đều có khoảng 50 000 khách, nên độ chính xác thống kê đồng đều.
  Nếu dùng `T_sim` cố định, cấu hình λ nhỏ (30 req/s) sẽ có ít khách hơn cấu hình λ lớn (1 200 req/s) tới 40 lần.
- `T_warm = 10% · T_sim`, đủ để hệ thống quên trạng thái rỗng ban đầu kể cả khi ρ = 0.9.
- Sinh R job với seed 1…R.

Tổng cộng (5 + 6 + 5 + 6 + 5 + 6) = 33 trường hợp × 30 seed = **990 lần chạy**.

#### `threshold(rho)`
Trả về 5% khi ρ ≤ 0.8 và 8% khi ρ ≥ 0.9, đúng theo đề. Ngưỡng nới rộng khi tải cao vì phương sai của W và L tăng
rất nhanh khi ρ tiến về 1 (tương quan giữa các khách liên tiếp mạnh hơn).

#### `fmt(metric, v)`
Định dạng số để in. **Đây là chỗ duy nhất đổi giây sang ms** (chỉ áp dụng cho W), đúng quy ước "chỉ đổi khi xuất
bảng". `None` được in thành `-`.

### 7.5 Đánh giá và xuất kết quả

#### `evaluate(cases, results, R)`
Với mỗi trường hợp và mỗi chỉ số:
- Bỏ qua nếu nghiệm lý thuyết là `None` (U của M/M/∞) hoặc P_block = 0 (hàng vô hạn, không có chặn).
- Tính `(mean, h)` qua 30 seed, sai số tương đối `|mean − ana| / ana`, và `in_ci` (lý thuyết có nằm trong CI không).
- `asserted = False` với P_block < 0.01 (xem 7.2).
- `ok = err < threshold(rho)`.
- Gom các dòng sai vào `failures`.

**Tại sao vừa có % sai số vừa có "Theory in CI"?** Đề yêu cầu tiêu chí % sai số. Cột "in CI" cung cấp thêm một góc
nhìn thống kê: với CI 95% đúng, khoảng 95% số dòng phải chứa giá trị lý thuyết. Kết quả thực tế là 105/112 (≈ 94%),
khớp kỳ vọng. Nếu tỷ lệ này thấp hẳn, đó là dấu hiệu mô phỏng bị lệch có hệ thống, kể cả khi % sai số vẫn nhỏ.

#### `render_markdown(rows, R, n_arrivals)` và `print_table(rows)`
Hai hàm xuất cùng một nội dung, một dạng bảng Markdown (ghi ra `docs/validation_results.md` để đưa vào báo cáo) và
một dạng bảng chữ thẳng cột cho terminal. Tài liệu được sinh tự động, nên luôn khớp với lần chạy gần nhất và không
ai phải chép số bằng tay.

#### `verification_checks()`
Kiểm tra *verification* (mô hình được cài đúng), tách biệt với *validation* (mô hình khớp lý thuyết):
1. **Tính lặp lại:** chạy M/M/c/K hai lần với cùng seed 7, rồi `assert a == b` (so sánh toàn bộ dict, khớp từng bit).
   Điều này xác nhận không có nguồn ngẫu nhiên hay thời gian thực nào lọt vào mô hình.
2. **Định luật Little:** trên M/M/1, `|L − X·W| / L < 2%`. Định luật Little đúng với mọi hệ thống ổn định, bất kể
   phân phối. Vì vậy nếu nó sai, phần thống kê (`QueueStats`) đang bị cài sai, chứ không phải do nhiễu.

#### `main(argv=None)`
1. Đọc tham số dòng lệnh: `--quick` (R = 5, 10 000 khách, không assert), `-R`, `--arrivals`, `--workers` (mặc định
   bằng số CPU), `--out`.
2. `stdout.reconfigure(encoding="utf-8")`. **Lý do:** console Windows mặc định dùng cp1252, nên in ký tự `±` sẽ gây
   `UnicodeEncodeError`.
3. Chạy `verification_checks()` trước. Nếu sai từ gốc thì dừng sớm, không mất khoảng 10 phút chạy cả bộ (580 s trên 16 core).
4. Chạy song song bằng `ProcessPoolExecutor` với `chunksize=4`.
   - **Tại sao dùng nhiều tiến trình thay vì nhiều luồng?** Mô phỏng thuần Python bị giới hạn bởi GIL, nên luồng
     không giúp chạy nhanh hơn.
   - **Chạy song song có làm hỏng tính lặp lại không?** Không. Mỗi job tự tạo rng từ seed của mình và không chia sẻ
     trạng thái. Thứ tự hoàn thành không ảnh hưởng kết quả.
   - `chunksize=4` giúp giảm chi phí truyền dữ liệu giữa các tiến trình.
   - `time.perf_counter()` **chỉ dùng để đo thời gian chạy của script**, không tham gia vào mô hình, nên không vi
     phạm quy ước Virtual Time.
5. `evaluate`, in bảng, ghi file Markdown (trừ chế độ `--quick`).
6. In `[result] x/y asserted comparisons passed`. Ở chế độ đầy đủ, `assert not failures` kèm danh sách dòng sai,
   rồi in "ALL 6 SINGLE-QUEUE MODELS VALIDATED".

#### Khối `if __name__ == "__main__"`
`sys.exit(0 …)` để script trả mã thoát 0 khi qua. Nếu assert sai, Python tự trả mã khác 0. Nhờ vậy có thể gắn script
vào CI hoặc vào script tổng của nhóm.

### 7.6 Kết quả (tóm tắt)
- **104/104** phép so sánh được assert đều đạt. Sai số lớn nhất là 2.31% (P_block của M/M/1/K tại ρ = 0.8), thấp hơn
  nhiều so với ngưỡng 5% / 8%.
- Giá trị lý thuyết nằm trong CI 95% ở 105/112 dòng, khoảng 94%.
- Chạy lại cùng seed cho kết quả giống hệt; định luật Little thỏa mãn.
- Chi tiết xem [architecture_and_validation.md §4](architecture_and_validation.md#4-validation-6-single-queue-models)
  và [validation_results.md](validation_results.md).
---

## 8. Các file còn lại của framework

Dev 1 không sửa các file này, nhưng cần biết chúng có vai trò gì.

| File | Nội dung | Dev 1 có dùng không |
|---|---|---|
| `bkitevsim/core/__init__.py` | Xuất API: `Event`, `BkSysLogicalProcess(State)`, `Simulation`, `BkSysCoEventLoop`, `BkSysCoLogicalProcess`, `BkSysFuture`, `TIMEOUT_ERROR`, `PriorityQueue`… | Có. `single_queues.py` import từ đây |
| `bkitevsim/core/logical_process.py` | `BkSysLogicalProcess` (lớp trừu tượng: `lp_id`, `local_virt_time`, `state`, `handle_event`, `send_event`) và `BkSysLogicalProcessState` | Gián tiếp, qua lớp con `BkSysCoLogicalProcess` |
| `bkitevsim/core/simulation.py` | `Simulation`, `SimulationOptions`: tạo LP từ factory và chọn dispatcher | Không |
| `bkitevsim/dispatchers/sequential_eventdispatcher.py` | `SequentialEventDispatcher`, `DistributedEventDispatcher` cho LP đồng bộ (không coroutine) | Không |
| `bkitevsim/queues/__init__.py` | Xuất `LadderQueue`, `PriorityQueue` | Gián tiếp, qua event loop |
| `apps/distributed_sequential_app.py`, `apps/synchronous_sequential_app.py` | Hai ví dụ gốc cho dispatcher đồng bộ | Chỉ để kiểm tra vẫn chạy được sau khi vá |
| `scenarios/__init__.py` | Import lười (lazy import) để `python -m scenarios.xxx` không cảnh báo | Có, để chạy được bằng `-m` |
| `bkitevsim/models/__init__.py` | **Mới**, chỉ có docstring | Đánh dấu là package, không import sẵn mô hình |
| `Makefile`, `README.md`, `LICENSE` | Lệnh chạy ví dụ, hướng dẫn, giấy phép chỉ dùng cho môn CO3007 | Không |
| `.gitignore` | `__pycache__/`, `*.pyc` | Không commit file biên dịch tạm |

**Lưu ý:** thư mục `bkitevsim/` gốc **không có** `__init__.py` ở cấp ngoài cùng (là namespace package). Vì vậy phải
import theo đường dẫn con: `from bkitevsim.core import ...`, không viết được `from bkitevsim import ...`.

---

## 9. Một lần chạy đi qua hệ thống như thế nào

Theo dõi các khách đầu tiên trong M/M/1 với λ = 80, μ = 100, seed 1 (số liệu lấy từ `run(..., trace=True)`):

1. `MM1(80, 100).run(T_sim, T_warm, seed=1)` tạo loop, stats, Server (LP 1) và ArrivalEvent (LP 0), đăng ký cả hai
   và gán event loop, rồi gọi `loop.run()`.
2. `asyncio.Runner` chạy `_run_async()`, tạo task cho `coroutine_run()` của hai LP, rồi `await asyncio.sleep(0)`.
   - Server: `coroutine_run()` gọi `run()` (hàng rỗng, không làm gì) rồi kết thúc → `TERMINATED`.
   - ArrivalEvent: rút `dt = 0.008602`, gọi `await self.advance_time(0.008602)`. Lời gọi này đẩy
     `(0.008602, 0, future)` vào `_time_waiters`, LP chuyển sang `WAITING_TIME`.
3. Không còn event và không còn LP `READY` → `_advance_time_to_next_event()` đặt `current_time = 0.008602` và
   resolve timer. ArrivalEvent chạy tiếp, gửi `Event(0.008602, 0→1, "ARRIVAL", Customer(1))`, rút `dt` kế tiếp và
   lại `advance_time`.
4. `_process_events_at_current_time()` giao `ARRIVAL` cho Server: `local_virt_time = 0.008602`, `handle_event` →
   `arrival` → `_touch()` → (không bị chặn) → `schedule()` → `beginService`. Hàm này rút `st = 0.003212` và gửi
   `Event(0.011815, 1→1, "END_SERVICE", Customer(1))`.
5. Tại t = 0.010702, khách 2 đến (timer của ArrivalEvent nổ trước `END_SERVICE` của khách 1). Server đang bận nên
   khách 2 vào hàng chờ.
6. Tại t = 0.011815, `END_SERVICE` của khách 1 được giao: `endService` → `schedule()` → `beginService(khách 2)`.
   Khách 2 đã chờ 1.1 ms.
7. Cứ thế: mỗi bước, vòng lặp nhảy tới mốc sớm hơn giữa timer kế tiếp của nguồn và `END_SERVICE` kế tiếp.
8. Khi lần đến kế tiếp vượt `T_sim`, `ArrivalEvent.coroutine_run()` `return` → `TERMINATED`. Các `END_SERVICE` còn
   lại vẫn được xử lý cho tới `end_time`; những cái sau `end_time` bị bỏ qua.
9. Không còn gì ≤ `end_time`: FIX-2 kiểm tra, không còn LP sống, nên log "All LPs terminated - simulation complete".
   `run()` chốt thống kê tại `T_sim` và trả về dict kết quả.

Trong toàn bộ quá trình, **không có dòng nào chờ đồng hồ thực**. 50 000 khách chạy xong trong khoảng 2–3 giây thực,
chủ yếu là chi phí của vòng lặp asyncio (mỗi event có vài lần `asyncio.sleep(0)`).

---

## 10. Hạn chế đã biết và lưu ý cho Dev 2 / Dev 3

1. **Lỗi quấn vòng của `LadderQueue` (`% 1024`).** Với `bucket_width` mặc định 1.0, chỉ an toàn khi mọi event có
   t < 1024 s. Ai tự tạo `BkSysCoEventLoop` cần chọn `bucket_width ≥ (thời điểm event lớn nhất) / 1024`, nên có dư.
   Ví dụ dùng như `single_queues.py`: `bucket_width = T_sim / 256`. Lỗi này chỉ ảnh hưởng `_event_queue`; timer
   (`advance_time`, timeout) nằm trong heap riêng nên luôn đúng.
2. **`handle_event` ném lỗi thì bị nuốt.** Event loop chỉ ghi `logger.error` rồi chạy tiếp. Nên bọc `try/except` và
   giữ lỗi lại như `Server.handle_event`, hoặc theo dõi log mức ERROR.
3. **Timer sau `end_time` làm LP bị kẹt.** `advance_time(dt)` với `now + dt > end_time` sẽ không bao giờ nổ, và
   FIX-2 sẽ cảnh báo `stranded LPs`. Kiểm tra trước khi chờ (như `ArrivalEvent`) hoặc kẹp bước chờ (như App LP).
   Cộng dồn số thực (0.1 × 10 ≠ 1.0) là nguyên nhân hay gặp.
4. **`register_lp` không gán event loop.** Luôn gọi thêm `lp.set_event_loop(loop)`, nếu không `advance_time` sẽ ném
   `RuntimeError("Event loop not set")`.
5. **`wait_for` và `handle_event` mặc định có thể nhận trùng event.** `handle_event` mặc định cất event vào
   `_pending_events`, đồng thời event đó cũng được trao cho `wait_for` đang chờ. Nếu sau đó gọi
   `wait_for_multiple`, nó sẽ trả lại bản đã cất. Khi dùng `wait_for`/`wait_for_multiple`, nên ghi đè `handle_event`
   (như hai LP trong demo mạng) hoặc chỉ dùng một trong hai cách.
6. **Tốc độ.** Vòng lặp gốc tốn khoảng 30 µs mỗi event (vài lần `asyncio.sleep(0)` cho mỗi event). Validation đầy
   đủ mất khoảng 10 phút trên 16 core (bản stand-in cũ mất khoảng 80 s). Khi thử nghiệm, dùng `--quick`.
7. **`t_quantile_975` khi không có scipy** sẽ lấy bậc tự do gần nhất **lớn hơn** trong bảng (cho t nhỏ hơn một
   chút). Với R = 30 thì giá trị khớp chính xác (df = 29 có trong bảng). Nếu đổi R sang giá trị lạ, nên cài scipy.
8. **Ở M/M/∞ và M/M/n/n, W gần như giống nhau giữa các mức ρ.** Lý do là cùng seed dùng chung luồng thời gian phục
   vụ (common random numbers). Đây là chủ đích, không phải lỗi.
9. **Cách dùng lại cho Dev 2 / Dev 3:**
   ```python
   from bkitevsim.models.single_queues import MMcK
   r = MMcK(lam=380.0, mu=100.0, c=4, K=16).run(T_sim=200.0, T_warmup=20.0, seed=1)
   print(r["W"] * 1000, "ms", r["P_block"])   # chỉ đổi sang ms khi in
   ```
   Với mô hình mới: kế thừa `BkSysCoLogicalProcess`, gửi `Event(timestamp, sender_id, receiver_id, event_type,
   payload)` bằng `send_event`, xử lý trong `handle_event`, ngủ bằng `self.advance_time` / `self.wait_until`, chờ
   bằng `self.wait_for(type, timeout=…)` / `self.wait_for_multiple(types, timeout=…)` và bắt `TIMEOUT_ERROR`.
   Timeout riêng thì dùng `loop.schedule_virtual_timer(lp_id, delay)`. **Không được** dùng `asyncio.sleep(d > 0)`
   hay `asyncio.wait_for`.

### Lệnh chạy

```bash
cd XX-004679A-sim
py -3.12 -m scenarios.coroutine_network_sim             # demo mạng (vài ms)
py -3.12 -m scenarios.validation_single_queues --quick  # smoke test (R=5, không assert)
py -3.12 -m scenarios.validation_single_queues          # đầy đủ (R=30), ghi docs/validation_results.md (~10 phút)
py -3.12 -m apps.distributed_sequential_app             # ví dụ gốc của framework
py -3.12 -m apps.synchronous_sequential_app             # ví dụ gốc của framework
```
