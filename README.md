# SPE-Assignment — Mô phỏng HTTP API Aggregator bằng Python asyncio

CO3007 · HK261 · Nhóm XX-004679A. Đặc tả: *Báo cáo Đặc tả Thiết kế – Giai đoạn 1*.

Mô phỏng DES trên thời gian ảo (framework `bkitevsim`, bản đã vá trong `vendor/`) cho mạng
hàng đợi mở Fork-Join: Admission Control (K = 64) → Runtime Dispatcher (Java Blocking,
Node.js, Bun, Java Spring Loom) → Network Link M/M/∞ → 3 backend (Auth M/M/8, Catalog
M/M/16, DB pool M/M/n/n). Kèm kiểm chứng 6 hàng đợi đơn so với nghiệm giải tích.

## Yêu cầu

- **Python ≥ 3.11** (khuyến nghị `py -3.12`). Framework dùng `asyncio.Runner` (spec §1.1.2),
  code dùng `dataclass(slots=True)`; Python 3.8 mặc định trên một số máy **không chạy được**.
- Không cần thư viện ngoài. Có `scipy` thì phân vị Student-t lấy từ `scipy`, không thì tra bảng.

Trên Windows, đặt biến môi trường trước khi chạy:

```bash
set PYTHONPATH=src
set PYTHONIOENCODING=utf-8
```

(Git Bash: `export PYTHONPATH=src PYTHONIOENCODING=utf-8`.)

## Lệnh chạy

```bash
# Unit test và kiểm tra bất biến (Verification, spec §1.5.2)
py -3.12 -m unittest discover -s tests
py -3.12 tests/verify_trace.py        # trace tất định 63.50 / 29.50 / 24.50 ms
py -3.12 tests/verify_invariants.py   # ΔN = 0, Little's Law, 3.0 × N_admitted, hai engine trùng nhau
py -3.12 tests/verify_baseline.py     # λ0 = 150: ngưỡng §2.1 và số giải tích §3.3

# Kiểm chứng 6 hàng đợi đơn và trạm CPU (Validation, spec §1.5.2) → docs/validation_results.md
py -3.12 -m spe_sim.validation --set all --impl both     # đầy đủ: R = 30, ~50 000 lượt đến/lần
py -3.12 -m spe_sim.validation --set generic             # chỉ bộ tham số chung
py -3.12 -m spe_sim.validation --set project             # bộ tham số theo Bảng 1
py -3.12 -m spe_sim.validation --set cpu                 # trạm CPU cô lập của 4 runtime
py -3.12 -m spe_sim.validation --quick                   # chạy thử nhanh, không ép ngưỡng

# Thí nghiệm mạng (lưới chính + độ nhạy) → CSV D3 mỗi kịch bản + summary.csv (mean ± CI 95 %)
py -3.12 -m spe_sim.runner --out results/
py -3.12 -m spe_sim.runner --out results/ --only spring_loom_vt -R 5 --t-sim 60   # chạy thử

# Chạy ma trận thực nghiệm theo nhóm (hỗ trợ resume từ per_seed_metrics.csv) → summary_metrics_ci95.csv
py -3.12 -m spe_sim.runner.run_study_matrix --out-dir results/final --group main --workers 8 --save-sample-seed -1
py -3.12 -m spe_sim.runner.run_study_matrix --out-dir results/final --group sensitivity --workers 8 --save-sample-seed -1

# Vẽ lại toàn bộ 9 biểu đồ đánh giá hiệu năng từ bảng tổng hợp CI 95%
py -3.12 -m spe_sim.runner.plot_evaluation --summary-csv results/final/summary_metrics_ci95.csv --out-dir results/final/figures
```

## Cấu trúc

```
src/spe_sim/
  core/        config (hằng số, profile 4 runtime, ScenarioConfig), events, rng (SeedStreams),
               analytics (6 hàng đợi đơn, Erlang-B/C, order statistics), stats (CI 95 %),
               kernel (adapter duy nhất tới bkitevsim), records (CSV D3)
  lp/infra/    source, admission, network_link, service_lp + station (StationCore),
               auth_cluster, catalog_service, db_pool, uv_pool, sinks, metrics
  lp/runtime/  dispatcher (bản đồng bộ), coroutine_dispatcher (asyncio — mặc định),
               models/ (4 runtime model)
  runner/      study (dựng hệ thống, chạy lưới), deterministic (trace tất định),
               run_study_matrix (chạy theo nhóm + resume + CI 95%), plot_evaluation (9 biểu đồ),
               __main__ (CLI)
  validation/  single_queue (6 hàng đợi trên LP của mạng), cpu_station (dispatcher cô lập),
               reference_server (bản tham chiếu độc lập), params, harness, __main__ (CLI)
configs/scenarios.json   lưới chính 72 kịch bản + 192 kịch bản độ nhạy
vendor/bkitevsim/        framework, đã vá FIX-1…FIX-5
docs/                    validation_results.md (sinh tự động), ghi_chu_hop_nhat.md (ghi chú cho báo cáo)
```

## Quy ước

- Mọi thời gian nội bộ tính bằng **giây**, tốc độ bằng **req/s**; chỉ nhân 1000 sang ms khi in.
- LP chỉ trả `Emission` (`handle_event(payload, now) -> list[Emission]`) và không import
  `bkitevsim`; `core/kernel.py` là nơi duy nhất gọi framework.
- Mỗi luồng ngẫu nhiên lấy từ `SeedStreams(seed).stream(tag)` để tái lập 100 %.
- Stochastic Workload: CPU, mạng và backend đều theo phân phối mũ (`cpu_dist = "exp"`);
  Deterministic Workload (trace) dùng CPU cố định.
