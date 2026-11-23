"""spe_sim — mô phỏng DES hệ thống HTTP API Aggregator (CO3007, nhóm XX-004679A).

Phân lớp:
    core/          hằng số, event, rng, records, analytics, adapter kernel
    lp/infra/      LP hạ tầng: source, admission, network link, 3 backend, sinks, metrics
    lp/runtime/    RuntimeDispatcherLP + 4 runtime model
    runner/        ghép LP và chạy kịch bản
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
