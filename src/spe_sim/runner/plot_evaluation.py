from __future__ import annotations

import argparse
import csv
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

RUNTIME_LABELS: dict[str, str] = {
    "blocking_baseline": "Blocking Baseline (Thread Pool)",
    "nodejs_event_loop": "Node.js Event Loop",
    "bun_event_loop": "Bun Event Loop",
    "spring_loom_vt": "Spring + Loom (Virtual Threads)",
}

RUNTIME_COLORS: dict[str, str] = {
    "blocking_baseline": "#d62728",
    "nodejs_event_loop": "#2ca02c",
    "bun_event_loop": "#ff7f0e",
    "spring_loom_vt": "#1f77b4",
}

RUNTIME_MARKERS: dict[str, str] = {
    "blocking_baseline": "o",
    "nodejs_event_loop": "s",
    "bun_event_loop": "^",
    "spring_loom_vt": "D",
}

RUNTIME_ORDER = [
    "blocking_baseline",
    "nodejs_event_loop",
    "bun_event_loop",
    "spring_loom_vt",
]


def _to_float(val: Any) -> float:
    if val in (None, "", "nan", "NaN"):
        return math.nan
    return float(val)


def load_summary(csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            parsed: dict[str, Any] = dict(r)
            parsed["lam"] = float(r["lam"])
            parsed["n_db"] = int(float(r["n_db"]))
            parsed["p_pin"] = float(r["p_pin"])
            parsed["p_uv"] = float(r["p_uv"])
            for col in (
                "throughput_mean_req_s",
                "throughput_ci95_req_s",
                "e_r_mean_ms",
                "e_r_ci95_ms",
                "p95_mean_ms",
                "p95_ci95_ms",
                "loss_rate_mean",
                "loss_rate_ci95",
                "dropped_rate_mean",
                "db_drop_rate_mean",
                "timeout_rate_mean",
            ):
                parsed[col] = _to_float(r.get(col))
            rows.append(parsed)
    return rows


def setup_style() -> None:
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "legend.fontsize": 9,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "axes.grid": True,
            "grid.alpha": 0.3,
            "grid.linestyle": "--",
        }
    )


def plot_main_sweep_grid(main_rows: list[dict[str, Any]], out_dir: Path) -> None:
    """Vẽ 4 biểu đồ chính theo tải lambda (50..450), mỗi biểu đồ 2 panel (gather vs as_completed)."""
    metrics_spec = [
        (
            "throughput_mean_req_s",
            "throughput_ci95_req_s",
            "Throughput X (req/s)",
            "System Throughput vs. Arrival Rate (λ)",
            "fig1_main_throughput.png",
            1.0,
        ),
        (
            "e_r_mean_ms",
            "e_r_ci95_ms",
            "Mean Response Time E[r] (ms)",
            "Mean Response Time E[r] vs. Arrival Rate (λ)",
            "fig2_main_mean_rt_ms.png",
            1.0,
        ),
        (
            "p95_mean_ms",
            "p95_ci95_ms",
            "95th Percentile Response Time P95 (ms)",
            "Tail Latency (P95) vs. Arrival Rate (λ)",
            "fig3_main_p95_ms.png",
            1.0,
        ),
        (
            "loss_rate_mean",
            "loss_rate_ci95",
            "Loss Rate P_loss (%)",
            "Request Loss Rate vs. Arrival Rate (λ)",
            "fig4_main_loss_rate.png",
            100.0,
        ),
    ]

    join_modes = ["gather", "as_completed"]
    join_titles = {
        "gather": "Join Mode: gather (3/3 branches)",
        "as_completed": "Join Mode: as_completed (2/3 quorum)",
    }

    for mean_col, ci_col, ylabel, suptitle, filename, scale in metrics_spec:
        fig, axes = plt.subplots(1, 2, figsize=(13, 5.0), sharey=True)
        for ax, jm in zip(axes, join_modes):
            subset = [r for r in main_rows if r["join_mode"] == jm]
            for rt in RUNTIME_ORDER:
                pts = sorted(
                    [r for r in subset if r["runtime_model"] == rt and not math.isnan(r[mean_col])],
                    key=lambda x: x["lam"],
                )
                if not pts:
                    continue
                xs = [p["lam"] for p in pts]
                ys = [p[mean_col] * scale for p in pts]
                cis = [0.0 if math.isnan(p[ci_col]) else p[ci_col] * scale for p in pts]

                color = RUNTIME_COLORS[rt]
                ax.plot(
                    xs,
                    ys,
                    label=RUNTIME_LABELS[rt],
                    color=color,
                    marker=RUNTIME_MARKERS[rt],
                    linewidth=2.0,
                    markersize=5,
                )
                lower = [max(0.0, y - c) for y, c in zip(ys, cis)]
                upper = [y + c for y, c in zip(ys, cis)]
                ax.fill_between(xs, lower, upper, color=color, alpha=0.14)

            ax.set_title(join_titles[jm])
            ax.set_xlabel("Arrival Rate λ (req/s)")
            ax.set_xticks(sorted({r["lam"] for r in subset}))

        axes[0].set_ylabel(ylabel)
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=2, frameon=True)
        fig.suptitle(suptitle, fontsize=13, fontweight="bold", y=1.02)
        fig.tight_layout(rect=[0, 0, 1, 0.90])
        fig.savefig(out_dir / filename, dpi=300, bbox_inches="tight")
        plt.close(fig)


def plot_join_mode_comparison(main_rows: list[dict[str, Any]], out_dir: Path) -> None:
    """So sánh trực tiếp gather vs as_completed trên từng runtime (E[r] và P95)."""
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
    axes_flat = axes.flatten()

    for ax, rt in zip(axes_flat, RUNTIME_ORDER):
        for jm, ls, marker in [("gather", "-", "o"), ("as_completed", "--", "s")]:
            pts = sorted(
                [
                    r
                    for r in main_rows
                    if r["runtime_model"] == rt
                    and r["join_mode"] == jm
                    and not math.isnan(r["p95_mean_ms"])
                ],
                key=lambda x: x["lam"],
            )
            if not pts:
                continue
            xs = [p["lam"] for p in pts]
            ys = [p["p95_mean_ms"] for p in pts]
            cis = [0.0 if math.isnan(p["p95_ci95_ms"]) else p["p95_ci95_ms"] for p in pts]
            ax.errorbar(
                xs,
                ys,
                yerr=cis,
                linestyle=ls,
                marker=marker,
                capsize=3,
                linewidth=1.8,
                label=f"{jm}",
            )
        ax.set_title(RUNTIME_LABELS[rt])
        ax.set_ylabel("P95 Response Time (ms)")
        ax.legend(loc="upper left")

    for ax in axes[1]:
        ax.set_xlabel("Arrival Rate λ (req/s)")

    fig.suptitle("Join Mode Impact on Tail Latency P95 (gather vs. as_completed)", fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_dir / "fig5_join_mode_p95_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def plot_loss_breakdown(main_rows: list[dict[str, Any]], out_dir: Path) -> None:
    """Phân rã nguyên nhân mất gói (admission dropped vs db_drop vs timeout) ở chế độ gather."""
    subset = [r for r in main_rows if r["join_mode"] == "gather"]
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.0), sharex=True, sharey=True)

    for ax, rt in zip(axes.flatten(), RUNTIME_ORDER):
        pts = sorted([r for r in subset if r["runtime_model"] == rt], key=lambda x: x["lam"])
        if not pts:
            continue
        xs = [str(int(p["lam"])) for p in pts]
        dropped = [p["dropped_rate_mean"] * 100.0 for p in pts]
        db_drop = [p["db_drop_rate_mean"] * 100.0 for p in pts]
        timeout = [p["timeout_rate_mean"] * 100.0 for p in pts]

        ax.bar(xs, dropped, label="Admission Dropped", color="#7f7f7f")
        ax.bar(xs, db_drop, bottom=dropped, label="DB Pool Drop", color="#ff7f0e")
        bottom2 = [a + b for a, b in zip(dropped, db_drop)]
        ax.bar(xs, timeout, bottom=bottom2, label="Timeout (>300ms)", color="#d62728")

        ax.set_title(RUNTIME_LABELS[rt])
        ax.set_ylabel("Loss Share (%)")

    for ax in axes[1]:
        ax.set_xlabel("Arrival Rate λ (req/s)")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.96), ncol=3, frameon=True)
    fig.suptitle("Loss Breakdown by Failure Mechanism (gather mode)", fontsize=13, fontweight="bold", y=1.01)
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(out_dir / "fig6_loss_breakdown_gather.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def _detect_sensitivity_factor(scenario_id: str) -> str:
    s = scenario_id.lower()
    if "ndb" in s or "n_db" in s:
        return "n_db"
    if "ppin" in s or "p_pin" in s:
        return "p_pin"
    if "puv" in s or "p_uv" in s:
        return "p_uv"
    return "other"


def plot_sensitivity(sens_rows: list[dict[str, Any]], out_dir: Path) -> None:
    """Vẽ phân tích độ nhạy cho n_db, p_pin, p_uv theo từng mức tải lambda."""
    if not sens_rows:
        return

    by_factor: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in sens_rows:
        by_factor[_detect_sensitivity_factor(r["scenario_id"])].append(r)

    lam_styles = {
        150.0: ("-", "o"),
        300.0: ("--", "s"),
        400.0: ("-.", "^"),
        450.0: (":", "D"),
    }

    # 1. Độ nhạy n_db: 2 panel theo join_mode, mỗi đường là một cặp (runtime_model, lam)
    if "n_db" in by_factor:
        fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.2), sharey=True)
        for ax, jm in zip(axes, ["gather", "as_completed"]):
            sub = [r for r in by_factor["n_db"] if r["join_mode"] == jm]
            lams = sorted({r["lam"] for r in sub})
            for rt in RUNTIME_ORDER:
                for lam in lams:
                    pts = sorted(
                        [r for r in sub if r["runtime_model"] == rt and r["lam"] == lam],
                        key=lambda x: x["n_db"],
                    )
                    if not pts:
                        continue
                    ls, mk = lam_styles.get(lam, ("-", "o"))
                    ax.errorbar(
                        [p["n_db"] for p in pts],
                        [p["p95_mean_ms"] for p in pts],
                        yerr=[0.0 if math.isnan(p["p95_ci95_ms"]) else p["p95_ci95_ms"] for p in pts],
                        label=f"{RUNTIME_LABELS[rt]} (λ={int(lam)})",
                        color=RUNTIME_COLORS[rt],
                        linestyle=ls,
                        marker=mk,
                        capsize=3,
                        linewidth=1.8,
                    )
            ax.set_title(f"Join Mode: {jm}")
            ax.set_xlabel("DB Connection Pool Size (N_db)")
            ax.set_xticks(sorted({r["n_db"] for r in sub}))
        axes[0].set_ylabel("P95 Response Time (ms)")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.99), ncol=3, frameon=True)
        fig.suptitle("Sensitivity Analysis: DB Pool Size N_db Across λ", fontsize=13, fontweight="bold", y=1.03)
        fig.tight_layout(rect=[0, 0, 1, 0.78])
        fig.savefig(out_dir / "fig7_sens_ndb_p95.png", dpi=300, bbox_inches="tight")
        plt.close(fig)

    # 2. Độ nhạy p_pin (Spring Loom VT): tách theo join_mode và từng mức lam
    if "p_pin" in by_factor:
        fig, axes = plt.subplots(1, 2, figsize=(12.5, 5.0))
        sub = [r for r in by_factor["p_pin"] if r["runtime_model"] == "spring_loom_vt"]
        lams = sorted({r["lam"] for r in sub})
        for jm, base_color in [("gather", "#1f77b4"), ("as_completed", "#ff7f0e")]:
            for lam in lams:
                pts = sorted(
                    [r for r in sub if r["join_mode"] == jm and r["lam"] == lam],
                    key=lambda x: x["p_pin"],
                )
                if not pts:
                    continue
                ls, mk = lam_styles.get(lam, ("-", "o"))
                xs = [p["p_pin"] * 100.0 for p in pts]
                lbl = f"{jm} (λ={int(lam)})"
                axes[0].errorbar(
                    xs,
                    [p["e_r_mean_ms"] for p in pts],
                    yerr=[0.0 if math.isnan(p["e_r_ci95_ms"]) else p["e_r_ci95_ms"] for p in pts],
                    color=base_color,
                    linestyle=ls,
                    marker=mk,
                    capsize=3,
                    linewidth=1.8,
                    label=lbl,
                )
                axes[1].errorbar(
                    xs,
                    [p["p95_mean_ms"] for p in pts],
                    yerr=[0.0 if math.isnan(p["p95_ci95_ms"]) else p["p95_ci95_ms"] for p in pts],
                    color=base_color,
                    linestyle=ls,
                    marker=mk,
                    capsize=3,
                    linewidth=1.8,
                    label=lbl,
                )
        axes[0].set_title("Mean Response Time E[r]")
        axes[0].set_ylabel("E[r] (ms)")
        axes[1].set_title("Tail Latency P95")
        axes[1].set_ylabel("P95 (ms)")
        for ax in axes:
            ax.set_xlabel("Carrier Thread Pinning Probability p_pin (%)")
            ax.set_xticks(sorted({r["p_pin"] * 100.0 for r in sub}))
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.98), ncol=4, frameon=True)
        fig.suptitle("Sensitivity Analysis: Spring Loom Carrier Pinning Across λ", fontsize=13, fontweight="bold", y=1.03)
        fig.tight_layout(rect=[0, 0, 1, 0.85])
        fig.savefig(out_dir / "fig8_sens_ppin_loom.png", dpi=300, bbox_inches="tight")
        plt.close(fig)

    # 3. Độ nhạy p_uv (Node.js & Bun): tách theo join_mode và từng mức lam
    if "p_uv" in by_factor:
        fig, axes = plt.subplots(1, 2, figsize=(13.5, 5.2), sharey=True)
        for ax, jm in zip(axes, ["gather", "as_completed"]):
            sub = [r for r in by_factor["p_uv"] if r["join_mode"] == jm]
            lams = sorted({r["lam"] for r in sub})
            for rt in ("nodejs_event_loop", "bun_event_loop"):
                for lam in lams:
                    pts = sorted(
                        [r for r in sub if r["runtime_model"] == rt and r["lam"] == lam],
                        key=lambda x: x["p_uv"],
                    )
                    if not pts:
                        continue
                    ls, mk = lam_styles.get(lam, ("-", "o"))
                    xs = [p["p_uv"] * 100.0 for p in pts]
                    ys = [p["p95_mean_ms"] for p in pts]
                    cis = [0.0 if math.isnan(p["p95_ci95_ms"]) else p["p95_ci95_ms"] for p in pts]
                    ax.errorbar(
                        xs,
                        ys,
                        yerr=cis,
                        label=f"{RUNTIME_LABELS[rt]} (λ={int(lam)})",
                        color=RUNTIME_COLORS[rt],
                        linestyle=ls,
                        marker=mk,
                        capsize=3,
                        linewidth=1.8,
                    )
            ax.set_title(f"Join Mode: {jm}")
            ax.set_xlabel("Libuv / Blocking Offload Probability p_uv (%)")
            ax.set_xticks(sorted({r["p_uv"] * 100.0 for r in by_factor["p_uv"]}))
        axes[0].set_ylabel("P95 Response Time (ms)")
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.98), ncol=4, frameon=True)
        fig.suptitle("Sensitivity Analysis: Event-Loop Blocking / Offload p_uv Across λ", fontsize=13, fontweight="bold", y=1.03)
        fig.tight_layout(rect=[0, 0, 1, 0.84])
        fig.savefig(out_dir / "fig9_sens_puv_eventloop.png", dpi=300, bbox_inches="tight")
        plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Vẽ toàn bộ biểu đồ đánh giá hiệu năng từ summary_metrics_ci95.csv.")
    parser.add_argument(
        "--summary-csv",
        type=Path,
        default=Path("results/final/summary_metrics_ci95.csv"),
        help="Đường dẫn tới file summary_metrics_ci95.csv",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("results/final/figures"),
        help="Thư mục lưu ảnh PNG đầu ra",
    )
    args = parser.parse_args()

    setup_style()
    rows = load_summary(args.summary_csv)
    main_rows = [r for r in rows if r.get("group") == "main"]
    sens_rows = [r for r in rows if r.get("group") == "sensitivity"]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    plot_main_sweep_grid(main_rows, args.out_dir)
    plot_join_mode_comparison(main_rows, args.out_dir)
    plot_loss_breakdown(main_rows, args.out_dir)
    plot_sensitivity(sens_rows, args.out_dir)

    created = sorted(p.name for p in args.out_dir.glob("*.png"))
    print(f"Đã xuất {len(created)} biểu đồ tại {args.out_dir}:")
    for name in created:
        print(f"  - {name}")


if __name__ == "__main__":
    main()