"""Validation of the 6 single-queue models against closed-form results.

For each model and each utilisation rho in {0.3,0.5,0.7,0.8,0.9} (+1.2 for the
finite-capacity models) we run R = 30 independent seeds, build a 95%
Student-t confidence interval and compare the mean with theory.
Pass criteria: relative error < 5% for rho <= 0.8 and < 8% for rho >= 0.9.

Run:   py -3.12 -m scenarios.validation_single_queues            (full)
       py -3.12 -m scenarios.validation_single_queues --quick    (R=5, short)
Simulation is in SI units (s, req/s); tables print times in ms.
"""
import argparse
import math
import os
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

from bkitevsim.models.single_queues import MODELS

# ---------------------------------------------------------------- analytics
# Each returns dict with L, Lq, W, Wq [s], X [req/s], P_block, U (per server).

def mm1(lam, mu):
    rho = lam / mu
    assert rho < 1, "M/M/1 unstable"
    L = rho / (1 - rho)
    W = 1 / (mu - lam)
    return dict(L=L, Lq=rho * L, W=W, Wq=rho / (mu - lam), X=lam, P_block=0.0, U=rho)


def mm1k(lam, mu, K):
    rho = lam / mu
    if abs(rho - 1) < 1e-12:
        p = [1 / (K + 1)] * (K + 1)
    else:
        p0 = (1 - rho) / (1 - rho ** (K + 1))
        p = [p0 * rho ** n for n in range(K + 1)]
    L = sum(n * pn for n, pn in enumerate(p))
    PK = p[K]
    lam_eff = lam * (1 - PK)
    U = 1 - p[0]
    W = L / lam_eff
    return dict(L=L, Lq=L - U, W=W, Wq=W - 1 / mu, X=lam_eff, P_block=PK, U=U)


def mminf(lam, mu):
    a = lam / mu
    return dict(L=a, Lq=0.0, W=1 / mu, Wq=0.0, X=lam, P_block=0.0, U=None)


def erlang_b(n, a):
    b = 1.0
    for k in range(1, n + 1):
        b = a * b / (k + a * b)
    return b


def mmnn(lam, mu, n):
    a = lam / mu
    B = erlang_b(n, a)
    lam_eff = lam * (1 - B)
    L = lam_eff / mu
    return dict(L=L, Lq=0.0, W=1 / mu, Wq=0.0, X=lam_eff, P_block=B, U=L / n)


def erlang_c(c, a):
    B = erlang_b(c, a)
    rho = a / c
    return B / (1 - rho * (1 - B))


def mmc(lam, mu, c):
    a = lam / mu
    rho = a / c
    assert rho < 1, "M/M/c unstable"
    C = erlang_c(c, a)
    Wq = C / (c * mu - lam)
    Lq = lam * Wq
    return dict(L=Lq + a, Lq=Lq, W=Wq + 1 / mu, Wq=Wq, X=lam, P_block=0.0, U=rho)


def mmck(lam, mu, c, K):
    """Finite birth-death CTMC solved exactly (states 0..K)."""
    w = [1.0]
    for n in range(1, K + 1):
        w.append(w[-1] * lam / (min(n, c) * mu))
    s = sum(w)
    p = [x / s for x in w]
    L = sum(n * pn for n, pn in enumerate(p))
    Lq = sum((n - c) * pn for n, pn in enumerate(p) if n > c)
    PK = p[K]
    lam_eff = lam * (1 - PK)
    W = L / lam_eff
    busy = sum(min(n, c) * pn for n, pn in enumerate(p))
    return dict(L=L, Lq=Lq, W=W, Wq=Lq / lam_eff, X=lam_eff, P_block=PK, U=busy / c)


# ------------------------------------------------------------- experiment
MU = 1 / 0.010          # 1/mu = 10 ms  ->  mu = 100 req/s per server
C_MULTI = 4             # servers for M/M/c, M/M/c/K
K_1 = 10                # capacity for M/M/1/K
K_C = 16                # capacity for M/M/c/K
N_LOSS = 10             # servers for M/M/n/n
C_REF_INF = 10          # reference "c" defining rho for M/M/inf: lam = rho*10*mu
RHOS = [0.3, 0.5, 0.7, 0.8, 0.9]
RHOS_FINITE = RHOS + [1.2]

CONFIGS = [
    # kendall, model kwargs builder (lam -> kwargs), lam(rho), analytic, finite?
    ("M/M/1",   lambda r: r * MU,             lambda l: {},                        lambda l: mm1(l, MU),                 False),
    ("M/M/1/K", lambda r: r * MU,             lambda l: {"K": K_1},                lambda l: mm1k(l, MU, K_1),           True),
    ("M/M/inf", lambda r: r * C_REF_INF * MU, lambda l: {},                        lambda l: mminf(l, MU),               False),
    ("M/M/n/n", lambda r: r * N_LOSS * MU,    lambda l: {"n": N_LOSS},             lambda l: mmnn(l, MU, N_LOSS),        True),
    ("M/M/c",   lambda r: r * C_MULTI * MU,   lambda l: {"c": C_MULTI},            lambda l: mmc(l, MU, C_MULTI),        False),
    ("M/M/c/K", lambda r: r * C_MULTI * MU,   lambda l: {"c": C_MULTI, "K": K_C},  lambda l: mmck(l, MU, C_MULTI, K_C),  True),
]

METRICS = ["W", "L", "U", "P_block"]
P_BLOCK_FLOOR = 0.01    # relative error of tiny probabilities is not meaningful


T_TABLE = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365,
           8: 2.306, 9: 2.262, 10: 2.228, 14: 2.145, 19: 2.093, 24: 2.064,
           29: 2.045, 39: 2.023, 49: 2.010, 99: 1.984}


def t_quantile_975(df):
    try:
        from scipy.stats import t
        return float(t.ppf(0.975, df))
    except ImportError:
        if df in T_TABLE:
            return T_TABLE[df]
        keys = [k for k in T_TABLE if k >= df]
        return T_TABLE[min(keys)] if keys else 1.960


def ci95(xs):
    m = statistics.fmean(xs)
    if len(xs) < 2:
        return m, 0.0
    h = t_quantile_975(len(xs) - 1) * statistics.stdev(xs) / math.sqrt(len(xs))
    return m, h


def _one_run(job):
    kendall, lam, kwargs, T_sim, T_warm, seed = job
    model = MODELS[kendall](lam, MU, **kwargs)
    r = model.run(T_sim, T_warm, seed)
    r.pop("events", None)
    return (kendall, lam, seed), r


def build_jobs(R, n_arrivals):
    jobs, cases = [], []
    for kendall, lam_of, kw_of, ana_of, finite in CONFIGS:
        for rho in (RHOS_FINITE if finite else RHOS):
            lam = lam_of(rho)
            T_sim = n_arrivals / lam                # seconds of virtual time
            T_warm = 0.1 * T_sim
            cases.append((kendall, rho, lam, ana_of(lam)))
            for seed in range(1, R + 1):
                jobs.append((kendall, lam, kw_of(lam), T_sim, T_warm, seed))
    return jobs, cases


def threshold(rho):
    return 0.05 if rho <= 0.8 else 0.08


def fmt(metric, v):
    if v is None:
        return "-"
    return f"{v * 1000:.3f}" if metric == "W" else f"{v:.4f}"


def evaluate(cases, results, R):
    rows, failures = [], []
    for kendall, rho, lam, ana in cases:
        runs = [results[(kendall, lam, s)] for s in range(1, R + 1)]
        for metric in METRICS:
            a = ana[metric]
            if a is None or (metric == "P_block" and a == 0.0):
                continue
            m, h = ci95([r[metric] for r in runs])
            err = abs(m - a) / a if a else float("nan")
            asserted = not (metric == "P_block" and a < P_BLOCK_FLOOR)
            in_ci = (m - h) <= a <= (m + h)
            ok = (err < threshold(rho)) if asserted else True
            rows.append(dict(model=kendall, rho=rho, lam=lam, metric=metric, mean=m, half=h,
                             ana=a, err=err, in_ci=in_ci, asserted=asserted, ok=ok))
            if not ok:
                failures.append(rows[-1])
    return rows, failures


def render_markdown(rows, R, n_arrivals):
    out = [f"R = {R} seeds, ~{n_arrivals} arrivals/run, warm-up = 10% of T_sim, "
           f"1/mu = 10 ms. W in ms.", "",
           "| Model | rho | lambda (req/s) | Metric | Simulation (mean ± 95% CI) | Analytical | % Error | Theory in CI | Pass |",
           "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        sim = f"{fmt(r['metric'], r['mean'])} ± {fmt(r['metric'], r['half'])}"
        pas = ("PASS" if r["ok"] else "FAIL") if r["asserted"] else "n/a (< 0.01)"
        out.append(f"| {r['model']} | {r['rho']} | {r['lam']:.0f} | {r['metric']} | {sim} | "
                   f"{fmt(r['metric'], r['ana'])} | {100 * r['err']:.2f}% | "
                   f"{'yes' if r['in_ci'] else 'no'} | {pas} |")
    return "\n".join(out)


def print_table(rows):
    hdr = f"{'Model':8} {'rho':>4} {'Metric':7} {'Simulation (mean ± 95% CI)':>28} {'Analytical':>11} {'%Err':>7} {'inCI':>5} Pass"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        sim = f"{fmt(r['metric'], r['mean'])} ± {fmt(r['metric'], r['half'])}"
        pas = ("PASS" if r["ok"] else "FAIL") if r["asserted"] else "n/a"
        print(f"{r['model']:8} {r['rho']:>4} {r['metric']:7} {sim:>28} "
              f"{fmt(r['metric'], r['ana']):>11} {100 * r['err']:6.2f}% "
              f"{'yes' if r['in_ci'] else 'no':>5} {pas}")


def verification_checks():
    """Reproducibility (same seed -> identical) and Little's law."""
    a = MODELS["M/M/c/K"](300, MU, c=4, K=16).run(50, 5, seed=7)
    b = MODELS["M/M/c/K"](300, MU, c=4, K=16).run(50, 5, seed=7)
    assert a == b, "same seed must reproduce identically"
    r = MODELS["M/M/1"](80, MU).run(500, 50, seed=3)
    little = abs(r["L"] - r["X"] * r["W"]) / r["L"]
    assert little < 0.02, f"Little's law violated: {little:.3%}"
    print(f"[verify] reproducibility OK; Little's law |L - X*W|/L = {little:.3%}")


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="R=5, short runs (smoke test)")
    ap.add_argument("-R", type=int, default=30)
    ap.add_argument("--arrivals", type=int, default=50_000, help="expected arrivals per run")
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--out", default=os.path.join("docs", "validation_results.md"))
    args = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    R, n_arr = (5, 10_000) if args.quick else (args.R, args.arrivals)

    verification_checks()
    jobs, cases = build_jobs(R, n_arr)
    print(f"[run] {len(jobs)} simulation runs on {args.workers} workers ...")
    t0 = time.perf_counter()
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        results = dict(ex.map(_one_run, jobs, chunksize=4))
    print(f"[run] done in {time.perf_counter() - t0:.1f} s wall-clock")

    rows, failures = evaluate(cases, results, R)
    print_table(rows)
    md = render_markdown(rows, R, n_arr)
    if not args.quick:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(md + "\n")
        print(f"[out] markdown table written to {args.out}")
    n_assert = sum(r["asserted"] for r in rows)
    print(f"[result] {n_assert - len(failures)}/{n_assert} asserted comparisons passed")
    if args.quick:
        print("[result] quick mode: smoke test only, thresholds not enforced")
        return rows
    assert not failures, "validation FAILED for: " + ", ".join(
        f"{f['model']} rho={f['rho']} {f['metric']} err={100 * f['err']:.2f}%" for f in failures)
    print("[result] ALL 6 SINGLE-QUEUE MODELS VALIDATED")
    return rows


if __name__ == "__main__":
    sys.exit(0 if main() is not None else 1)
