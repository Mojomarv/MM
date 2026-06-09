"""Generalization test — the honest way to detect edge.

The walk-forward's "pick the best config on train" approach is biased: it
systematically selects the most OVERFIT strategy (the one that curve-fit
the training noise hardest), which then fails out-of-sample. That tells
us about overfitting but not about real edge.

This test does the opposite. We choose a small set of FIXED, a-priori
sensible configs (NOT optimized on this data) and ask one question:

    Does the SAME rule, with the SAME params, make money across
    MULTIPLE markets in the OUT-OF-SAMPLE period?

A rule that works on BTC+ETH+SOL+BNB out-of-sample with identical params
has real signal. A rule that needs different params per market is
curve-fit. Consistency across markets is the strongest edge evidence we
can get from limited history.

Usage:
    python backtest/generalize.py --tf 1h
    python backtest/generalize.py --tf 4h --markets BTC,ETH,SOL,BNB,HYPE
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from engine import load_candles, run_backtest
import strategies as S

# A-priori candidate rules — chosen from the family aggregate (trend &
# combo on higher TFs looked structurally best), NOT tuned per market.
CANDIDATES = [
    ("ema_cross 12/26",      S.ema_crossover,      {"fast": 12, "slow": 26}),
    ("ema_cross 21/55",      S.ema_crossover,      {"fast": 21, "slow": 55}),
    ("donchian 20",          S.donchian_breakout,  {"n": 20}),
    ("donchian 40",          S.donchian_breakout,  {"n": 40}),
    ("supertrend 10/3",      S.supertrend_follow,  {"n": 10, "mult": 3.0}),
    ("atr_breakout 20/0.5",  S.atr_breakout,       {"n": 20, "mult": 0.5}),
    ("wavetrend_cross 10/21", S.wavetrend_cross,   {"n1": 10, "n2": 21}),
    ("adx_regime 25",        S.adx_regime,         {"adx_n": 14, "adx_min": 25, "fast": 12, "slow": 26}),
    ("confluence 2",         S.confluence_vote,    {"threshold": 2}),
    ("confluence 3",         S.confluence_vote,    {"threshold": 3}),
]


def split_metrics(sym, tf, fn, params, fee, slip, train_frac):
    df = load_candles(sym, tf)
    split = int(len(df) * train_frac)
    out = {}
    for label, sub in (("train", df.iloc[:split]), ("test", df.iloc[split:])):
        sig = fn(sub, **params)
        r = run_backtest(sub, sig, sym=sym, tf=tf, strategy="", params=params,
                          taker_fee_bps=fee, slippage_bps=slip)
        out[label] = r.metrics
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--markets", default="BTC,ETH,SOL,BNB,HYPE")
    ap.add_argument("--tf", default="1h")
    ap.add_argument("--fee", type=float, default=5.0)
    ap.add_argument("--slip", type=float, default=2.0)
    ap.add_argument("--train-frac", type=float, default=0.67)
    args = ap.parse_args()

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]

    print("\n" + "=" * 100)
    print(f"GENERALIZATION TEST @ {args.tf}  (same params across all markets, "
          f"OOS = last {1-args.train_frac:.0%})")
    print("=" * 100)

    summary = []
    for label, fn, params in CANDIDATES:
        test_sharpes, test_rets = [], []
        cells = []
        for sym in markets:
            try:
                m = split_metrics(sym, args.tf, fn, params, args.fee,
                                   args.slip, args.train_frac)
            except FileNotFoundError:
                continue
            ts = m["test"]["sharpe"]
            tr = m["test"]["total_return"]
            test_sharpes.append(ts)
            test_rets.append(tr)
            cells.append((sym, m["train"]["sharpe"], ts, tr))

        if not test_sharpes:
            continue
        mean_oos = float(np.mean(test_sharpes))
        n_pos = sum(s > 0 for s in test_sharpes)
        n = len(test_sharpes)
        summary.append((label, mean_oos, n_pos, n, np.mean(test_rets)))

        flag = "  <== consistent" if n_pos >= max(1, int(0.8 * n)) and mean_oos > 0.3 else ""
        print(f"\n{label:<22} | OOS mean Sharpe {mean_oos:+.2f} | "
              f"markets positive OOS: {n_pos}/{n} | "
              f"mean OOS ret {np.mean(test_rets)*100:+.1f}%{flag}")
        print(f"  {'market':<6}{'train_shrp':>11}{'test_shrp':>11}{'test_ret':>10}")
        for sym, tr_s, te_s, te_r in cells:
            mark = "" if te_s > 0 else "  (lost)"
            print(f"  {sym:<6}{tr_s:>11.2f}{te_s:>11.2f}{te_r*100:>9.1f}%{mark}")

    # ── ranked summary ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print(f"RANKED BY OUT-OF-SAMPLE CONSISTENCY @ {args.tf}")
    print("=" * 70)
    print(f"{'rule':<22}{'OOS Sharpe':>12}{'mkts +OOS':>12}{'OOS ret':>10}")
    for label, mean_oos, n_pos, n, mret in sorted(summary, key=lambda x: -x[1]):
        print(f"{label:<22}{mean_oos:>12.2f}{f'{n_pos}/{n}':>12}{mret*100:>9.1f}%")
    print("\nReal edge = same params, positive across most markets, OOS. "
          "\nNeeds per-market tuning to work = overfit.")


if __name__ == "__main__":
    main()
