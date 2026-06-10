"""Multi-regime validation on 2 years of Binance 15m data.

The 60-day Rise sample was a single downtrend. A trend/breakout rule that
shorts a falling market looks great there but may die in a bull rally or
get whipsawed to death in chop. This test splits 2 years into quarters,
labels each quarter's regime by buy-&-hold, and checks whether the SAME
fixed rule survives across all of them.

Decisive question: is the rule net-positive across bull + bear + chop,
and is its worst-regime drawdown survivable?

Usage:
    python backtest/regime_test.py --tf 15m --fee 0 --slip 1
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from engine import run_backtest, compute_metrics
import strategies as S

DATA_DIR = Path(__file__).resolve().parent / "data"

# The candidates that generalized across markets OOS at 15m.
CANDIDATES = [
    ("ema_cross 21/55",     S.ema_crossover,     {"fast": 21, "slow": 55}),
    ("donchian 40",         S.donchian_breakout, {"n": 40}),
    ("atr_breakout 20/0.5", S.atr_breakout,      {"n": 20, "mult": 0.5}),
    ("adx_regime 25",       S.adx_regime,        {"adx_n": 14, "adx_min": 25, "fast": 12, "slow": 26}),
    ("ema_cross 12/26",     S.ema_crossover,     {"fast": 12, "slow": 26}),
]


def load_binance(sym: str, tf: str) -> pd.DataFrame:
    path = DATA_DIR / f"{sym}_binance_{tf}.csv"
    df = pd.read_csv(path)
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df.set_index("dt").sort_index()[["open", "high", "low", "close", "volume"]]


def regime_label(bh_ret: float) -> str:
    if bh_ret > 0.15:  return "BULL"
    if bh_ret < -0.15: return "BEAR"
    return "chop"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--tf", default="15m")
    ap.add_argument("--fee", type=float, default=0.0)
    ap.add_argument("--slip", type=float, default=1.0)
    ap.add_argument("--quarters", type=int, default=8)
    ap.add_argument("--long-only", action="store_true",
                    help="go long-or-flat, never short (crypto has upward drift)")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    # Aggregate across markets per quarter: average the candidate's return
    # over all symbols, so we judge a rule on the basket not one coin.
    for label, fn, params in CANDIDATES:
        print("\n" + "=" * 88)
        print(f"{label}   (fee={args.fee}bp slip={args.slip}bp/side, "
              f"basket of {len(syms)})")
        print("=" * 88)
        print(f"{'quarter':<20}{'regime':<7}{'BH ret':>9}{'strat ret':>11}"
              f"{'sharpe':>8}{'maxDD':>8}{'trades':>8}")
        print("-" * 88)

        # collect per-quarter aggregated stats
        q_returns: list[float] = []
        worst = 1e9
        regime_rows: dict[str, list[float]] = {"BULL": [], "BEAR": [], "chop": []}

        # Build the union quarter grid from BTC (all share the same span).
        base = load_binance(syms[0], args.tf)
        edges = pd.date_range(base.index[0], base.index[-1],
                              periods=args.quarters + 1)

        for qi in range(args.quarters):
            lo, hi = edges[qi], edges[qi + 1]
            strat_rets, bh_rets, sharpes, dds, trades = [], [], [], [], []
            for sym in syms:
                df = load_binance(sym, args.tf)
                win = df[(df.index >= lo) & (df.index < hi)]
                if len(win) < 100:
                    continue
                kw = dict(params)
                if args.long_only:
                    kw["long_only"] = True
                sig = fn(win, **kw)
                res = run_backtest(win, sig, sym=sym, tf=args.tf, strategy="",
                                    params=params, taker_fee_bps=args.fee,
                                    slippage_bps=args.slip)
                strat_rets.append(res.metrics["total_return"])
                sharpes.append(res.metrics["sharpe"])
                dds.append(res.metrics["max_drawdown"])
                trades.append(res.metrics["n_trades"])
                bh_rets.append(win["close"].iloc[-1] / win["close"].iloc[0] - 1)

            if not strat_rets:
                continue
            avg_strat = float(np.mean(strat_rets))
            avg_bh = float(np.mean(bh_rets))
            reg = regime_label(avg_bh)
            q_returns.append(avg_strat)
            worst = min(worst, avg_strat)
            regime_rows[reg].append(avg_strat)

            qlabel = f"{lo.strftime('%Y-%m')}->{hi.strftime('%m')}"
            print(f"{qlabel:<20}{reg:<7}{avg_bh*100:>+8.1f}%{avg_strat*100:>+10.1f}%"
                  f"{np.mean(sharpes):>8.2f}{np.mean(dds)*100:>+7.1f}%"
                  f"{int(np.mean(trades)):>8}")

        # summary
        n_pos = sum(r > 0 for r in q_returns)
        nq = len(q_returns)
        print("-" * 88)
        print(f"  quarters positive: {n_pos}/{nq}   "
              f"mean q-return: {np.mean(q_returns)*100:+.1f}%   "
              f"worst q: {worst*100:+.1f}%")
        for reg in ("BULL", "BEAR", "chop"):
            rs = regime_rows[reg]
            if rs:
                print(f"    {reg:<5} ({len(rs)}q): avg {np.mean(rs)*100:+.1f}%  "
                      f"win {sum(r>0 for r in rs)}/{len(rs)}")

    print("\n" + "=" * 88)
    print("Robust edge = positive across BULL, BEAR and chop, survivable worst quarter.")
    print("Dies in chop = trend-follower whipsaw; consider ADX-gating or skip.")


if __name__ == "__main__":
    main()
