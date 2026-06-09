"""Out-of-sample validation — the test that separates edge from overfit.

For each market × timeframe we:
  1. Split history into TRAIN (first 2/3) and TEST (last 1/3).
  2. On TRAIN, pick the single best config (by Sharpe) across ALL
     strategies+params.
  3. Apply that exact config, untouched, to TEST.
  4. Compare in-sample vs out-of-sample Sharpe.

If IS Sharpe is high but OOS collapses to ~0 or negative, the strategy
was curve-fit. If OOS holds up, there may be real signal.

Usage:
    python backtest/walkforward.py --markets BTC,ETH,SOL,BNB,HYPE --tfs 15m,1h,4h
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from engine import load_candles, run_backtest
from strategies import STRATEGIES


def best_on(df: pd.DataFrame, sym: str, tf: str, fee: float, slip: float,
            long_only: bool, min_trades: int):
    """Return (strategy, params, metrics) with the highest Sharpe on df."""
    best = None
    for name, spec in STRATEGIES.items():
        for params in spec["grid"]:
            kw = dict(params)
            if long_only:
                kw["long_only"] = True
            sig = spec["fn"](df, **kw)
            res = run_backtest(df, sig, sym=sym, tf=tf, strategy=name,
                                params=params, taker_fee_bps=fee,
                                slippage_bps=slip)
            if res.metrics["n_trades"] < min_trades:
                continue
            if best is None or res.metrics["sharpe"] > best[2]["sharpe"]:
                best = (name, params, res.metrics, spec["fn"])
    return best


def apply_config(df, sym, tf, fn, params, fee, slip, long_only):
    kw = dict(params)
    if long_only:
        kw["long_only"] = True
    sig = fn(df, **kw)
    return run_backtest(df, sig, sym=sym, tf=tf, strategy="", params=params,
                         taker_fee_bps=fee, slippage_bps=slip).metrics


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--markets", default="BTC,ETH,SOL,BNB,HYPE")
    ap.add_argument("--tfs", default="15m,1h,4h")
    ap.add_argument("--fee", type=float, default=5.0)
    ap.add_argument("--slip", type=float, default=2.0)
    ap.add_argument("--long-only", action="store_true")
    ap.add_argument("--min-trades", type=int, default=15)
    ap.add_argument("--train-frac", type=float, default=0.67)
    args = ap.parse_args()

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]
    tfs = [t.strip() for t in args.tfs.split(",") if t.strip()]

    print("\n" + "=" * 104)
    print(f"WALK-FORWARD (train {args.train_frac:.0%} -> test {1-args.train_frac:.0%}, "
          f"fee={args.fee}bp slip={args.slip}bp "
          f"{'LONG-ONLY' if args.long_only else 'long/short'}, "
          f"min {args.min_trades} trades in-sample)")
    print("=" * 104)
    print(f"{'sym':<5}{'tf':<5}{'best train config':<34}"
          f"{'IS ret':>8}{'IS shrp':>8}{'  |':>3}{'OOS ret':>9}{'OOS shrp':>9}"
          f"{'OOS trd':>8}{'verdict':>12}")
    print("-" * 104)

    held_up = 0
    total = 0
    oos_sharpes = []
    for sym in markets:
        for tf in tfs:
            try:
                df = load_candles(sym, tf)
            except FileNotFoundError:
                continue
            if len(df) < 300:
                continue
            split = int(len(df) * args.train_frac)
            train, test = df.iloc[:split], df.iloc[split:]

            best = best_on(train, sym, tf, args.fee, args.slip,
                           args.long_only, args.min_trades)
            if best is None:
                continue
            name, params, is_m, fn = best
            oos_m = apply_config(test, sym, tf, fn, params,
                                  args.fee, args.slip, args.long_only)

            total += 1
            oos_sharpes.append(oos_m["sharpe"])
            verdict = "HELD UP" if oos_m["sharpe"] > 0.5 else (
                      "decayed" if oos_m["sharpe"] > 0 else "FAILED")
            if oos_m["sharpe"] > 0.5:
                held_up += 1

            cfg = f"{name} {_pstr(params)}"
            print(f"{sym:<5}{tf:<5}{cfg:<34}"
                  f"{is_m['total_return']*100:>+7.1f}%{is_m['sharpe']:>8.2f}{'  |':>3}"
                  f"{oos_m['total_return']*100:>+8.1f}%{oos_m['sharpe']:>9.2f}"
                  f"{oos_m['n_trades']:>8}{verdict:>12}")

    print("-" * 104)
    if total:
        print(f"\nOut-of-sample summary across {total} market×tf cells:")
        print(f"  HELD UP (OOS Sharpe > 0.5): {held_up}/{total} "
              f"({held_up/total*100:.0f}%)")
        print(f"  mean OOS Sharpe: {np.mean(oos_sharpes):.2f}  "
              f"median: {np.median(oos_sharpes):.2f}")
        print(f"  OOS Sharpe > 0: {sum(s>0 for s in oos_sharpes)}/{total}")
        print("\nReminder: high IS Sharpe + collapsed OOS = curve-fit, not edge.")


def _pstr(params: dict) -> str:
    return ",".join(f"{k}={v}" for k, v in params.items())


if __name__ == "__main__":
    main()
