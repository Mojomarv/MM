"""Run the full backtest matrix and print a ranked comparison.

Sweeps:  markets × timeframes × strategies × parameter grids
For each: total return, annualized Sharpe, max drawdown, #trades, win rate,
exposure — all net of taker fees + slippage.

Also computes a buy-&-hold baseline per market/timeframe so we can see
whether a strategy actually beats just holding (the honest bar).

Usage:
    python backtest/run.py
    python backtest/run.py --markets BTC,ETH --tfs 15m,1h,4h --top 25
    python backtest/run.py --fee 5 --slip 2 --long-only
"""
from __future__ import annotations

import argparse
import itertools

import numpy as np
import pandas as pd

from engine import load_candles, run_backtest, compute_metrics, TIMEFRAMES
from strategies import STRATEGIES


def buy_hold(df: pd.DataFrame, tf: str) -> dict:
    ret = df["close"].pct_change().fillna(0.0)
    pos = pd.Series(1.0, index=df.index)
    turn = pd.Series(0.0, index=df.index); turn.iloc[0] = 1.0
    return compute_metrics(ret, pos, turn, tf)


def fmt_pct(x: float) -> str:
    if x != x:  # nan
        return "  n/a"
    return f"{x*100:+6.1f}%"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--markets", default="BTC,ETH,SOL,BNB,HYPE")
    ap.add_argument("--tfs", default="5m,15m,1h,4h")
    ap.add_argument("--fee", type=float, default=5.0, help="taker fee bps per side")
    ap.add_argument("--slip", type=float, default=2.0, help="slippage bps per side")
    ap.add_argument("--long-only", action="store_true")
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--min-trades", type=int, default=5,
                    help="ignore configs with fewer trades (overfit guard)")
    args = ap.parse_args()

    markets = [m.strip().upper() for m in args.markets.split(",") if m.strip()]
    tfs = [t.strip() for t in args.tfs.split(",") if t.strip()]

    rows: list[dict] = []
    baselines: dict[tuple[str, str], dict] = {}

    for sym in markets:
        for tf in tfs:
            try:
                df = load_candles(sym, tf)
            except FileNotFoundError as exc:
                print(f"! {exc}")
                continue
            if len(df) < 200:
                continue
            baselines[(sym, tf)] = buy_hold(df, tf)

            for strat_name, spec in STRATEGIES.items():
                fn = spec["fn"]
                for params in spec["grid"]:
                    kwargs = dict(params)
                    if args.long_only:
                        kwargs["long_only"] = True
                    sig = fn(df, **kwargs)
                    res = run_backtest(
                        df, sig, sym=sym, tf=tf, strategy=strat_name,
                        params=params, taker_fee_bps=args.fee,
                        slippage_bps=args.slip,
                    )
                    m = res.metrics
                    rows.append({
                        "sym": sym, "tf": tf, "family": spec["family"],
                        "strategy": strat_name, "params": _pstr(params),
                        **m,
                    })

    if not rows:
        print("No results — did you run fetch_data.py?")
        return

    res_df = pd.DataFrame(rows)
    # Overfit guard: require a minimum number of trades.
    res_df = res_df[res_df["n_trades"] >= args.min_trades].copy()

    # ── Top configs by Sharpe ────────────────────────────────────────
    print("\n" + "=" * 96)
    print(f"TOP {args.top} CONFIGS BY SHARPE  "
          f"(fee={args.fee}bp/side slip={args.slip}bp/side "
          f"{'LONG-ONLY' if args.long_only else 'long/short'})")
    print("=" * 96)
    print(f"{'sym':<5}{'tf':<5}{'family':<12}{'strategy':<16}{'params':<22}"
          f"{'ret':>8}{'sharpe':>8}{'maxDD':>8}{'trades':>7}{'win':>6}{'expo':>6}")
    print("-" * 96)
    top = res_df.sort_values("sharpe", ascending=False).head(args.top)
    for _, r in top.iterrows():
        print(f"{r['sym']:<5}{r['tf']:<5}{r['family']:<12}{r['strategy']:<16}"
              f"{r['params']:<22}{fmt_pct(r['total_return']):>8}"
              f"{r['sharpe']:>8.2f}{fmt_pct(r['max_drawdown']):>8}"
              f"{r['n_trades']:>7}{r['win_rate']*100:>5.0f}%{r['exposure']*100:>5.0f}%")

    # ── Buy-&-hold baselines ─────────────────────────────────────────
    print("\n" + "=" * 60)
    print("BUY-&-HOLD BASELINE (per market, any tf gives same)")
    print("=" * 60)
    print(f"{'sym':<6}{'ret':>9}{'sharpe':>9}{'maxDD':>9}")
    seen = set()
    for (sym, tf), m in baselines.items():
        if sym in seen:
            continue
        seen.add(sym)
        print(f"{sym:<6}{fmt_pct(m['total_return']):>9}{m['sharpe']:>9.2f}"
              f"{fmt_pct(m['max_drawdown']):>9}")

    # ── Aggregate: which family×tf is systematically best? ──────────
    print("\n" + "=" * 72)
    print("AVG SHARPE BY FAMILY × TIMEFRAME  (mean across markets & params)")
    print("=" * 72)
    piv = res_df.pivot_table(index="family", columns="tf",
                              values="sharpe", aggfunc="mean")
    # order tfs sensibly
    cols = [t for t in TIMEFRAMES if t in piv.columns]
    piv = piv.reindex(columns=cols)
    print(piv.round(2).to_string())

    # ── How many configs beat buy-&-hold on Sharpe? ─────────────────
    bh_sharpe = np.mean([m["sharpe"] for m in baselines.values()])
    n_beat = int((res_df["sharpe"] > max(bh_sharpe, 0)).sum())
    n_pos = int((res_df["total_return"] > 0).sum())
    print(f"\nConfigs tested (>= {args.min_trades} trades): {len(res_df)}")
    print(f"  positive net return: {n_pos} ({n_pos/len(res_df)*100:.0f}%)")
    print(f"  Sharpe > max(buy-hold, 0): {n_beat} ({n_beat/len(res_df)*100:.0f}%)")

    # Save full results for later inspection (path relative to this file).
    from pathlib import Path as _Path
    out = res_df.sort_values("sharpe", ascending=False)
    out_path = _Path(__file__).resolve().parent / "results.csv"
    out.to_csv(out_path, index=False)
    print(f"\nFull results -> {out_path}")


def _pstr(params: dict) -> str:
    return ",".join(f"{k}={v}" for k, v in params.items())


if __name__ == "__main__":
    main()
