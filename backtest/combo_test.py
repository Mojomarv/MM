"""Combination sweep with built-in overfit discipline.

Combines base long-only signals with higher-timeframe regime gates and
ensemble voting, on 2 years of Binance 15m data (4-coin basket).

Methodology guards:
  * Signals are computed ONCE over the full series (no per-window warmup
    truncation; matches how a live system carries state).
  * SELECTION on year 1, CONFIRMATION on year 2. With ~50 combos tested,
    year-1 rankings alone are guaranteed to contain lucky configs; only
    combos whose year-2 (untouched) performance holds up count.
  * Basket-level evaluation (equal-weight across coins) so a combo must
    work broadly, not on one lucky coin.

Combo space:
  bases:     ema 12/26, ema 21/55, donchian 20/40, atr_breakout 20/0.5,
             supertrend 10/3, rsi dip-buy 14/35  (all long-or-flat)
  gates:     none, price > SMA(200/400/800) on 15m  (~2/4/8 days)
  ensembles: 2-of-3 and 3-of-3 votes of {ema 21/55, donchian 40, supertrend}

Usage:
    python backtest/combo_test.py --fee 0 --slip 1
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from engine import run_backtest, TIMEFRAMES, MINUTES_PER_YEAR
import strategies as S

DATA_DIR = Path(__file__).resolve().parent / "data"


def load_binance(sym: str, tf: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / f"{sym}_binance_{tf}.csv")
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df.set_index("dt").sort_index()[["open", "high", "low", "close", "volume"]]


def build_components(df: pd.DataFrame) -> dict[str, pd.Series]:
    """All base long-only signals + regime gates, computed once per symbol."""
    c = df["close"]
    comps = {
        "ema_12_26":  S.ema_crossover(df, 12, 26, long_only=True),
        "ema_21_55":  S.ema_crossover(df, 21, 55, long_only=True),
        "don_20":     S.donchian_breakout(df, 20, long_only=True),
        "don_40":     S.donchian_breakout(df, 40, long_only=True),
        "atr_20_05":  S.atr_breakout(df, 20, 14, 0.5, long_only=True),
        "st_10_3":    S.supertrend_follow(df, 10, 3.0, long_only=True),
        "rsi_dip":    S.rsi_reversion(df, 14, 35.0, 65.0, long_only=True),
    }
    for n in (200, 400, 800):
        comps[f"gate_{n}"] = (c > c.rolling(n, min_periods=n).mean()).astype(float)
    return comps


BASES = ["ema_12_26", "ema_21_55", "don_20", "don_40",
         "atr_20_05", "st_10_3", "rsi_dip"]
GATES: list[int | None] = [None, 200, 400, 800]
ENSEMBLES = [
    ("vote2(e21,d40,st)", ["ema_21_55", "don_40", "st_10_3"], 2),
    ("vote3(e21,d40,st)", ["ema_21_55", "don_40", "st_10_3"], 3),
    ("vote2(e12,d20,st)", ["ema_12_26", "don_20", "st_10_3"], 2),
]


def combo_signal(comps: dict[str, pd.Series], base: str | tuple,
                  gate: int | None) -> pd.Series:
    if isinstance(base, tuple):                      # ensemble: (members, k)
        members, k = base
        votes = sum(comps[m] for m in members)
        sig = (votes >= k).astype(float)
    else:
        sig = comps[base]
    if gate is not None:
        sig = sig * comps[f"gate_{gate}"]
    return sig


def seg_metrics(net_ret: pd.Series, tf: str) -> dict:
    """Return/sharpe/maxDD for an arbitrary slice of a net-return series."""
    if len(net_ret) == 0:
        return {"ret": 0.0, "sharpe": 0.0, "maxdd": 0.0}
    bars_per_year = MINUTES_PER_YEAR / TIMEFRAMES[tf][1]
    total = float((1 + net_ret).prod() - 1)
    sd = net_ret.std(ddof=0)
    sharpe = float(net_ret.mean() / sd * np.sqrt(bars_per_year)) if sd > 0 else 0.0
    eq = (1 + net_ret).cumprod()
    maxdd = float((eq / eq.cummax() - 1).min())
    return {"ret": total, "sharpe": sharpe, "maxdd": maxdd}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--tf", default="15m")
    ap.add_argument("--fee", type=float, default=0.0)
    ap.add_argument("--slip", type=float, default=1.0)
    ap.add_argument("--top", type=int, default=20)
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    # Load data + compute component signals once per symbol.
    frames: dict[str, pd.DataFrame] = {}
    comps: dict[str, dict[str, pd.Series]] = {}
    for sym in syms:
        frames[sym] = load_binance(sym, args.tf)
        comps[sym] = build_components(frames[sym])
        print(f"  components ready: {sym} ({len(frames[sym])} bars)")

    # Build the full combo list.
    combos: list[tuple[str, str | tuple, int | None]] = []
    for b in BASES:
        for g in GATES:
            combos.append((f"{b}" + (f" +gate{g}" if g else ""), b, g))
    for label, members, k in ENSEMBLES:
        for g in GATES:
            combos.append((f"{label}" + (f" +gate{g}" if g else ""),
                           (members, k), g))
    print(f"\nSweeping {len(combos)} combos over {len(syms)}-coin basket "
          f"(fee={args.fee}bp slip={args.slip}bp/side, long-only)\n")

    # Buy-&-hold basket baseline.
    bh = pd.concat({s: frames[s]["close"].pct_change().fillna(0.0)
                    for s in syms}, axis=1, join="inner").mean(axis=1)
    mid = bh.index[len(bh) // 2]
    bh_y1, bh_y2 = seg_metrics(bh[bh.index < mid], args.tf), seg_metrics(bh[bh.index >= mid], args.tf)

    rows = []
    for label, base, gate in combos:
        nets, trades = [], []
        for sym in syms:
            sig = combo_signal(comps[sym], base, gate)
            res = run_backtest(frames[sym], sig, sym=sym, tf=args.tf,
                                strategy=label, params={},
                                taker_fee_bps=args.fee, slippage_bps=args.slip)
            nets.append(res.net_ret.rename(sym))
            trades.append(res.metrics["n_trades"])
        basket = pd.concat(nets, axis=1, join="inner").mean(axis=1)
        y1, y2 = basket[basket.index < mid], basket[basket.index >= mid]
        m1, m2, mf = seg_metrics(y1, args.tf), seg_metrics(y2, args.tf), seg_metrics(basket, args.tf)
        rows.append({
            "combo": label, "base": base, "gate": gate,
            "y1_ret": m1["ret"], "y1_sharpe": m1["sharpe"],
            "y2_ret": m2["ret"], "y2_sharpe": m2["sharpe"],
            "full_ret": mf["ret"], "full_maxdd": mf["maxdd"],
            "trades_yr": int(np.mean(trades) / 2),
        })

    res_df = pd.DataFrame(rows).sort_values("y1_sharpe", ascending=False)

    print("=" * 100)
    print(f"SELECT ON YEAR 1 -> CONFIRM ON YEAR 2   "
          f"(B&H basket: y1 {bh_y1['ret']*100:+.0f}%/{bh_y1['sharpe']:.1f}  "
          f"y2 {bh_y2['ret']*100:+.0f}%/{bh_y2['sharpe']:.1f})")
    print("=" * 100)
    print(f"{'combo':<28}{'y1 ret':>8}{'y1 shp':>8} | {'y2 ret':>8}{'y2 shp':>8}"
          f"{'full ret':>10}{'maxDD':>8}{'trd/yr':>8}  verdict")
    print("-" * 100)
    for _, r in res_df.head(args.top).iterrows():
        if r["y2_sharpe"] > 0.7 and r["y2_ret"] > 0:
            verdict = "CONFIRMED"
        elif r["y2_sharpe"] > 0:
            verdict = "weak"
        else:
            verdict = "failed"
        print(f"{r['combo']:<28}{r['y1_ret']*100:>+7.1f}%{r['y1_sharpe']:>8.2f} |"
              f"{r['y2_ret']*100:>+7.1f}%{r['y2_sharpe']:>8.2f}"
              f"{r['full_ret']*100:>+9.1f}%{r['full_maxdd']*100:>+7.1f}%"
              f"{r['trades_yr']:>8}  {verdict}")

    # Quarter-by-quarter detail for the best CONFIRMED combo.
    confirmed = res_df[(res_df["y2_sharpe"] > 0.7) & (res_df["y2_ret"] > 0)]
    if len(confirmed):
        best = confirmed.iloc[0]
        label, base, gate = best["combo"], best["base"], best["gate"]
        nets = []
        for sym in syms:
            sig = combo_signal(comps[sym], base, gate)
            res = run_backtest(frames[sym], sig, sym=sym, tf=args.tf,
                                strategy=label, params={},
                                taker_fee_bps=args.fee, slippage_bps=args.slip)
            nets.append(res.net_ret.rename(sym))
        basket = pd.concat(nets, axis=1, join="inner").mean(axis=1)
        edges = pd.date_range(basket.index[0], basket.index[-1], periods=9)
        print("\n" + "=" * 76)
        print(f"QUARTERLY DETAIL — best confirmed: {label}")
        print("=" * 76)
        print(f"{'quarter':<18}{'regime':<7}{'B&H':>9}{'strategy':>11}{'maxDD':>9}")
        print("-" * 76)
        for i in range(8):
            lo, hi = edges[i], edges[i + 1]
            mask = (basket.index >= lo) & ((basket.index < hi) if i < 7 else (basket.index <= hi))
            q = basket[mask]
            qb = bh[(bh.index >= lo) & ((bh.index < hi) if i < 7 else (bh.index <= hi))]
            mb, mq = seg_metrics(qb, args.tf), seg_metrics(q, args.tf)
            reg = "BULL" if mb["ret"] > 0.15 else ("BEAR" if mb["ret"] < -0.15 else "chop")
            print(f"{lo.strftime('%Y-%m')}->{hi.strftime('%y-%m'):<8}{reg:<7}"
                  f"{mb['ret']*100:>+8.1f}%{mq['ret']*100:>+10.1f}%{mq['maxdd']*100:>+8.1f}%")
    else:
        print("\nNo combo confirmed on year 2. The sweep found nothing robust.")

    out_path = Path(__file__).resolve().parent / "combo_results.csv"
    res_df.to_csv(out_path, index=False)
    print(f"\nFull results -> {out_path}")


if __name__ == "__main__":
    main()
