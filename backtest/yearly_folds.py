"""Per-year confirmation of the funding-crowding veto across a full cycle.

One train/test split can still be lucky. This slices 4.4 years (2022 bear
-> 2026) into calendar-year folds and asks the demanding question: does
ema 21/55 long-only + crowd-veto beat the bare base in EACH year
independently — bear, recovery, bull, chop?

Signals/z-scores are computed over the FULL series (rolling look-back
only, no leakage) and then sliced per year for evaluation. Basket =
equal-weight 4 coins. fee=0, slip=1bp/side (zero-fee venue thesis).

Usage:
    python backtest/yearly_folds.py --thr 0.75
"""
from __future__ import annotations

import argparse

import numpy as np
import pandas as pd

from engine import run_backtest, TIMEFRAMES, MINUTES_PER_YEAR
import strategies as S
import altdata_overlay as A


def seg(net: pd.Series, tf: str) -> dict:
    if len(net) < 5:
        return {"ret": 0.0, "sharpe": 0.0, "maxdd": 0.0}
    bpy = MINUTES_PER_YEAR / TIMEFRAMES[tf][1]
    sd = net.std(ddof=0)
    eq = (1 + net).cumprod()
    return {"ret": float((1 + net).prod() - 1),
            "sharpe": float(net.mean() / sd * np.sqrt(bpy)) if sd > 0 else 0.0,
            "maxdd": float((eq / eq.cummax() - 1).min())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--tf", default="15m")
    ap.add_argument("--thr", type=float, default=0.75)
    ap.add_argument("--fee", type=float, default=0.0)
    ap.add_argument("--slip", type=float, default=1.0)
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    base_nets, veto_nets, bh_nets = [], [], []
    for sym in syms:
        df = A.load_15m(sym)
        base = S.ema_crossover(df, 21, 55, long_only=True)
        feats = A.daily_features(sym)
        absz = A.align_daily_to_15m(feats["funding_absz"], df.index).fillna(0.0)
        veto = base.where(~(absz > args.thr), 0.0)

        rb = run_backtest(df, base, sym=sym, tf=args.tf, strategy="", params={},
                           taker_fee_bps=args.fee, slippage_bps=args.slip)
        rv = run_backtest(df, veto, sym=sym, tf=args.tf, strategy="", params={},
                           taker_fee_bps=args.fee, slippage_bps=args.slip)
        base_nets.append(rb.net_ret.rename(sym))
        veto_nets.append(rv.net_ret.rename(sym))
        bh_nets.append(df["close"].pct_change().fillna(0.0).rename(sym))

    base_bk = pd.concat(base_nets, axis=1, join="inner").mean(axis=1)
    veto_bk = pd.concat(veto_nets, axis=1, join="inner").mean(axis=1)
    bh_bk = pd.concat(bh_nets, axis=1, join="inner").mean(axis=1)

    years = sorted({d.year for d in base_bk.index})

    print("\n" + "=" * 92)
    print(f"PER-YEAR FOLDS — ema 21/55 long-only +/- crowd-veto(|fund|z>{args.thr})  "
          f"(fee={args.fee} slip={args.slip}, basket)")
    print("=" * 92)
    print(f"{'year':<7}{'regime':<8}{'B&H ret':>9} | "
          f"{'base ret':>9}{'base DD':>9} | "
          f"{'veto ret':>9}{'veto shp':>9}{'veto DD':>9}{'delta':>9}")
    print("-" * 92)

    veto_wins = 0
    n_years = 0
    for y in years:
        mask = base_bk.index.year == y
        bh_m = seg(bh_bk[mask], args.tf)
        b_m = seg(base_bk[mask], args.tf)
        v_m = seg(veto_bk[mask], args.tf)
        regime = ("BULL" if bh_m["ret"] > 0.25 else
                  "BEAR" if bh_m["ret"] < -0.25 else "chop")
        delta = (v_m["ret"] - b_m["ret"]) * 100
        # only count "full-ish" years for the win tally
        if mask.sum() > 5000:
            n_years += 1
            if delta > -1:
                veto_wins += 1
        print(f"{y:<7}{regime:<8}{bh_m['ret']*100:>+8.1f}% | "
              f"{b_m['ret']*100:>+8.1f}%{b_m['maxdd']*100:>+8.1f}% | "
              f"{v_m['ret']*100:>+8.1f}%{v_m['sharpe']:>9.2f}{v_m['maxdd']*100:>+8.1f}%"
              f"{delta:>+8.1f}pp")

    # full-period
    print("-" * 92)
    bh_f, b_f, v_f = seg(bh_bk, args.tf), seg(base_bk, args.tf), seg(veto_bk, args.tf)
    print(f"{'ALL':<7}{'cycle':<8}{bh_f['ret']*100:>+8.1f}% | "
          f"{b_f['ret']*100:>+8.1f}%{b_f['maxdd']*100:>+8.1f}% | "
          f"{v_f['ret']*100:>+8.1f}%{v_f['sharpe']:>9.2f}{v_f['maxdd']*100:>+8.1f}%"
          f"{(v_f['ret']-b_f['ret'])*100:>+8.1f}pp")

    print(f"\nVeto did not hurt in {veto_wins}/{n_years} full years.")
    print(f"Full cycle: buy-&-hold {bh_f['ret']*100:+.0f}% | "
          f"base {b_f['ret']*100:+.0f}% (DD {b_f['maxdd']*100:.0f}%) | "
          f"veto {v_f['ret']*100:+.0f}% (DD {v_f['maxdd']*100:.0f}%, "
          f"Sharpe {v_f['sharpe']:.2f})")


if __name__ == "__main__":
    main()
