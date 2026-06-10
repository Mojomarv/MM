"""Phase 3: does alt-data IMPROVE the trend base out-of-sample?

IC is necessary but not sufficient. A feature only matters if layering it
onto the surviving price strategy (ema 21/55 long-only @ 15m) raises
year-2 (untouched) performance above the bare base.

Overlays tested (daily features, point-in-time lagged 1 day, applied as a
multiplier on the 15m position):
  base            ema 21/55 long-only, no overlay
  +crowd_veto     position -> 0 when |funding| z-score > 1 (extreme leverage)
  +crowd_half     position *= 0.5 when |funding| z-score > 1
  +froth_gate     long only when froth (fng_z*funding_z) >= 0
  +froth_size     position *= scaled froth in [0.3 .. 1.0]

Verdict logic: an overlay must improve BOTH years (or improve y2 without
wrecking y1) to count. If the bare base wins, alt-data added nothing here.

Usage:
    python backtest/altdata_overlay.py --fee 0 --slip 1
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from engine import run_backtest, TIMEFRAMES, MINUTES_PER_YEAR
import strategies as S

DATA_DIR = Path(__file__).resolve().parent / "data"


def load_15m(sym: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / f"{sym}_binance_15m.csv")
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    return df.set_index("dt").sort_index()[["open", "high", "low", "close", "volume"]]


def _z(s, n):
    m = s.rolling(n, min_periods=n // 2).mean()
    sd = s.rolling(n, min_periods=n // 2).std(ddof=0)
    return (s - m) / sd.replace(0.0, np.nan)


def daily_features(sym: str) -> pd.DataFrame:
    """Daily funding_abs_z and froth, lagged 1 day (point-in-time)."""
    f = pd.read_csv(DATA_DIR / f"funding_{sym}.csv")
    f["dt"] = pd.to_datetime(f["ts"], unit="s", utc=True)
    fund = f.set_index("dt")["funding"].resample("1D").mean()

    g = pd.read_csv(DATA_DIR / "fear_greed.csv")
    g["dt"] = pd.to_datetime(g["ts"], unit="s", utc=True).dt.floor("D")
    fng = g.set_index("dt")["value"].astype(float)

    d = pd.DataFrame({"funding": fund}).join(fng.rename("fng"), how="left")
    d["fng"] = d["fng"].ffill()
    d["funding_absz"] = _z(d["funding"].abs(), 30)
    d["froth"] = _z(d["fng"], 30) * _z(d["funding"], 30)
    # lag 1 day: value known at close of D applies to D+1 onward
    return d[["funding_absz", "froth"]].shift(1)


def align_daily_to_15m(daily: pd.Series, idx: pd.DatetimeIndex) -> pd.Series:
    """Forward-fill a lagged daily feature onto the 15m grid."""
    return daily.reindex(idx.union(daily.index)).ffill().reindex(idx)


def overlays(base: pd.Series, absz: pd.Series, froth: pd.Series) -> dict:
    out = {"base": base}
    out["+crowd_veto"] = base.where(~(absz > 1.0), 0.0)
    out["+crowd_half"] = base * np.where(absz > 1.0, 0.5, 1.0)
    out["+froth_gate"] = base.where(~(froth < 0), 0.0)
    fmult = froth.clip(-1, 1).add(1).div(2).clip(0.3, 1.0).fillna(1.0)  # [0.3,1]
    out["+froth_size"] = base * fmult
    return out


def seg(net: pd.Series, tf: str) -> dict:
    if len(net) == 0:
        return {"ret": 0, "sharpe": 0, "maxdd": 0}
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
    ap.add_argument("--fee", type=float, default=0.0)
    ap.add_argument("--slip", type=float, default=1.0)
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    # Build per-overlay basket net-return series.
    overlay_names = ["base", "+crowd_veto", "+crowd_half", "+froth_gate", "+froth_size"]
    per_overlay: dict[str, list[pd.Series]] = {k: [] for k in overlay_names}

    for sym in syms:
        df = load_15m(sym)
        base_sig = S.ema_crossover(df, 21, 55, long_only=True)
        feats = daily_features(sym)
        absz = align_daily_to_15m(feats["funding_absz"], df.index).fillna(0.0)
        froth = align_daily_to_15m(feats["froth"], df.index).fillna(0.0)
        sigs = overlays(base_sig, absz, froth)
        for name, sig in sigs.items():
            res = run_backtest(df, sig, sym=sym, tf=args.tf, strategy=name,
                                params={}, taker_fee_bps=args.fee,
                                slippage_bps=args.slip)
            per_overlay[name].append(res.net_ret.rename(sym))

    # Split midpoint (shared across coins).
    any_idx = per_overlay["base"][0].index
    mid = any_idx[len(any_idx) // 2]

    print("\n" + "=" * 92)
    print(f"ALT-DATA OVERLAY on ema 21/55 long-only @ {args.tf}  "
          f"(fee={args.fee}bp slip={args.slip}bp, 4-coin basket)")
    print("=" * 92)
    print(f"{'overlay':<14}{'y1 ret':>9}{'y1 shp':>8}{'y2 ret':>9}{'y2 shp':>8}"
          f"{'full ret':>10}{'maxDD':>8}{'vs base y2':>12}")
    print("-" * 92)

    base_y2 = None
    rows = {}
    for name in overlay_names:
        basket = pd.concat(per_overlay[name], axis=1, join="inner").mean(axis=1)
        y1, y2 = basket[basket.index < mid], basket[basket.index >= mid]
        m1, m2, mf = seg(y1, args.tf), seg(y2, args.tf), seg(basket, args.tf)
        rows[name] = (m1, m2, mf)
        if name == "base":
            base_y2 = m2

    for name in overlay_names:
        m1, m2, mf = rows[name]
        delta = (m2["ret"] - base_y2["ret"]) * 100
        tag = "" if name == "base" else (f"{delta:+.1f}pp"
              + ("  better" if delta > 1 else ("  worse" if delta < -1 else "  ~same")))
        print(f"{name:<14}{m1['ret']*100:>+8.1f}%{m1['sharpe']:>8.2f}"
              f"{m2['ret']*100:>+8.1f}%{m2['sharpe']:>8.2f}"
              f"{mf['ret']*100:>+9.1f}%{mf['maxdd']*100:>+7.1f}%{tag:>12}")

    print("-" * 92)
    print("An overlay earns its keep only if it lifts y2 (untouched) without "
          "wrecking y1.\nIf base wins, funding/F&G added no incremental edge here.")


if __name__ == "__main__":
    main()
