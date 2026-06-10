"""Does alt-data predict forward returns? Decile + IC analysis.

This runs BEFORE any strategy is built. For each candidate feature we
measure whether it actually separates future returns — cheaply, honestly,
and with point-in-time discipline. Most features will show ~zero IC;
that's the whole point of testing first.

Methodology:
  * Daily frequency. Funding (8h) and F&G (daily) are slow features;
    evaluating them on the 15m grid would massively overlap samples and
    inflate significance. We resample price to daily and test there.
  * Point-in-time lag. Funding for day D = mean of D's settlements,
    knowable at D's close. F&G is lagged 1 full day (decision at close of
    D uses F&G of D-1) to avoid any same-day peek.
  * Forward return = close[D+h]/close[D] - 1, feature measured at close[D].
  * Pooled across the 4-coin basket for more samples. IC = Spearman rank
    corr (feature vs forward return). Decile table shows mean fwd return
    per feature decile — monotonic or extreme-decile separation = signal.

Usage:
    python backtest/feature_ic.py --horizons 1,3,7
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"


def load_daily_price(sym: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / f"{sym}_binance_15m.csv")
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df = df.set_index("dt").sort_index()
    daily = df["close"].resample("1D").last().to_frame("close")
    daily["ret1"] = daily["close"].pct_change()
    return daily


def load_funding_daily(sym: str) -> pd.DataFrame:
    f = pd.read_csv(DATA_DIR / f"funding_{sym}.csv")
    f["dt"] = pd.to_datetime(f["ts"], unit="s", utc=True)
    f = f.set_index("dt").sort_index()
    # daily mean funding — knowable at that day's close
    return f["funding"].resample("1D").mean().to_frame("funding")


def load_fng() -> pd.DataFrame:
    g = pd.read_csv(DATA_DIR / "fear_greed.csv")
    g["dt"] = pd.to_datetime(g["ts"], unit="s", utc=True).dt.floor("D")
    g = g.set_index("dt").sort_index()
    return g[["value"]].rename(columns={"value": "fng"})


def build_features(sym: str) -> pd.DataFrame:
    px = load_daily_price(sym)
    fund = load_funding_daily(sym)
    fng = load_fng()

    df = px.join(fund, how="left").join(fng, how="left")
    df["funding"] = df["funding"].ffill()
    df["fng"] = df["fng"].ffill()

    # ── point-in-time lags ───────────────────────────────────────────
    # F&G lagged 1 day (decision at close[D] uses F&G of D-1).
    df["fng"] = df["fng"].shift(1)

    # ── engineered features (all use only past/known data) ───────────
    df["funding_z"]   = _z(df["funding"], 30)             # 30d z-score
    df["funding_sma"] = df["funding"].rolling(7).mean()   # positioning trend
    df["funding_abs"] = df["funding"].abs()               # crowding magnitude
    df["fng_z"]       = _z(df["fng"], 30)
    df["fng_chg"]     = df["fng"].diff(3)                  # 3d sentiment shift
    df["fng_level"]   = df["fng"]
    # interaction: greed + crowded longs (froth) vs fear + crowded shorts
    df["froth"]       = df["fng_z"] * df["funding_z"]
    return df


FEATURES = ["funding", "funding_z", "funding_sma", "funding_abs",
            "fng_level", "fng_z", "fng_chg", "froth"]


def _z(s: pd.Series, n: int) -> pd.Series:
    m = s.rolling(n, min_periods=n // 2).mean()
    sd = s.rolling(n, min_periods=n // 2).std(ddof=0)
    return (s - m) / sd.replace(0.0, np.nan)


def spearman_ic(x: pd.Series, y: pd.Series) -> float:
    d = pd.concat([x, y], axis=1).dropna()
    if len(d) < 30:
        return float("nan")
    return float(d.iloc[:, 0].rank().corr(d.iloc[:, 1].rank()))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--horizons", default="1,3,7")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    horizons = [int(h) for h in args.horizons.split(",")]

    # Pool features + forward returns across the basket.
    pooled = []
    for sym in syms:
        df = build_features(sym)
        for h in horizons:
            df[f"fwd{h}"] = df["close"].shift(-h) / df["close"] - 1.0
        df["sym"] = sym
        pooled.append(df)
    P = pd.concat(pooled)

    print("\n" + "=" * 78)
    print(f"INFORMATION COEFFICIENT (Spearman, pooled {len(syms)}-coin basket)")
    print("  IC ~ 0.00 = noise.  |IC| 0.03-0.06 = weak-but-real for daily crypto.")
    print("  |IC| > 0.10 with this little data usually means a look-ahead bug.")
    print("=" * 78)
    hdr = "feature".ljust(14) + "".join(f"  IC(fwd{h}d)".rjust(12) for h in horizons)
    print(hdr)
    print("-" * 78)
    ic_table = {}
    for feat in FEATURES:
        ics = [spearman_ic(P[feat], P[f"fwd{h}"]) for h in horizons]
        ic_table[feat] = ics
        print(feat.ljust(14) + "".join(f"{ic:+.4f}".rjust(12) for ic in ics))

    # ── decile detail for the strongest feature at the 7d horizon ────
    h = horizons[-1]
    strongest = max(FEATURES, key=lambda f: abs(ic_table[f][-1])
                    if ic_table[f][-1] == ic_table[f][-1] else 0)
    print("\n" + "=" * 78)
    print(f"DECILE ANALYSIS — {strongest} vs fwd{h}d return "
          f"(monotonic across deciles = real signal)")
    print("=" * 78)
    d = P[[strongest, f"fwd{h}"]].dropna()
    d["decile"] = pd.qcut(d[strongest].rank(method="first"), 10, labels=False)
    tab = d.groupby("decile")[f"fwd{h}"].agg(["mean", "count"])
    print(f"{'decile':<8}{'feat range':<22}{'mean fwd ret':>14}{'n':>8}")
    qs = d.groupby("decile")[strongest].agg(["min", "max"])
    for dec in range(10):
        rng = f"[{qs.loc[dec,'min']:+.3f},{qs.loc[dec,'max']:+.3f}]"
        mr = tab.loc[dec, "mean"]
        bar = "#" * int(abs(mr) * 500)
        sign = "+" if mr >= 0 else "-"
        print(f"D{dec:<7}{rng:<22}{mr*100:>+12.2f}%  {sign}{bar}")
    spread = tab["mean"].iloc[-1] - tab["mean"].iloc[0]
    print(f"\nTop-vs-bottom decile spread: {spread*100:+.2f}% over {h}d "
          f"({'CONTRARIAN' if spread<0 else 'MOMENTUM'} in this feature)")
    print("\nNext: features with real IC become filters/sizing on the trend base.")


if __name__ == "__main__":
    main()
