"""Classic systematic strategies.

Each strategy is a function (df) -> pd.Series of target positions in
{-1, 0, +1}, computed from each bar's CLOSE (the engine handles the
one-bar execution lag, so these may look at the current close freely).

Three families:
  TREND        — ride momentum: MA crossover, Donchian breakout, MACD
  MEAN-REVERT  — fade extremes: RSI, Bollinger z-score, %-from-MA
  BREAKOUT     — trade range breaks with a volatility confirmation filter

Indicator helpers are kept inline and dependency-free (pure pandas) so
the whole lab is portable.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


# ── indicator helpers ─────────────────────────────────────────────────
def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()

def _rsi(s: pd.Series, n: int) -> pd.Series:
    delta = s.diff()
    up = delta.clip(lower=0.0)
    dn = (-delta).clip(lower=0.0)
    roll_up = up.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    roll_dn = dn.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()
    rs = roll_up / roll_dn.replace(0.0, np.nan)
    return 100.0 - 100.0 / (1.0 + rs)

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()

def _hlc3(df: pd.DataFrame) -> pd.Series:
    return (df["high"] + df["low"] + df["close"]) / 3.0

def _wavetrend(df: pd.DataFrame, n1: int = 10, n2: int = 21
               ) -> tuple[pd.Series, pd.Series]:
    """LazyBear WaveTrend oscillator. Returns (wt1, wt2).
      ap   = HLC3
      esa  = EMA(ap, n1)
      d    = EMA(|ap-esa|, n1)
      ci   = (ap-esa) / (0.015*d)
      wt1  = EMA(ci, n2);  wt2 = SMA(wt1, 4)
    Overbought ~ +60, oversold ~ -60."""
    ap = _hlc3(df)
    esa = _ema(ap, n1)
    d = _ema((ap - esa).abs(), n1)
    ci = (ap - esa) / (0.015 * d.replace(0.0, np.nan))
    wt1 = _ema(ci, n2)
    wt2 = wt1.rolling(4, min_periods=4).mean()
    return wt1, wt2

def _adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """Average Directional Index — trend-strength (not direction). High
    ADX (>25) = strong trend, low (<20) = chop/range."""
    h, l, c = df["high"], df["low"], df["close"]
    up = h.diff()
    dn = -l.diff()
    plus_dm = ((up > dn) & (up > 0)) * up
    minus_dm = ((dn > up) & (dn > 0)) * dn
    atr = _atr(df, n)
    plus_di = 100 * plus_dm.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean() / atr
    minus_di = 100 * minus_dm.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean() / atr
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, np.nan)
    return dx.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean()

def _supertrend(df: pd.DataFrame, n: int = 10, mult: float = 3.0) -> pd.Series:
    """Supertrend direction series in {-1, +1}. ATR-banded trend follower."""
    atr = _atr(df, n)
    hl2 = (df["high"] + df["low"]) / 2.0
    upper = hl2 + mult * atr
    lower = hl2 - mult * atr
    close = df["close"].to_numpy()
    up = upper.to_numpy(); lo = lower.to_numpy()
    n_bars = len(close)
    dir_ = np.ones(n_bars)                      # +1 = uptrend
    fub = up.copy(); flb = lo.copy()
    for i in range(1, n_bars):
        # final upper/lower band carry-forward
        fub[i] = up[i] if (up[i] < fub[i-1] or close[i-1] > fub[i-1]) else fub[i-1]
        flb[i] = lo[i] if (lo[i] > flb[i-1] or close[i-1] < flb[i-1]) else flb[i-1]
        if close[i] > fub[i-1]:
            dir_[i] = 1
        elif close[i] < flb[i-1]:
            dir_[i] = -1
        else:
            dir_[i] = dir_[i-1]
    return pd.Series(dir_, index=df.index)

def _stoch(df: pd.DataFrame, n: int = 14, smooth: int = 3) -> pd.Series:
    low_n = df["low"].rolling(n, min_periods=n).min()
    high_n = df["high"].rolling(n, min_periods=n).max()
    k = 100 * (df["close"] - low_n) / (high_n - low_n).replace(0.0, np.nan)
    return k.rolling(smooth, min_periods=smooth).mean()

def _cci(df: pd.DataFrame, n: int = 20) -> pd.Series:
    tp = _hlc3(df)
    ma = tp.rolling(n, min_periods=n).mean()
    md = (tp - ma).abs().rolling(n, min_periods=n).mean()
    return (tp - ma) / (0.015 * md.replace(0.0, np.nan))


# ── TREND ─────────────────────────────────────────────────────────────
def sma_crossover(df: pd.DataFrame, fast: int = 20, slow: int = 50,
                   long_only: bool = False) -> pd.Series:
    """Long when fast SMA > slow SMA, short (or flat) otherwise."""
    c = df["close"]
    f, s = _sma(c, fast), _sma(c, slow)
    pos = pd.Series(0.0, index=df.index)
    pos[f > s] = 1.0
    pos[f < s] = 0.0 if long_only else -1.0
    return pos

def ema_crossover(df: pd.DataFrame, fast: int = 12, slow: int = 26,
                   long_only: bool = False) -> pd.Series:
    c = df["close"]
    f, s = _ema(c, fast), _ema(c, slow)
    pos = pd.Series(0.0, index=df.index)
    pos[f > s] = 1.0
    pos[f < s] = 0.0 if long_only else -1.0
    return pos

def donchian_breakout(df: pd.DataFrame, n: int = 20,
                       long_only: bool = False) -> pd.Series:
    """Long on close above the prior n-bar high, short below prior n-bar low.
    Classic turtle-style breakout; holds until the opposite break."""
    hi = df["high"].rolling(n, min_periods=n).max().shift(1)
    lo = df["low"].rolling(n, min_periods=n).min().shift(1)
    c = df["close"]
    raw = pd.Series(np.nan, index=df.index)
    raw[c > hi] = 1.0
    raw[c < lo] = 0.0 if long_only else -1.0
    return raw.ffill().fillna(0.0)

def macd_trend(df: pd.DataFrame, fast: int = 12, slow: int = 26,
                signal: int = 9, long_only: bool = False) -> pd.Series:
    c = df["close"]
    macd = _ema(c, fast) - _ema(c, slow)
    sig = macd.ewm(span=signal, adjust=False, min_periods=signal).mean()
    pos = pd.Series(0.0, index=df.index)
    pos[macd > sig] = 1.0
    pos[macd < sig] = 0.0 if long_only else -1.0
    return pos


# ── MEAN-REVERSION ────────────────────────────────────────────────────
def rsi_reversion(df: pd.DataFrame, n: int = 14, lo: float = 30.0,
                   hi: float = 70.0, long_only: bool = False) -> pd.Series:
    """Buy when RSI < lo (oversold), sell when RSI > hi (overbought).
    Exit to flat when RSI crosses back through the 50 midline."""
    r = _rsi(df["close"], n)
    pos = pd.Series(np.nan, index=df.index)
    pos[r < lo] = 1.0
    pos[r > hi] = 0.0 if long_only else -1.0
    mid_up = (r > 50) & (r.shift(1) <= 50)     # recovered from oversold
    mid_dn = (r < 50) & (r.shift(1) >= 50)     # rolled over from overbought
    pos[mid_up | mid_dn] = 0.0
    return pos.ffill().fillna(0.0)

def bollinger_reversion(df: pd.DataFrame, n: int = 20, k: float = 2.0,
                         long_only: bool = False) -> pd.Series:
    """Fade Bollinger band touches: long below lower band, short above
    upper band, exit to flat at the mid band."""
    c = df["close"]
    ma = _sma(c, n)
    sd = c.rolling(n, min_periods=n).std(ddof=0)
    upper, lower = ma + k * sd, ma - k * sd
    pos = pd.Series(np.nan, index=df.index)
    pos[c < lower] = 1.0
    pos[c > upper] = 0.0 if long_only else -1.0
    # exit at mid-band cross
    crossed_mid = (c >= ma) & (c.shift(1) < ma) | (c <= ma) & (c.shift(1) > ma)
    pos[crossed_mid] = 0.0
    return pos.ffill().fillna(0.0)

def zscore_reversion(df: pd.DataFrame, n: int = 30, k: float = 1.5,
                      long_only: bool = False) -> pd.Series:
    """Z-score of price vs rolling mean; long when z < -k, short when z > +k,
    flat when |z| < 0.3 (snap back to mean)."""
    c = df["close"]
    ma = _sma(c, n)
    sd = c.rolling(n, min_periods=n).std(ddof=0)
    z = (c - ma) / sd
    pos = pd.Series(np.nan, index=df.index)
    pos[z < -k] = 1.0
    pos[z > k] = 0.0 if long_only else -1.0
    pos[z.abs() < 0.3] = 0.0
    return pos.ffill().fillna(0.0)


# ── BREAKOUT (range break with ATR confirmation) ──────────────────────
def atr_breakout(df: pd.DataFrame, n: int = 20, atr_n: int = 14,
                  mult: float = 0.5, long_only: bool = False) -> pd.Series:
    """Breakout above prior n-bar high + mult×ATR (and symmetric short).
    The ATR buffer filters false breaks in chop."""
    hi = df["high"].rolling(n, min_periods=n).max().shift(1)
    lo = df["low"].rolling(n, min_periods=n).min().shift(1)
    atr = _atr(df, atr_n)
    c = df["close"]
    raw = pd.Series(np.nan, index=df.index)
    raw[c > hi + mult * atr] = 1.0
    raw[c < lo - mult * atr] = 0.0 if long_only else -1.0
    return raw.ffill().fillna(0.0)


# ── WAVETREND ─────────────────────────────────────────────────────────
def wavetrend_cross(df: pd.DataFrame, n1: int = 10, n2: int = 21,
                     long_only: bool = False) -> pd.Series:
    """Momentum mode: long while wt1 > wt2, short while wt1 < wt2."""
    wt1, wt2 = _wavetrend(df, n1, n2)
    pos = pd.Series(0.0, index=df.index)
    pos[wt1 > wt2] = 1.0
    pos[wt1 < wt2] = 0.0 if long_only else -1.0
    return pos

def wavetrend_revert(df: pd.DataFrame, n1: int = 10, n2: int = 21,
                      ob: float = 60.0, os_: float = -60.0,
                      long_only: bool = False) -> pd.Series:
    """Reversion mode: buy on bullish cross from oversold (wt1 crosses up
    over wt2 while < os_), sell on bearish cross from overbought. Exit to
    flat on the opposite cross."""
    wt1, wt2 = _wavetrend(df, n1, n2)
    cross_up = (wt1 > wt2) & (wt1.shift(1) <= wt2.shift(1))
    cross_dn = (wt1 < wt2) & (wt1.shift(1) >= wt2.shift(1))
    pos = pd.Series(np.nan, index=df.index)
    pos[cross_up & (wt2 < os_)] = 1.0
    if long_only:
        pos[cross_dn] = 0.0
    else:
        pos[cross_dn & (wt2 > ob)] = -1.0
        pos[cross_up & (wt2 >= os_)] = 0.0     # exit short on any bull cross
    return pos.ffill().fillna(0.0)


# ── SUPERTREND / STOCH / CCI ──────────────────────────────────────────
def supertrend_follow(df: pd.DataFrame, n: int = 10, mult: float = 3.0,
                       long_only: bool = False) -> pd.Series:
    d = _supertrend(df, n, mult)
    if long_only:
        return d.clip(lower=0.0)
    return d

def stoch_revert(df: pd.DataFrame, n: int = 14, lo: float = 20.0,
                  hi: float = 80.0, long_only: bool = False) -> pd.Series:
    k = _stoch(df, n)
    pos = pd.Series(np.nan, index=df.index)
    pos[k < lo] = 1.0
    pos[k > hi] = 0.0 if long_only else -1.0
    mid_up = (k > 50) & (k.shift(1) <= 50)
    mid_dn = (k < 50) & (k.shift(1) >= 50)
    pos[mid_up | mid_dn] = 0.0
    return pos.ffill().fillna(0.0)

def cci_revert(df: pd.DataFrame, n: int = 20, thresh: float = 100.0,
                long_only: bool = False) -> pd.Series:
    cci = _cci(df, n)
    pos = pd.Series(np.nan, index=df.index)
    pos[cci < -thresh] = 1.0
    pos[cci > thresh] = 0.0 if long_only else -1.0
    pos[cci.abs() < 20] = 0.0
    return pos.ffill().fillna(0.0)


# ── COMBINATIONS ──────────────────────────────────────────────────────
# These are the highest-value additions. Two patterns that consistently
# matter in real systematic trading:
#
#   1. TREND-FILTERED entries — only take a base signal in the direction
#      of the higher-timeframe trend. This directly fixes why naive
#      mean-reversion failed: it was fading strong trends. With a filter,
#      a reversion long only fires when the slow trend is already up.
#
#   2. CONFLUENCE voting — require N independent signals to agree before
#      taking a position. Fewer trades, higher conviction, less noise.

def _trend_dir(df: pd.DataFrame, n: int = 200) -> pd.Series:
    """+1 when close > slow SMA (uptrend), -1 below. The regime gate."""
    ma = _sma(df["close"], n)
    d = pd.Series(0.0, index=df.index)
    d[df["close"] > ma] = 1.0
    d[df["close"] < ma] = -1.0
    return d

def trend_filtered_revert(df: pd.DataFrame, rsi_n: int = 14, lo: float = 30.0,
                           hi: float = 70.0, trend_n: int = 200,
                           long_only: bool = False) -> pd.Series:
    """RSI reversion, but only LONG when the slow trend is up and only
    SHORT when it's down. Classic 'buy the dip in an uptrend' rule."""
    base = rsi_reversion(df, n=rsi_n, lo=lo, hi=hi, long_only=long_only)
    trend = _trend_dir(df, trend_n)
    out = base.copy()
    out[(base > 0) & (trend < 0)] = 0.0        # no longs in downtrend
    out[(base < 0) & (trend > 0)] = 0.0        # no shorts in uptrend
    return out

def trend_filtered_wt(df: pd.DataFrame, n1: int = 10, n2: int = 21,
                       os_: float = -60.0, ob: float = 60.0,
                       trend_n: int = 200, long_only: bool = False) -> pd.Series:
    """WaveTrend reversion gated by the slow trend direction."""
    base = wavetrend_revert(df, n1=n1, n2=n2, ob=ob, os_=os_, long_only=long_only)
    trend = _trend_dir(df, trend_n)
    out = base.copy()
    out[(base > 0) & (trend < 0)] = 0.0
    out[(base < 0) & (trend > 0)] = 0.0
    return out

def adx_regime(df: pd.DataFrame, adx_n: int = 14, adx_min: float = 25.0,
                fast: int = 12, slow: int = 26, long_only: bool = False) -> pd.Series:
    """Trade EMA-crossover trend ONLY when ADX says the market is actually
    trending (ADX > adx_min); go flat in chop. A regime switch."""
    base = ema_crossover(df, fast=fast, slow=slow, long_only=long_only)
    adx = _adx(df, adx_n)
    out = base.copy()
    out[adx < adx_min] = 0.0
    return out

def confluence_vote(df: pd.DataFrame, threshold: int = 2,
                     trend_n: int = 200, long_only: bool = False) -> pd.Series:
    """Sum three independent trend/breakout signals; take a position only
    when at least `threshold` agree. Components:
      - EMA 12/26 crossover
      - Donchian-20 breakout
      - Supertrend(10,3) direction
    """
    s1 = ema_crossover(df, 12, 26)
    s2 = donchian_breakout(df, 20)
    s3 = supertrend_follow(df, 10, 3.0)
    votes = s1.add(s2, fill_value=0).add(s3, fill_value=0)
    pos = pd.Series(0.0, index=df.index)
    pos[votes >= threshold] = 1.0
    pos[votes <= -threshold] = 0.0 if long_only else -1.0
    return pos


# ── strategy registry: name -> (fn, family, default param grid) ───────
# Each grid entry is a dict of kwargs; the runner expands the product.
STRATEGIES: dict[str, dict] = {
    "sma_cross": {
        "fn": sma_crossover, "family": "trend",
        "grid": [{"fast": f, "slow": s}
                 for f, s in [(10, 30), (20, 50), (20, 100), (50, 200)]],
    },
    "ema_cross": {
        "fn": ema_crossover, "family": "trend",
        "grid": [{"fast": f, "slow": s}
                 for f, s in [(9, 21), (12, 26), (21, 55)]],
    },
    "donchian": {
        "fn": donchian_breakout, "family": "trend",
        "grid": [{"n": n} for n in [10, 20, 40, 55]],
    },
    "macd": {
        "fn": macd_trend, "family": "trend",
        "grid": [{"fast": 12, "slow": 26, "signal": 9}],
    },
    "rsi_revert": {
        "fn": rsi_reversion, "family": "mean_revert",
        "grid": [{"n": n, "lo": lo, "hi": 100 - lo}
                 for n in [7, 14] for lo in [20, 30]],
    },
    "bollinger_revert": {
        "fn": bollinger_reversion, "family": "mean_revert",
        "grid": [{"n": n, "k": k} for n in [20, 30] for k in [1.5, 2.0, 2.5]],
    },
    "zscore_revert": {
        "fn": zscore_reversion, "family": "mean_revert",
        "grid": [{"n": n, "k": k} for n in [20, 30, 50] for k in [1.0, 1.5, 2.0]],
    },
    "atr_breakout": {
        "fn": atr_breakout, "family": "breakout",
        "grid": [{"n": n, "mult": m} for n in [20, 40] for m in [0.25, 0.5, 1.0]],
    },
    # ── new single strategies ────────────────────────────────────────
    "wavetrend_cross": {
        "fn": wavetrend_cross, "family": "trend",
        "grid": [{"n1": a, "n2": b} for a, b in [(10, 21), (9, 12), (10, 28)]],
    },
    "wavetrend_revert": {
        "fn": wavetrend_revert, "family": "mean_revert",
        "grid": [{"n1": 10, "n2": 21, "os_": o, "ob": -o}
                 for o in [-53.0, -60.0, -70.0]],
    },
    "supertrend": {
        "fn": supertrend_follow, "family": "trend",
        "grid": [{"n": n, "mult": m} for n in [10, 14] for m in [2.0, 3.0]],
    },
    "stoch_revert": {
        "fn": stoch_revert, "family": "mean_revert",
        "grid": [{"n": n, "lo": lo, "hi": 100 - lo}
                 for n in [14, 21] for lo in [20, 25]],
    },
    "cci_revert": {
        "fn": cci_revert, "family": "mean_revert",
        "grid": [{"n": n, "thresh": t} for n in [20, 30] for t in [100, 150]],
    },
    # ── combinations ─────────────────────────────────────────────────
    "tf_rsi_revert": {
        "fn": trend_filtered_revert, "family": "combo",
        "grid": [{"rsi_n": 14, "lo": lo, "hi": 100 - lo, "trend_n": tn}
                 for lo in [25, 30] for tn in [100, 200]],
    },
    "tf_wavetrend": {
        "fn": trend_filtered_wt, "family": "combo",
        "grid": [{"n1": 10, "n2": 21, "os_": -60.0, "ob": 60.0, "trend_n": tn}
                 for tn in [100, 200]],
    },
    "adx_regime": {
        "fn": adx_regime, "family": "combo",
        "grid": [{"adx_n": 14, "adx_min": a, "fast": 12, "slow": 26}
                 for a in [20, 25, 30]],
    },
    "confluence": {
        "fn": confluence_vote, "family": "combo",
        "grid": [{"threshold": t} for t in [2, 3]],
    },
}
