"""Vectorized backtest engine for directional perps strategies.

Design principles (the discipline the MM bot lacked):
  * No look-ahead. A signal computed from data through close[t-1] sets the
    position HELD during bar t. We use position[t] = signal[t].shift(1).
  * Honest costs. Every change in target position pays
        |Δposition| × (taker_fee_bps + slippage_bps) / 10_000
    A full long→short flip therefore pays the cost twice (|1−(−1)|=2).
  * 24/7 annualization. Crypto trades every day, so bars_per_year uses 365d.

Position convention: target position ∈ {-1, 0, +1} (or fractional). +1 =
fully long one unit of notional, −1 = fully short, 0 = flat. 1× leverage,
returns are on notional.

A strategy is just a function df -> pd.Series of target positions aligned
to df.index. The engine handles execution, costs, equity, and metrics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"

# Timeframe label -> pandas resample rule and minutes-per-bar.
TIMEFRAMES: dict[str, tuple[str, int]] = {
    "1m":  ("1min",  1),
    "5m":  ("5min",  5),
    "15m": ("15min", 15),
    "1h":  ("1h",    60),
    "4h":  ("4h",    240),
}

MINUTES_PER_YEAR = 365 * 24 * 60


def load_candles(sym: str, tf: str = "1m") -> pd.DataFrame:
    """Load cached 1m candles and resample to the requested timeframe.
    Returns a DataFrame indexed by UTC datetime with open/high/low/close/volume."""
    path = DATA_DIR / f"{sym}_1m.csv"
    if not path.exists():
        raise FileNotFoundError(f"no cached data for {sym} at {path}; run fetch_data.py")
    df = pd.read_csv(path)
    df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)
    df = df.set_index("dt").sort_index()
    if tf == "1m":
        return df[["open", "high", "low", "close", "volume"]]
    rule, _ = TIMEFRAMES[tf]
    agg = df.resample(rule).agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"),
    ).dropna(subset=["open", "close"])
    return agg


@dataclass
class BacktestResult:
    sym: str
    tf: str
    strategy: str
    params: dict
    equity: pd.Series                       # cumulative equity, starts at 1.0
    net_ret: pd.Series                      # per-bar net return after costs
    position: pd.Series                     # position held per bar
    metrics: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        m = self.metrics
        return (f"<{self.sym}/{self.tf} {self.strategy}{self.params} "
                f"ret={m.get('total_return', 0):.1%} "
                f"sharpe={m.get('sharpe', 0):.2f} "
                f"maxDD={m.get('max_drawdown', 0):.1%} "
                f"trades={m.get('n_trades', 0)}>")


def run_backtest(df: pd.DataFrame, signal: pd.Series, *, sym: str, tf: str,
                  strategy: str, params: dict,
                  taker_fee_bps: float = 5.0,
                  slippage_bps: float = 2.0) -> BacktestResult:
    """Run a vectorized backtest. `signal` is the target position computed
    from each bar's CLOSE; it is shifted forward one bar before being
    applied, so no look-ahead leaks in."""
    close = df["close"].astype(float)
    bar_ret = close.pct_change().fillna(0.0)

    # Position held during bar t = signal decided at close[t-1].
    pos = signal.reindex(df.index).ffill().fillna(0.0).shift(1).fillna(0.0)

    cost_rate = (taker_fee_bps + slippage_bps) / 10_000.0
    turnover = pos.diff().abs().fillna(pos.abs())   # first bar: entry cost
    cost = turnover * cost_rate

    strat_ret = pos * bar_ret
    net_ret = strat_ret - cost
    equity = (1.0 + net_ret).cumprod()

    metrics = compute_metrics(net_ret, pos, turnover, tf)
    return BacktestResult(sym, tf, strategy, params, equity, net_ret, pos, metrics)


def compute_metrics(net_ret: pd.Series, pos: pd.Series,
                     turnover: pd.Series, tf: str) -> dict:
    _, minutes_per_bar = TIMEFRAMES[tf]
    bars_per_year = MINUTES_PER_YEAR / minutes_per_bar

    n = len(net_ret)
    total_return = float((1.0 + net_ret).prod() - 1.0)
    years = n / bars_per_year
    cagr = (1.0 + total_return) ** (1.0 / years) - 1.0 if years > 0 and total_return > -1 else float("nan")

    mu = net_ret.mean()
    sd = net_ret.std(ddof=0)
    sharpe = float(mu / sd * np.sqrt(bars_per_year)) if sd > 0 else 0.0

    # Sortino (downside deviation only)
    downside = net_ret[net_ret < 0]
    dd_sd = downside.std(ddof=0)
    sortino = float(mu / dd_sd * np.sqrt(bars_per_year)) if dd_sd > 0 else 0.0

    equity = (1.0 + net_ret).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1.0
    max_drawdown = float(drawdown.min())

    # Per-"trade" stats: a trade = a contiguous nonzero-position run.
    # Approximate via turnover events (each entry/flip is a trade).
    n_trades = int((turnover > 1e-9).sum())
    # Win rate on bar returns while in a position (proxy).
    in_pos = pos.abs() > 1e-9
    pos_rets = net_ret[in_pos]
    win_rate = float((pos_rets > 0).mean()) if len(pos_rets) else 0.0

    gains = net_ret[net_ret > 0].sum()
    losses = -net_ret[net_ret < 0].sum()
    profit_factor = float(gains / losses) if losses > 0 else float("inf")

    exposure = float(in_pos.mean())

    return {
        "total_return":  total_return,
        "cagr":          cagr,
        "sharpe":        sharpe,
        "sortino":       sortino,
        "max_drawdown":  max_drawdown,
        "n_trades":      n_trades,
        "win_rate":      win_rate,
        "profit_factor": profit_factor,
        "exposure":      exposure,
        "n_bars":        n,
    }
