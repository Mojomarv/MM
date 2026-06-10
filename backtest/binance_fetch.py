"""Fetch deep historical klines from Binance for multi-regime backtesting.

Rise only has ~60 days of history (one regime). To test whether a
strategy survives bull / bear / chop, we need years of data. Binance
spot klines are public (no auth) and go back to ~2017-2020 for majors.

Saves to data/{SYM}_binance_{tf}.csv with columns ts,open,high,low,close,
volume — the same schema the engine uses (but already at the target tf,
so the regime loader reads them directly without resampling).

Usage:
    python backtest/binance_fetch.py --symbols BTC,ETH,SOL,BNB --tf 1h --days 730
"""
from __future__ import annotations

import argparse
import csv
import json
import time
import urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

BINANCE = "https://api.binance.com/api/v3/klines"
TF_MS = {
    "5m": 300_000, "15m": 900_000,
    "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000,
}


def fetch_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> list:
    """Paginate Binance klines (max 1000/request) over [start, end]."""
    out: list = []
    step = TF_MS[interval] * 1000
    cur = start_ms
    while cur < end_ms:
        url = (f"{BINANCE}?symbol={symbol}&interval={interval}"
               f"&startTime={cur}&endTime={min(cur + step, end_ms)}&limit=1000")
        for attempt in range(4):
            try:
                with urllib.request.urlopen(url, timeout=20) as r:
                    chunk = json.loads(r.read())
                break
            except Exception as exc:  # noqa: BLE001
                if attempt == 3:
                    raise
                print(f"  retry {symbol} ({type(exc).__name__})…")
                time.sleep(1.5)
        if not chunk:
            cur += step
            continue
        out.extend(chunk)
        cur = chunk[-1][0] + TF_MS[interval]   # next bar after last
        time.sleep(0.25)                       # respect rate limits
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--tf", default="15m", choices=list(TF_MS))
    ap.add_argument("--days", type=int, default=730)
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - args.days * 86_400_000

    for sym in syms:
        pair = f"{sym}USDT"
        print(f"Fetching {pair} {args.tf} -- {args.days}d…")
        kl = fetch_klines(pair, args.tf, start_ms, now_ms)
        # de-dup by openTime, sort
        rows = {}
        for k in kl:
            ts = int(k[0]) // 1000
            rows[ts] = (ts, float(k[1]), float(k[2]), float(k[3]),
                        float(k[4]), float(k[5]))
        ordered = [rows[t] for t in sorted(rows)]
        path = DATA_DIR / f"{sym}_binance_{args.tf}.csv"
        with path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["ts", "open", "high", "low", "close", "volume"])
            w.writerows(ordered)
        if ordered:
            span_d = (ordered[-1][0] - ordered[0][0]) / 86_400
            print(f"  -> {len(ordered)} bars ({span_d:.0f}d, "
                  f"{time.strftime('%Y-%m-%d', time.gmtime(ordered[0][0]))} "
                  f"to {time.strftime('%Y-%m-%d', time.gmtime(ordered[-1][0]))}) "
                  f"-> {path.name}")
        else:
            print(f"  -> no data for {pair}")


if __name__ == "__main__":
    main()
