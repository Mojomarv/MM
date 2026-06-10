"""Fetch free alternative-data feeds with deep history.

Two feeds, both free and point-in-time-able:
  * Binance perp FUNDING RATE — settles every 8h, reflects leverage/
    positioning (not spot price). Back to ~2020 for majors.
  * Crypto FEAR & GREED index (alternative.me) — daily, back to 2018.

Saved to data/funding_{SYM}.csv (ts,funding,mark) and data/fear_greed.csv
(ts,value,classification). Timestamps are unix-seconds at the moment the
datum SETTLED/PUBLISHED — the IC harness applies publish-lag on top so we
never peek at a value before it was knowable.

Usage:
    python backtest/altdata_fetch.py --symbols BTC,ETH,SOL,BNB --days 730
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

FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
FNG_URL = "https://api.alternative.me/fng/?limit=0&format=json"


def _get(url: str, timeout: int = 20):
    for attempt in range(4):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return json.loads(r.read())
        except Exception as exc:  # noqa: BLE001
            if attempt == 3:
                raise
            print(f"  retry ({type(exc).__name__})…")
            time.sleep(1.5)


def fetch_funding(sym: str, days: int) -> list[tuple[int, float, float]]:
    """Paginate funding history; return [(ts_sec, funding_rate, mark), ...]."""
    pair = f"{sym}USDT"
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86_400_000
    rows: dict[int, tuple] = {}
    cur = start_ms
    EIGHT_H = 8 * 3_600_000
    while cur < now_ms:
        url = f"{FUNDING_URL}?symbol={pair}&startTime={cur}&limit=1000"
        chunk = _get(url)
        if not chunk:
            break
        for c in chunk:
            ts = int(c["fundingTime"]) // 1000
            rows[ts] = (ts, float(c["fundingRate"]), float(c.get("markPrice") or 0))
        last = chunk[-1]["fundingTime"]
        if last <= cur:
            break
        cur = last + EIGHT_H
        time.sleep(0.2)
    return [rows[k] for k in sorted(rows)]


def fetch_fng() -> list[tuple[int, int, str]]:
    data = _get(FNG_URL)["data"]
    out = [(int(d["timestamp"]), int(d["value"]), d["value_classification"])
           for d in data]
    return sorted(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="BTC,ETH,SOL,BNB")
    ap.add_argument("--days", type=int, default=730)
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    for sym in syms:
        print(f"Funding {sym}USDT -- {args.days}d…")
        rows = fetch_funding(sym, args.days)
        path = DATA_DIR / f"funding_{sym}.csv"
        with path.open("w", newline="") as f:
            w = csv.writer(f); w.writerow(["ts", "funding", "mark"]); w.writerows(rows)
        if rows:
            span = (rows[-1][0] - rows[0][0]) / 86_400
            print(f"  -> {len(rows)} settlements ({span:.0f}d) -> {path.name}")

    print("Fear & Greed (full history)…")
    fng = fetch_fng()
    path = DATA_DIR / "fear_greed.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["ts", "value", "classification"]); w.writerows(fng)
    print(f"  -> {len(fng)} daily points "
          f"({time.strftime('%Y-%m-%d', time.gmtime(fng[0][0]))} "
          f"to {time.strftime('%Y-%m-%d', time.gmtime(fng[-1][0]))}) -> {path.name}")


if __name__ == "__main__":
    main()
