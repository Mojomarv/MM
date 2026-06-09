"""Fetch historical 1-minute OHLCV candles from Rise and cache to CSV.

Rise's /v1/trading-view-data endpoint:
  - param `interval` is the bar size in NANOSECONDS (1min = 60_000_000_000)
  - params `from` / `to` are unix-SECONDS with 9 trailing zeros (nanoseconds)
  - response: {"data": {"data": [{product_id, interval, time(ns string),
    open, high, low, close, volume}, ...]}}
  - per-request span cap ~ 30 days; we paginate in 25-day windows.

We pull the finest grain (1m) once per market and resample to coarser
timeframes locally in the backtest engine, so the cached CSV is the single
source of truth.

Usage (from repo root, with the risex1 account env for credentials):
    python backtest/fetch_data.py --days 60 --markets BTC,ETH,SOL,BNB,HYPE
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "bots" / "mm" / "risex_mm"))
from rise_client import RiseSigner, RiseRest, EIP712Domain  # noqa: E402

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

ONE_MIN_NS = 60_000_000_000
WINDOW_DAYS = 25          # under the ~30d per-request cap
SEC_PER_DAY = 86_400

# market symbol -> market_id (from /v1/markets; majors are stable ids)
DEFAULT_MARKET_IDS = {
    "BTC": 1, "ETH": 2, "BNB": 3, "SOL": 4, "HYPE": 5,
    "XRP": 6, "TAO": 7, "ZEC": 8, "ONDO": 9, "NEAR": 10,
    "DOGE": 14,
}


async def _make_rest() -> RiseRest:
    load_dotenv(REPO / "accounts" / "risex1" / ".env")
    signer = RiseSigner(
        os.environ["RISE_ACCOUNT_ADDRESS"],
        os.environ["RISE_SIGNER_ADDRESS"],
        os.environ["RISE_SIGNER_PRIVATE_KEY"],
        EIP712Domain("x", "1", 0, "0x0000000000000000000000000000000000000000"),
    )
    return RiseRest(os.environ["RISE_REST_BASE_URL"], signer)


async def fetch_window(rest: RiseRest, market_id: int,
                        from_s: int, to_s: int) -> list[dict]:
    qs = (f"market_id={market_id}&interval={ONE_MIN_NS}"
          f"&from={from_s}000000000&to={to_s}000000000")
    sess = await rest._sess()
    async with sess.get(f"{rest.base_url}/v1/trading-view-data?{qs}") as r:
        body = json.loads(await r.text())
    return ((body.get("data") or {}).get("data")) or []


async def fetch_market(rest: RiseRest, sym: str, market_id: int,
                        days: int) -> list[tuple[int, float, float, float, float, float]]:
    """Return sorted, de-duplicated [(ts_sec, o, h, l, c, v), ...]."""
    now_s = int(time.time())
    start_s = now_s - days * SEC_PER_DAY
    rows: dict[int, tuple] = {}
    win_s = start_s
    while win_s < now_s:
        win_e = min(win_s + WINDOW_DAYS * SEC_PER_DAY, now_s)
        candles = await fetch_window(rest, market_id, win_s, win_e)
        for c in candles:
            ts = int(c["time"]) // 1_000_000_000      # ns -> sec
            rows[ts] = (
                ts,
                float(c["open"]), float(c["high"]),
                float(c["low"]),  float(c["close"]),
                float(c.get("volume") or 0.0),
            )
        print(f"  {sym}: window {time.strftime('%m-%d', time.gmtime(win_s))}"
              f"->{time.strftime('%m-%d', time.gmtime(win_e))} "
              f"+{len(candles)} (total {len(rows)})")
        win_s = win_e
        await asyncio.sleep(0.15)   # be polite to the API
    return [rows[k] for k in sorted(rows)]


def write_csv(sym: str, rows: list[tuple]) -> Path:
    path = DATA_DIR / f"{sym}_1m.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "open", "high", "low", "close", "volume"])
        w.writerows(rows)
    return path


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--markets", type=str, default="BTC,ETH,SOL,BNB,HYPE")
    args = ap.parse_args()

    syms = [s.strip().upper() for s in args.markets.split(",") if s.strip()]
    rest = await _make_rest()
    try:
        for sym in syms:
            mid = DEFAULT_MARKET_IDS.get(sym)
            if mid is None:
                print(f"! unknown market {sym}, skipping")
                continue
            print(f"Fetching {sym} (id={mid}) -- {args.days}d of 1m bars...")
            rows = await fetch_market(rest, sym, mid, args.days)
            path = write_csv(sym, rows)
            if rows:
                span_h = (rows[-1][0] - rows[0][0]) / 3600
                print(f"  -> wrote {len(rows)} rows ({span_h:.0f}h) to {path}\n")
            else:
                print(f"  -> no data for {sym}\n")
    finally:
        await rest.close()


if __name__ == "__main__":
    asyncio.run(main())
