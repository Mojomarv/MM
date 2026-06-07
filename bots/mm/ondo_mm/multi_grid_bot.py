"""
Multi-pair grid MM on OndoPerps — port of the Lighter / Rise bot.

For each user-selected market the bot posts a symmetric POST_ONLY ladder
around the live mid (same math as the Lighter/Rise ports):

  bids at mid * (1 - k * GRID_STEP_BP / 1e4) for k = 1..LEVELS_PER_SIDE
  asks at mid * (1 + k * GRID_STEP_BP / 1e4) for k = 1..LEVELS_PER_SIDE

Cap-aware, exit-only, per-pair stop-loss, b/s imbalance halt, adaptive
throttle, orphan detection (WS + REST sweep) — all carried over.

Differences from the Lighter/Rise ports
---------------------------------------
* Auth is HMAC-SHA256 over 3 headers (see ondo_client.OndoSigner).
  No EIP-712, no JWT refresh.
* Markets are identified by ticker strings ("AAPL-USD.P"), not numeric IDs.
* Prices and sizes are decimal strings snapped to per-market
  `baseIncrement` / `quoteIncrement`.
* Order status enum: open / fullyfilled / canceled (no "pending" state).
* Cancel uses path param; cancel-all is a batch DELETE over comma-separated
  IDs (we group all known orderIds and send one batch per pass).
* Realized PnL comes from the `fillsPerps` WS channel (per-fill `pnl`).
* **Non-zero fees**: maker 0.015% (1.5 bp), taker 0.035% (3.5 bp). Grid
  step needs to cover at least ~3 bp per round-trip just to break even on
  fees; calibrate accordingly.

Env (same risk knobs as the Lighter/Rise bots):
  PAIRS_INCLUDE            CSV of ticker strings (e.g. "AAPL-USD.P,NVDA-USD.P")
  ORDER_SIZE_USD           $50      target USD notional per limit order
  LEVELS_PER_SIDE          3
  GRID_STEP_BP             8.0
  MAX_NOTIONAL_PER_PAIR_USD  500
  MAX_TOTAL_NOTIONAL_USD    2000
  REQUOTE_DRIFT_BP         4.0
  REQUOTE_MAX_AGE_SEC      300
  POLL_INTERVAL_SEC        1.0
  SESSION_DD_LIMIT_USD     -10.0
  PAIR_STOP_LOSS_USD       -3.0
  FILL_IMBALANCE_RATIO     2.5
  FILL_IMBALANCE_MIN_FILLS 8
  TRADING                  false

Plus from .env:
  ONDO_REST_BASE_URL       https://api.ondoperps.xyz
  ONDO_WS_URL              wss://api.ondoperps.xyz/ws
  ONDO_KEY_ID              ondoKeyId_...
  ONDO_API_SECRET          ondoApiSecret_...
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import websockets
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.control_server import BotState, run_control_server  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from ondo_client import (  # noqa: E402
    OndoSigner, OndoRest, OndoRestError,
    MarketMeta, parse_markets_response,
    snap_price, snap_size, to_float,
    ws_login, ws_ping_loop, WS_PING_INTERVAL_SEC,
    make_client_order_id,
)


load_dotenv(".env")

REST_BASE_URL = os.environ["ONDO_REST_BASE_URL"]
WS_URL        = os.environ["ONDO_WS_URL"]
KEY_ID        = os.environ["ONDO_KEY_ID"]
API_SECRET    = os.environ["ONDO_API_SECRET"]


# ---- env helpers ---------------------------------------------------------

def _env_float(k: str, default: float) -> float:
    v = os.environ.get(k)
    if v is None or v == "": return default
    try: return float(v)
    except ValueError: return default

def _env_int(k: str, default: int) -> int:
    v = os.environ.get(k)
    if v is None or v == "": return default
    try: return int(float(v))
    except ValueError: return default

def _env_bool(k: str, default: bool) -> bool:
    v = (os.environ.get(k) or "").strip().lower()
    if v in ("1", "true", "yes", "on"):  return True
    if v in ("0", "false", "no", "off"): return False
    return default


PAIRS_INCLUDE_RAW       = (os.environ.get("PAIRS_INCLUDE") or "").strip()
ORDER_SIZE_USD          = _env_float("ORDER_SIZE_USD", 50.0)
LEVELS_PER_SIDE         = max(1, _env_int("LEVELS_PER_SIDE", 3))
GRID_STEP_BP            = _env_float("GRID_STEP_BP", 8.0)
MAX_NOTIONAL_PER_PAIR_USD = _env_float("MAX_NOTIONAL_PER_PAIR_USD", 500.0)
MAX_TOTAL_NOTIONAL_USD    = _env_float("MAX_TOTAL_NOTIONAL_USD", 2000.0)
REQUOTE_DRIFT_BP        = _env_float("REQUOTE_DRIFT_BP", 4.0)
REQUOTE_MAX_AGE_SEC     = _env_float("REQUOTE_MAX_AGE_SEC", 300.0)
POLL_INTERVAL_SEC       = _env_float("POLL_INTERVAL_SEC", 1.0)
SESSION_DD_LIMIT_USD    = _env_float("SESSION_DD_LIMIT_USD", -10.0)
MAX_FV_AGE_SEC          = _env_float("MAX_FV_AGE_SEC", 120.0)
PLACE_THROTTLE_SEC      = _env_float("PLACE_THROTTLE_SEC", 0.05)
# OndoPerps lets you do ~25 req/s steady, burst 50. 50ms gap baseline is
# safely under that limit even with 100+ concurrent slots.

PAIR_STOP_LOSS_USD       = _env_float("PAIR_STOP_LOSS_USD", -3.0)
FILL_IMBALANCE_MIN_FILLS = _env_int("FILL_IMBALANCE_MIN_FILLS", 8)
FILL_IMBALANCE_RATIO     = _env_float("FILL_IMBALANCE_RATIO", 2.5)

WS_PING_TIMEOUT_SEC     = _env_float("WS_PING_TIMEOUT_SEC", 20.0)
# Exponential backoff for WS reconnects so a misconfigured auth or a
# transient outage doesn't trigger a 429 storm against the 25 req/s WS
# rate limit. Resets to MIN on successful subscribe.
WS_BACKOFF_MIN_SEC      = 3.0
WS_BACKOFF_MAX_SEC      = 60.0

# When ONDO_DEBUG_WS=1, every WS frame from every consumer is logged at
# INFO. Use for diagnosing missing channels / wrong subscribe shapes.
_DEBUG_WS = (os.environ.get("ONDO_DEBUG_WS") or "").strip().lower() in (
    "1", "true", "yes", "on",
)

TRADING       = _env_bool("TRADING", _env_bool("TRADING_ENABLED", False))
CONTROL_PORT  = _env_int("CONTROL_PORT", 8140)

# When true the bot boots with state_pub.running=False — WS feeds + REST
# wire up and the dashboard populates, but the quote_loop sits idle until
# you POST /start (i.e. click "Start" on the dashboard). Default off
# keeps the legacy behaviour where the bot quotes immediately.
START_PAUSED  = _env_bool("START_PAUSED", False)

DATA_DIR   = Path("data")
TRADES_CSV = DATA_DIR / "wti_trades.csv"


# ---- live-tunable params -------------------------------------------------
# `_LIVE` is rebound to state_pub.grid_params in main(). Strategy code
# reads its tunable values via _live_float/_live_int so a POST /config
# from the dashboard takes effect on the next reconcile tick without a
# restart. The module-level constants above remain the startup defaults
# (and the fallback if a key is ever missing from _LIVE).

_LIVE: dict[str, Any] = {}

def _live_float(k: str, default: float) -> float:
    v = _LIVE.get(k, default)
    try: return float(v)
    except (TypeError, ValueError): return default

def _live_int(k: str, default: int) -> int:
    v = _LIVE.get(k, default)
    try: return int(v)
    except (TypeError, ValueError): return default


# ---- state ---------------------------------------------------------------

@dataclass
class Bbo:
    bid_px: float | None = None
    ask_px: float | None = None
    updated_ms: int = 0

    @property
    def mid(self) -> float | None:
        if self.bid_px is None or self.ask_px is None:
            return None
        return 0.5 * (self.bid_px + self.ask_px)


@dataclass
class LevelState:
    coid: str             # clientOrderId (string)
    is_buy: bool
    price_str: str        # snapped decimal string sent on the wire
    size_str: str
    placed_at_ms: int


@dataclass
class PairState:
    bbo: Bbo = field(default_factory=Bbo)
    position: float = 0.0
    avg_entry: float = 0.0
    realized_pnl: float = 0.0    # accumulated from fillsPerps
    unrealized_pnl: float = 0.0  # from positionsPerps
    position_value: float = 0.0
    levels: dict[int, LevelState] = field(default_factory=dict)
    cap_warned: bool = False
    halt_reason: str | None = None
    fills_buy: int = 0
    fills_sell: int = 0


@dataclass
class State:
    pairs: dict[str, PairState] = field(default_factory=dict)
    coid_counter: int = 1
    # coid -> server orderId (string). OndoPerps cancels by path param so
    # we always have a string ID either way (server or client form).
    coid_to_oid: dict[str, str] = field(default_factory=dict)
    placed_coids: dict[str, float] = field(default_factory=dict)
    # (market, coid, orderId) tuples queued by orphan detection.
    orphans_to_cancel: list[tuple[str, str, str]] = field(default_factory=list)
    last_signed_op_ts: float = 0.0
    current_throttle_sec: float = 0.0
    consecutive_signed_ok: int = 0
    last_throttle_bump_log_ts: float = 0.0
    session_start_equity: float | None = None
    current_equity: float | None = None
    halt_reason: str | None = None
    halt_detail: str = ""
    realized_baseline: dict[str, float] = field(default_factory=dict)
    realized_baseline_armed: bool = False


# ---- WS consumers --------------------------------------------------------

async def consume_depth_book(st: State, metas: dict[str, MarketMeta],
                                stop: asyncio.Event, log: logging.Logger):
    """Subscribe to depthBooksPerps for all configured markets; maintain BBO.

    The depth-book channel docs only describe snapshot payloads (no
    documented incremental delta semantics). We treat every payload as a
    full snapshot: replace the local bids/asks per market on each update.
    The bot only needs top-of-book, so we just take asks[0] / bids[0].
    """
    markets = list(metas.keys())

    def _level_price(level) -> str | None:
        # Live API uses array form ["price", "size"]; the public docs
        # example showed {"price": "...", "size": "..."} but that's wrong.
        # Be tolerant of both so a future doc fix doesn't break us.
        if isinstance(level, (list, tuple)) and level:
            return level[0]
        if isinstance(level, dict):
            return level.get("price")
        return None

    def update_bbo(payload: list):
        for entry in payload:
            mkt = entry.get("market")
            if mkt not in st.pairs: continue
            asks = entry.get("asks") or []
            bids = entry.get("bids") or []
            if not asks or not bids: continue
            bid_px = _level_price(bids[0])
            ask_px = _level_price(asks[0])
            if bid_px is None or ask_px is None: continue
            b = st.pairs[mkt].bbo
            try:
                b.bid_px = float(bid_px)
                b.ask_px = float(ask_px)
            except (TypeError, ValueError):
                continue
            b.updated_ms = int(time.time() * 1000)

    backoff = WS_BACKOFF_MIN_SEC
    while not stop.is_set():
        ping_task: asyncio.Task | None = None
        try:
            async with websockets.connect(WS_URL, close_timeout=2,
                                            max_queue=128) as ws:
                # Include explicit `depthLevels` and `limit`. Docs list
                # them as optional but on the live API a bare subscribe
                # (no depthLevels) produces no updates at all.
                await ws.send(json.dumps({
                    "op":          "subscribe",
                    "channel":     "depthBooksPerps",
                    "markets":     markets,
                    "depthLevels": "0.01",
                    "limit":       20,
                }))
                ping_task = asyncio.create_task(ws_ping_loop(ws, stop))
                log.info(f"depth-book WS subscribed to {len(markets)} markets")
                backoff = WS_BACKOFF_MIN_SEC      # reset on successful connect
                async for raw in ws:
                    if stop.is_set(): break
                    if _DEBUG_WS:
                        log.info(f"[depth-book frame] {raw[:400]}")
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "pong": continue
                    if t == "error":
                        log.warning(f"depth-book WS error: {msg}")
                        continue
                    if msg.get("channel") != "depthBooksPerps":
                        continue
                    data = msg.get("data")
                    if isinstance(data, list):
                        update_bbo(data)
        except Exception as exc:  # noqa: BLE001
            log.warning(f"depth-book WS: {type(exc).__name__}: {exc} "
                        f"— reconnect in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
        finally:
            if ping_task is not None:
                ping_task.cancel()


async def consume_orders(st: State, signer: OndoSigner,
                            stop: asyncio.Event, log: logging.Logger):
    """ordersPerps channel: maintains coid_to_oid and runs orphan detection.

    Status enum: "open" / "fullyfilled" / "canceled". Treat anything not
    "open" as terminal — drop from tracked state.
    """
    OPEN_STATUS = "open"
    TERMINAL    = {"fullyfilled", "canceled"}

    def apply(rows):
        if not isinstance(rows, list): return
        tracked_coids: set[str] = set()
        coid_to_slot: dict[str, tuple[str, int]] = {}
        for mkt_, ps_ in st.pairs.items():
            for slot_, lvl_ in ps_.levels.items():
                tracked_coids.add(lvl_.coid)
                coid_to_slot[lvl_.coid] = (mkt_, slot_)
        already_enqueued = {coid for (_m, coid, _oid) in st.orphans_to_cancel}

        for o in rows:
            coid = str(o.get("clientOrderId") or "")
            oid  = str(o.get("orderId") or "")
            mkt  = str(o.get("market") or "")
            status = str(o.get("status") or "").lower()
            if not coid or not oid:
                continue
            if status == OPEN_STATUS:
                st.coid_to_oid[coid] = oid
                if (coid not in tracked_coids
                        and coid in st.placed_coids
                        and coid not in already_enqueued):
                    st.orphans_to_cancel.append((mkt, coid, oid))
                    already_enqueued.add(coid)
            elif status in TERMINAL:
                st.coid_to_oid.pop(coid, None)
                st.placed_coids.pop(coid, None)
                slot_info = coid_to_slot.get(coid)
                if slot_info is not None:
                    mkt_done, slot_done = slot_info
                    ps_done = st.pairs.get(mkt_done)
                    if (ps_done is not None
                            and ps_done.levels.get(slot_done) is not None
                            and ps_done.levels[slot_done].coid == coid):
                        ps_done.levels.pop(slot_done, None)

    backoff = WS_BACKOFF_MIN_SEC
    while not stop.is_set():
        ping_task: asyncio.Task | None = None
        try:
            async with websockets.connect(WS_URL, close_timeout=2,
                                            max_queue=128) as ws:
                if not await ws_login(ws, signer, log):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
                    continue
                await ws.send(json.dumps({
                    "op":      "subscribe",
                    "channel": "ordersPerps",
                }))
                ping_task = asyncio.create_task(ws_ping_loop(ws, stop))
                log.info("orders WS subscribed")
                backoff = WS_BACKOFF_MIN_SEC
                async for raw in ws:
                    if stop.is_set(): break
                    if _DEBUG_WS:
                        log.info(f"[orders frame] {raw[:400]}")
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "pong": continue
                    if t == "error":
                        log.warning(f"orders WS error: {msg}")
                        continue
                    if msg.get("channel") != "ordersPerps":
                        continue
                    apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"orders WS: {type(exc).__name__}: {exc} "
                        f"— reconnect in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
        finally:
            if ping_task is not None:
                ping_task.cancel()


async def consume_positions(st: State, signer: OndoSigner,
                              stop: asyncio.Event, log: logging.Logger):
    """positionsPerps: maintain signed position + unrealized PnL per market.

    OndoPerps reports `direction` ("long"/"short"/"neutral") and
    `netQuantity` (unsigned size). We sign locally: long -> +, short -> -.
    """

    def apply(rows):
        if not isinstance(rows, list): return
        for p in rows:
            mkt = p.get("market")
            if mkt not in st.pairs: continue
            ps = st.pairs[mkt]
            direction = str(p.get("direction") or "").lower()
            qty = to_float(p.get("netQuantity"))
            sign = 1.0 if direction == "long" else -1.0 if direction == "short" else 0.0
            ps.position    = sign * qty
            ps.avg_entry   = to_float(p.get("averageEntryPrice"))
            ps.unrealized_pnl = to_float(p.get("unrealizedPnl"))
            ps.position_value = to_float(p.get("notionalValue"))

    backoff = WS_BACKOFF_MIN_SEC
    while not stop.is_set():
        ping_task: asyncio.Task | None = None
        try:
            async with websockets.connect(WS_URL, close_timeout=2,
                                            max_queue=64) as ws:
                if not await ws_login(ws, signer, log):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
                    continue
                await ws.send(json.dumps({
                    "op":      "subscribe",
                    "channel": "positionsPerps",
                }))
                ping_task = asyncio.create_task(ws_ping_loop(ws, stop))
                log.info("positions WS subscribed")
                backoff = WS_BACKOFF_MIN_SEC
                async for raw in ws:
                    if stop.is_set(): break
                    if _DEBUG_WS:
                        log.info(f"[positions frame] {raw[:400]}")
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "pong": continue
                    if msg.get("channel") != "positionsPerps":
                        continue
                    apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"positions WS: {type(exc).__name__}: {exc} "
                        f"— reconnect in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
        finally:
            if ping_task is not None:
                ping_task.cancel()


async def consume_fills(st: State, signer: OndoSigner,
                          stop: asyncio.Event, log: logging.Logger):
    """fillsPerps: accumulate realized PnL + b/s counters + CSV rows.

    OndoPerps charges non-zero fees (maker 1.5bp / taker 3.5bp), so the
    per-fill `fee` is real — we subtract it from realized so `realized_pnl`
    is net of fees, same as how the dashboard interprets it on Lighter.
    `feeRebate` is the rebate portion if any (added back).
    """

    def apply(rows):
        if not isinstance(rows, list): return
        for f in rows:
            mkt = f.get("market")
            if mkt not in st.pairs: continue
            ps = st.pairs[mkt]
            side = str(f.get("side") or "").lower()
            if side == "buy":  ps.fills_buy  += 1
            if side == "sell": ps.fills_sell += 1
            # `pnl` is realized PnL on this fill (set only when the fill
            # reduced an existing position); `fee` is positive, `feeRebate`
            # is positive when granted.
            pnl     = to_float(f.get("pnl"))
            fee     = to_float(f.get("fee"))
            rebate  = to_float(f.get("feeRebate"))
            ps.realized_pnl += pnl - fee + rebate

            price = f.get("price")
            size  = f.get("size")
            if price and size:
                try:
                    _csv_trade_log(mkt, side.upper(), float(size), float(price), "ok")
                except (TypeError, ValueError):
                    pass
            log.info(
                f"{mkt} FILL {side.upper()} {size}@{price} "
                f"maker={f.get('isMaker')} fee={fee} pnl={pnl}"
            )

    backoff = WS_BACKOFF_MIN_SEC
    while not stop.is_set():
        ping_task: asyncio.Task | None = None
        try:
            async with websockets.connect(WS_URL, close_timeout=2,
                                            max_queue=128) as ws:
                if not await ws_login(ws, signer, log):
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
                    continue
                await ws.send(json.dumps({
                    "op":      "subscribe",
                    "channel": "fillsPerps",
                }))
                ping_task = asyncio.create_task(ws_ping_loop(ws, stop))
                log.info("fills WS subscribed")
                backoff = WS_BACKOFF_MIN_SEC
                async for raw in ws:
                    if stop.is_set(): break
                    if _DEBUG_WS:
                        log.info(f"[fills frame] {raw[:400]}")
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "pong": continue
                    if msg.get("channel") != "fillsPerps":
                        continue
                    apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"fills WS: {type(exc).__name__}: {exc} "
                        f"— reconnect in {backoff:.1f}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, WS_BACKOFF_MAX_SEC)
        finally:
            if ping_task is not None:
                ping_task.cancel()


# ---- orphan cleanup ------------------------------------------------------

async def orphan_cleanup_loop(st: State, rest: OndoRest | None,
                                metas: dict[str, MarketMeta],
                                stop: asyncio.Event, log: logging.Logger):
    if rest is None:
        return
    last_gc_ts = time.time()
    while not stop.is_set():
        await asyncio.sleep(ORPHAN_SCAN_INTERVAL_SEC)

        # Batch-drain whatever's queued (cap per pass).
        ids_to_cancel: list[str] = []
        drained_set: set[str] = set()
        while st.orphans_to_cancel and len(ids_to_cancel) < ORPHAN_DRAIN_PER_PASS \
                and not stop.is_set():
            _mkt, coid, oid = st.orphans_to_cancel.pop(0)
            tracked = False
            for ps in st.pairs.values():
                if any(lvl.coid == coid for lvl in ps.levels.values()):
                    tracked = True
                    break
            if tracked: continue
            ids_to_cancel.append(oid)
            drained_set.add(coid)

        if ids_to_cancel:
            log.warning(f"orphan batch cancel: {len(ids_to_cancel)} ids")
            await _throttle_signed_op(st)
            try:
                await rest.batch_cancel(ids_to_cancel)
            except OndoRestError as exc:
                if exc.is_rate_limited:
                    _on_rate_limit(st, log, "orphan batch")
                log.warning(f"orphan batch cancel raised: {exc}")
            except Exception as exc:  # noqa: BLE001
                log.warning(f"orphan batch cancel raised: "
                            f"{type(exc).__name__}: {exc}")
            else:
                _on_signed_ok(st, log)
                for coid in drained_set:
                    st.coid_to_oid.pop(coid, None)
                    st.placed_coids.pop(coid, None)

        now = time.time()
        if now - last_gc_ts > 60.0:
            last_gc_ts = now
            cutoff = now - ORPHAN_PLACED_COID_TTL_SEC
            stale = [c for c, ts in st.placed_coids.items() if ts < cutoff]
            for c in stale:
                st.placed_coids.pop(c, None)
            if stale:
                log.info(f"orphan cleanup: GC'd {len(stale)} stale placed_coids")


async def rest_orphan_sweep_loop(st: State, metas: dict[str, MarketMeta],
                                   rest: OndoRest, stop: asyncio.Event,
                                   log: logging.Logger):
    """Periodic REST poll of open orders — backstop when WS missed an event."""
    while not stop.is_set():
        await asyncio.sleep(REST_SWEEP_INTERVAL_SEC)
        tracked_coids: set[str] = set()
        for ps in st.pairs.values():
            for lvl in ps.levels.values():
                tracked_coids.add(lvl.coid)
        already_enqueued = {coid for (_m, coid, _oid) in st.orphans_to_cancel}

        new_orphans = 0
        healed = 0
        try:
            resp = await asyncio.wait_for(rest.get_open_orders(),
                                            timeout=REST_SWEEP_TIMEOUT_SEC)
        except Exception as exc:  # noqa: BLE001
            log.warning(f"REST sweep failed: {type(exc).__name__}: {exc}")
            continue

        orders = (resp.get("result") or {})
        if isinstance(orders, dict):
            orders = orders.get("orders") or []
        if not isinstance(orders, list):
            continue

        for o in orders:
            coid = str(o.get("clientOrderId") or "")
            oid  = str(o.get("orderId") or "")
            mkt  = str(o.get("market") or "")
            if not coid or not oid: continue
            if coid not in st.coid_to_oid:
                st.coid_to_oid[coid] = oid
                healed += 1
            if (coid not in tracked_coids
                    and coid in st.placed_coids
                    and coid not in already_enqueued):
                st.orphans_to_cancel.append((mkt, coid, oid))
                already_enqueued.add(coid)
                new_orphans += 1

        if new_orphans or healed:
            log.info(
                f"REST sweep: +{new_orphans} orphans enqueued, "
                f"+{healed} coid->oid healed, "
                f"queue_size={len(st.orphans_to_cancel)}"
            )


# ---- order helpers -------------------------------------------------------

def _make_coid(st: State, market_idx: int, slot: int) -> str:
    st.coid_counter = (st.coid_counter + 1) & 0xFFFFFFFF
    if st.coid_counter == 0:
        st.coid_counter = 1
    return make_client_order_id(st.coid_counter, market_idx, slot)


def _level_slot(level_idx: int, is_buy: bool) -> int:
    # Slot encoding depends on N (LEVELS_PER_SIDE): bids = 1..N,
    # asks = N+1..2N. Reading N live means changing it on the fly is OK
    # — the reconciler will cancel slots no longer in desired_orders'
    # output and re-place the renumbered asks. There's a brief ~1-2s
    # window during the transition where slot meanings shift; the
    # existing drift/age requote machinery cleans it up automatically.
    n = _live_int("levels_per_side", LEVELS_PER_SIDE)
    return level_idx if is_buy else (n + level_idx)


THROTTLE_BUMP_CAP_SEC      = 5.0
THROTTLE_DECAY_AFTER_OK    = 25
THROTTLE_DECAY_FACTOR      = 0.85


async def _throttle_signed_op(st: State) -> None:
    if st.current_throttle_sec <= 0:
        st.current_throttle_sec = PLACE_THROTTLE_SEC
    now = time.time()
    target = max(st.last_signed_op_ts + st.current_throttle_sec, now)
    st.last_signed_op_ts = target
    if target > now:
        await asyncio.sleep(target - now)


def _on_rate_limit(st: State, log: logging.Logger, context: str) -> None:
    old = st.current_throttle_sec
    st.current_throttle_sec = min(st.current_throttle_sec * 2, THROTTLE_BUMP_CAP_SEC)
    st.consecutive_signed_ok = 0
    now = time.time()
    if now - st.last_throttle_bump_log_ts > 1.0:
        log.warning(
            f"rate-limited ({context}): throttle {old:.2f}s -> "
            f"{st.current_throttle_sec:.2f}s"
        )
        st.last_throttle_bump_log_ts = now


def _on_signed_ok(st: State, log: logging.Logger) -> None:
    if st.current_throttle_sec <= PLACE_THROTTLE_SEC:
        st.consecutive_signed_ok = 0
        return
    st.consecutive_signed_ok += 1
    if st.consecutive_signed_ok >= THROTTLE_DECAY_AFTER_OK:
        st.consecutive_signed_ok = 0
        old = st.current_throttle_sec
        st.current_throttle_sec = max(
            PLACE_THROTTLE_SEC, st.current_throttle_sec * THROTTLE_DECAY_FACTOR
        )
        log.info(
            f"throttle decay: {old:.2f}s -> {st.current_throttle_sec:.2f}s "
            f"(baseline {PLACE_THROTTLE_SEC}s)"
        )


def _csv_trade_log(market: str, side: str, size: float, price: float,
                    outcome: str, error: str = "") -> None:
    DATA_DIR.mkdir(exist_ok=True)
    new_file = not TRADES_CSV.exists() or TRADES_CSV.stat().st_size == 0
    with TRADES_CSV.open("a", newline="", buffering=1) as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["ts", "market", "side", "size", "price",
                        "outcome", "attempt", "error"])
        w.writerow([time.time(), market, side, size,
                    f"{price:.6f}", outcome, 1, error])


async def place_order(rest: OndoRest | None, meta: MarketMeta,
                       coid: str, price_str: str, size_str: str,
                       is_buy: bool, st: State,
                       log: logging.Logger) -> tuple[bool, str, str]:
    if rest is None:
        return True, "dry_run", ""
    try:
        result = await rest.place_order(
            market=meta.market,
            side="buy" if is_buy else "sell",
            price=price_str,
            size=size_str,
            client_order_id=coid,
            post_only=True,
            order_type="limit",
            time_in_force="GTC",
        )
    except OndoRestError as exc:
        if exc.is_rate_limited:
            _on_rate_limit(st, log, f"place {meta.market}")
        return False, str(exc), ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}", ""
    _on_signed_ok(st, log)
    inner = result.get("result") or {}
    return True, "placed", str(inner.get("orderId") or "")


async def cancel_one(rest: OndoRest, meta: MarketMeta, coid: str,
                      st: State, log: logging.Logger) -> bool:
    """Cancel preferring the server orderId when known, falling back to the
    client:coid path form."""
    oid = st.coid_to_oid.get(coid)
    await _throttle_signed_op(st)
    try:
        if oid:
            await rest.cancel_order(oid, by_client_id=False)
        else:
            await rest.cancel_order(coid, by_client_id=True)
    except OndoRestError as exc:
        if exc.is_rate_limited:
            _on_rate_limit(st, log, f"cancel {meta.market}")
        # `order_not_found` / `order_already_canceled` / `order_already_fully_filled`
        # are benign — the order's already gone, just clear local state.
        benign = ("order_not_found", "order_already_canceled",
                  "order_already_fully_filled")
        if any(b in str(exc).lower() for b in benign):
            st.coid_to_oid.pop(coid, None)
            return True
        log.warning(f"{meta.market} cancel coid={coid} raised: {exc}")
        return False
    except Exception as exc:  # noqa: BLE001
        log.warning(f"{meta.market} cancel coid={coid} raised: "
                    f"{type(exc).__name__}: {exc}")
        return False
    _on_signed_ok(st, log)
    st.coid_to_oid.pop(coid, None)
    return True


async def cancel_all_orders_safe(rest: OndoRest, st: State,
                                   log: logging.Logger) -> None:
    """Cancel all known coids via one batch call. Falls back to
    per-market /v1/perps/orders if we don't have any local state yet."""
    ids = [oid for oid in st.coid_to_oid.values() if oid]
    if not ids:
        # Pull authoritative list from REST.
        try:
            resp = await rest.get_open_orders()
            orders = (resp.get("result") or [])
            if isinstance(orders, dict):
                orders = orders.get("orders") or []
            ids = [str(o.get("orderId")) for o in orders if o.get("orderId")]
        except Exception as exc:  # noqa: BLE001
            log.warning(f"cancel_all: REST listing failed: {exc}")
            return
    if not ids:
        return
    # OndoPerps caps batch_cancel at 20 orderIDs per call (the API returns
    # "Too many orderIDs, max is 20" otherwise).
    for i in range(0, len(ids), 20):
        chunk = ids[i:i+20]
        try:
            await rest.batch_cancel(chunk)
        except OndoRestError as exc:
            log.warning(f"cancel_all batch raised: {exc}")
        except Exception as exc:  # noqa: BLE001
            log.warning(f"cancel_all batch raised: "
                        f"{type(exc).__name__}: {exc}")
        # Tiny gap between batches so we don't trip the 25 req/s gateway
        # when there are dozens of leftover orders.
        await asyncio.sleep(0.1)


# ---- ladder logic --------------------------------------------------------

def desired_orders(meta: MarketMeta, ps: PairState, exit_only: bool = False,
                    ) -> list[tuple[int, bool, str, str, float]]:
    """Return list of (level_slot, is_buy, price_str, size_str, price_dec)."""
    mid = ps.bbo.mid
    if mid is None or mid <= 0:
        return []
    order_size_usd  = _live_float("order_size_usd",        ORDER_SIZE_USD)
    grid_step_bp    = _live_float("grid_step_bp",          GRID_STEP_BP)
    cap_per_pair    = _live_float("max_notional_per_pair", MAX_NOTIONAL_PER_PAIR_USD)
    size_dec_target = order_size_usd / mid
    # Snap size to baseIncrement; bump up to one increment minimum.
    size_str = snap_size(size_dec_target, meta.base_increment)
    try:
        size_dec_snapped = float(size_str)
    except ValueError:
        return []
    if size_dec_snapped <= 0:
        size_str = format(meta.base_increment.normalize(), 'f')
        size_dec_snapped = float(size_str)

    out: list[tuple[int, bool, str, str, float]] = []
    cap_base = cap_per_pair / mid
    n_levels = _live_int("levels_per_side", LEVELS_PER_SIDE)
    for k in range(1, n_levels + 1):
        bp = k * grid_step_bp
        bid_px = mid * (1 - bp / 10_000.0)
        ask_px = mid * (1 + bp / 10_000.0)
        if ps.bbo.ask_px is not None and bid_px >= ps.bbo.ask_px:
            bid_px = ps.bbo.ask_px - float(meta.quote_increment)
        if ps.bbo.bid_px is not None and ask_px <= ps.bbo.bid_px:
            ask_px = ps.bbo.bid_px + float(meta.quote_increment)

        bid_projected = ps.position + k * size_dec_snapped
        ask_projected = ps.position - k * size_dec_snapped
        bid_ok = bid_projected <= cap_base
        ask_ok = ask_projected >= -cap_base
        if exit_only:
            if ps.position > 0:    bid_ok = False
            elif ps.position < 0:  ask_ok = False
            else:                  bid_ok = ask_ok = False

        if bid_ok:
            out.append((_level_slot(k, True), True,
                        snap_price(bid_px, meta.quote_increment),
                        size_str, bid_px))
        if ask_ok:
            out.append((_level_slot(k, False), False,
                        snap_price(ask_px, meta.quote_increment),
                        size_str, ask_px))
    return out


def total_notional_usd(st: State) -> float:
    return sum(abs(ps.position_value) for ps in st.pairs.values())


# OndoPerps currently lists 22 markets across 4 asset classes — none are
# crypto majors, so the directional-delta widget here is mostly equity vs
# commodity vs index. Tickers in PAIRS_INCLUDE are the canonical id format.
ASSET_CLASS_BY_MARKET: dict[str, str] = {
    "XAU-USD.P":    "commodity",
    "XAG-USD.P":    "commodity",
    "WTI-USD.P":    "commodity",
    "US500-USD.P":  "index",
    "US100-USD.P":  "index",
    "DRAM-USD.P":   "etf",
}


def classify_market(mkt: str) -> str:
    return ASSET_CLASS_BY_MARKET.get(mkt, "equity")


def compute_directional_delta(st: State) -> dict[str, Any]:
    out: dict[str, Any] = {
        "crypto": 0.0, "equity": 0.0, "commodity": 0.0, "index": 0.0,
        "etf": 0.0, "total": 0.0, "by_market": {},
    }
    for mkt, ps in st.pairs.items():
        mid = ps.bbo.mid
        if mid is None or mid <= 0 or ps.position == 0:
            out["by_market"][mkt] = 0.0
            continue
        delta = ps.position * mid
        cls = classify_market(mkt)
        out[cls] = out.get(cls, 0.0) + delta
        out["total"] += delta
        out["by_market"][mkt] = delta
    for k in ("crypto", "equity", "commodity", "index", "etf", "total"):
        out[k] = round(out[k], 2)
    out["by_market"] = {k: round(v, 2) for k, v in out["by_market"].items()}
    return out


def evaluate_pair_halt(ps: PairState, st: State, now_ms: int,
                        mkt: str) -> str | None:
    if ps.bbo.mid is None:
        return "no_book"
    if (now_ms - ps.bbo.updated_ms) > MAX_FV_AGE_SEC * 1000:
        return "book_stale"
    if ps.halt_reason and ps.halt_reason.startswith(("pair_stop_loss", "fill_imbalance")):
        return ps.halt_reason
    pair_stop_loss = _live_float("pair_stop_loss_usd",     PAIR_STOP_LOSS_USD)
    imbalance_ratio = _live_float("fill_imbalance_ratio",   FILL_IMBALANCE_RATIO)
    imbalance_min   = _live_int  ("fill_imbalance_min_fills", FILL_IMBALANCE_MIN_FILLS)
    baseline = st.realized_baseline.get(mkt, 0.0)
    sess_realized = ps.realized_pnl - baseline
    if pair_stop_loss < 0 and sess_realized <= pair_stop_loss:
        return f"pair_stop_loss({sess_realized:+.2f})"
    total_fills = ps.fills_buy + ps.fills_sell
    if total_fills >= imbalance_min:
        dom = max(ps.fills_buy, ps.fills_sell)
        sub = max(1, min(ps.fills_buy, ps.fills_sell))
        ratio = dom / sub
        if ratio >= imbalance_ratio:
            return f"fill_imbalance({ps.fills_buy}/{ps.fills_sell})"
    return None


def session_dd_pnl(st: State) -> float | None:
    if st.session_start_equity is None or st.current_equity is None:
        return None
    return st.current_equity - st.session_start_equity


def exit_only_reason(st: State) -> str | None:
    cap_total = _live_float("max_total_notional",    MAX_TOTAL_NOTIONAL_USD)
    dd_limit  = _live_float("session_dd_limit_usd",  SESSION_DD_LIMIT_USD)
    total = total_notional_usd(st)
    if total >= cap_total:
        return f"total_notional(${total:.0f}>=${cap_total:.0f})"
    pnl = session_dd_pnl(st)
    if pnl is not None and pnl < dd_limit:
        return f"session_dd({pnl:+.2f})"
    return None


# ---- main loop -----------------------------------------------------------

UNCONFIRMED_TIMEOUT_SEC = 120.0
ORPHAN_PLACED_COID_TTL_SEC = 3600.0
ORPHAN_SCAN_INTERVAL_SEC   = 1.0
ORPHAN_DRAIN_PER_PASS      = 20      # API caps batch_cancel at 20 ids
REST_SWEEP_INTERVAL_SEC    = 180.0
REST_SWEEP_TIMEOUT_SEC     = 8.0


def _resolve_unconfirmed(st: State, ps: PairState, slot: int,
                          log: logging.Logger, mkt: str) -> str:
    lvl = ps.levels.get(slot)
    if lvl is None:
        return 'ok'
    if lvl.coid in st.coid_to_oid:
        return 'ok'
    age_s = (time.time() * 1000 - lvl.placed_at_ms) / 1000.0
    if age_s < UNCONFIRMED_TIMEOUT_SEC:
        return 'wait'
    log.warning(
        f"{mkt} slot={slot} coid={lvl.coid} unconfirmed for {age_s:.1f}s "
        f"— removing local state (will rely on client: cancel form if needed)"
    )
    return 'drop'


async def reconcile_pair(mkt: str, ps: PairState, meta: MarketMeta,
                          rest: OndoRest | None, market_idx: int,
                          st: State, exit_only: bool,
                          log: logging.Logger) -> int:
    now_ms = int(time.time() * 1000)
    ph = evaluate_pair_halt(ps, st, now_ms, mkt)
    if ph is not None:
        if ps.halt_reason != ph:
            log.info(f"{mkt} pair-halt: {ph}")
            ps.halt_reason = ph
        if rest is not None and ps.levels:
            for slot, lvl in list(ps.levels.items()):
                state = _resolve_unconfirmed(st, ps, slot, log, mkt)
                if state == 'wait': continue
                if state == 'drop':
                    ps.levels.pop(slot, None)
                    continue
                ok_c = await cancel_one(rest, meta, lvl.coid, st, log)
                if ok_c:
                    ps.levels.pop(slot, None)
        return 0
    if ps.halt_reason:
        log.info(f"{mkt} resume (was: {ps.halt_reason})")
        ps.halt_reason = None

    desired = desired_orders(meta, ps, exit_only=exit_only)
    desired_by_slot = {d[0]: d for d in desired}

    for slot in list(ps.levels.keys()):
        if slot in desired_by_slot: continue
        if rest is None:
            ps.levels.pop(slot, None)
            continue
        state = _resolve_unconfirmed(st, ps, slot, log, mkt)
        if state == 'wait': continue
        if state == 'drop':
            ps.levels.pop(slot, None)
            continue
        ok_c = await cancel_one(rest, meta, ps.levels[slot].coid, st, log)
        if ok_c:
            ps.levels.pop(slot, None)

    placed = 0
    for slot, is_buy, price_str, size_str, price_dec in desired:
        existing = ps.levels.get(slot)
        if existing is not None and rest is not None:
            state = _resolve_unconfirmed(st, ps, slot, log, mkt)
            if state == 'wait': continue
            if state == 'drop':
                ps.levels.pop(slot, None)
                existing = None

        max_age   = _live_float("requote_max_age_sec", REQUOTE_MAX_AGE_SEC)
        drift_lim = _live_float("requote_drift_bp",    REQUOTE_DRIFT_BP)
        need_place = True
        if existing is not None:
            age_s = (time.time() * 1000 - existing.placed_at_ms) / 1000.0
            if existing.price_str == price_str and age_s < max_age:
                need_place = False
            else:
                try:
                    old_px = float(existing.price_str)
                    new_px = float(price_str)
                    drift_bp = abs(new_px - old_px) / old_px * 10_000.0 \
                                 if old_px > 0 else 1e9
                except ValueError:
                    drift_bp = 1e9
                if drift_bp <= drift_lim and age_s < max_age:
                    need_place = False
        if not need_place:
            continue

        if existing is not None and rest is not None:
            ok_c = await cancel_one(rest, meta, existing.coid, st, log)
            if not ok_c:
                continue
            ps.levels.pop(slot, None)

        await _throttle_signed_op(st)
        coid = _make_coid(st, market_idx, slot)
        ok, info_msg, order_id = await place_order(
            rest, meta, coid, price_str, size_str, is_buy, st, log,
        )
        if ok:
            now_ms_p = int(time.time() * 1000)
            ps.levels[slot] = LevelState(
                coid=coid, is_buy=is_buy,
                price_str=price_str, size_str=size_str,
                placed_at_ms=now_ms_p,
            )
            st.placed_coids[coid] = now_ms_p / 1000.0
            if order_id:
                st.coid_to_oid[coid] = order_id
            placed += 1
            log.info(
                f"{mkt} {'BUY' if is_buy else 'SELL'} slot={slot} "
                f"px={price_str} sz={size_str} coid={coid} "
                f"oid={order_id or '?'}"
            )
        else:
            log.warning(
                f"{mkt} {'BUY' if is_buy else 'SELL'} slot={slot} "
                f"px={price_str} FAILED: {info_msg}"
            )
    return placed


async def quote_loop(st: State, rest: OndoRest | None,
                      metas: dict[str, MarketMeta], market_idx: dict[str, int],
                      state_pub: BotState, log: logging.Logger,
                      stop: asyncio.Event):
    while not stop.is_set():
        await asyncio.sleep(POLL_INTERVAL_SEC)
        if not state_pub.running:
            continue

        exo_reason = exit_only_reason(st)
        exit_only = exo_reason is not None
        if exit_only and st.halt_reason != "exit_only":
            log.warning(f"EXIT-ONLY: {exo_reason} — only exposure-reducing rungs placed")
            st.halt_reason = "exit_only"
            st.halt_detail = exo_reason
        elif exit_only and st.halt_detail != exo_reason:
            st.halt_detail = exo_reason
        elif not exit_only and st.halt_reason == "exit_only":
            log.info(f"exit-only resume (was: {st.halt_detail})")
            st.halt_reason = None
            st.halt_detail = ""

        positions_snap: dict[str, float] = {}
        bbo_snap: dict[str, dict[str, float]] = {}
        ex_pnl: dict[str, dict[str, float]] = {}

        for mkt, ps in st.pairs.items():
            meta = metas.get(mkt)
            if meta is None: continue
            positions_snap[mkt] = ps.position
            if ps.bbo.bid_px is not None and ps.bbo.ask_px is not None:
                bbo_snap[mkt] = {"bid": ps.bbo.bid_px, "ask": ps.bbo.ask_px}
            baseline = st.realized_baseline.get(mkt, 0.0)
            live_unreal = ps.unrealized_pnl
            if ps.bbo.mid is not None and ps.position != 0 and ps.avg_entry > 0:
                live_unreal = ps.position * (ps.bbo.mid - ps.avg_entry)
            ex_pnl[mkt] = {
                "position":          ps.position,
                "avg_entry":         ps.avg_entry,
                "unrealized_pnl":    round(live_unreal, 4),
                "unrealized_pnl_ws": round(ps.unrealized_pnl, 4),
                "realized_pnl":      round(ps.realized_pnl, 4),
                "realized_session":  round(ps.realized_pnl - baseline, 4),
                "position_value":    ps.position_value,
            }
            try:
                await reconcile_pair(
                    mkt, ps, meta, rest, market_idx.get(mkt, 0),
                    st, exit_only, log,
                )
            except Exception as exc:  # noqa: BLE001
                log.error(f"{mkt} reconcile raised: {type(exc).__name__}: {exc}")

        state_pub.tick(positions=positions_snap, bbo=bbo_snap)
        state_pub.exchange_pnl = ex_pnl
        state_pub.directional_delta = compute_directional_delta(st)


# ---- equity loop ---------------------------------------------------------

async def equity_loop(st: State, rest: OndoRest, state_pub: BotState,
                       log: logging.Logger, stop: asyncio.Event):
    while not stop.is_set():
        try:
            resp = await rest.get_portfolio_summary()
            summary = resp.get("result") or {}
            try:
                total = float(summary.get("marginBalance") or 0)
                # Available is not directly exposed; netInvested gives a
                # rough utilization signal but isn't a hard available.
                avail = total
            except (TypeError, ValueError):
                total = avail = 0.0
            state_pub.set_balance(total=total, available=avail, currency="USDC")
            if total > 0:
                if st.session_start_equity is None:
                    st.session_start_equity = total
                    log.info(f"session start equity: ${total:.2f}")
                st.current_equity = total
        except Exception as exc:  # noqa: BLE001
            state_pub.set_balance(error=str(exc))
        try:
            await asyncio.wait_for(stop.wait(), timeout=30.0)
            return
        except asyncio.TimeoutError:
            continue


# ---- status --------------------------------------------------------------

async def status_loop(st: State, state_pub: BotState, log: logging.Logger,
                       stop: asyncio.Event):
    while not stop.is_set():
        await asyncio.sleep(10)
        eq = st.current_equity
        pnl = (eq - st.session_start_equity) if (eq and st.session_start_equity) else None
        active = sum(1 for ps in st.pairs.values() if ps.bbo.mid is not None)
        resting = sum(len(ps.levels) for ps in st.pairs.values())
        # Recompute expected from live levels_per_side so the dashboard
        # KPIs and the orphan-accumulation warning track config changes.
        n_levels = _live_int("levels_per_side", LEVELS_PER_SIDE)
        expected = len(st.pairs) * n_levels * 2
        pct = (resting / expected * 100) if expected > 0 else 0
        net_notional = total_notional_usd(st)
        log.info(
            f"pairs_active={active}/{len(st.pairs)} "
            f"resting_orders={resting}/{expected} ({pct:.0f}%) "
            f"net_notional=${net_notional:.0f} eq={'?' if eq is None else f'${eq:.2f}'} "
            f"sess_pnl={'?' if pnl is None else f'{pnl:+.2f}'} halt={st.halt_reason or 'no'}"
        )


# ---- close routine -------------------------------------------------------

async def close_all_positions(st: State, rest: OndoRest,
                                metas: dict[str, MarketMeta],
                                log: logging.Logger):
    """Cancel everything in one batch then market-flatten each non-zero
    position with reduceOnly+IOC market orders."""
    await cancel_all_orders_safe(rest, st, log)
    for ps in st.pairs.values():
        ps.levels.clear()
    await asyncio.sleep(0.5)

    for mkt, ps in st.pairs.items():
        meta = metas.get(mkt)
        if meta is None: continue
        if abs(ps.position) * (ps.bbo.mid or 0.0) < 1.0:
            continue
        size_str = snap_size(abs(ps.position), meta.base_increment)
        if float(size_str) <= 0: continue
        is_buy = ps.position < 0
        log.info(f"close {mkt}: pos={ps.position:+g} -> "
                 f"{'BUY' if is_buy else 'SELL'} size={size_str}")
        await _throttle_signed_op(st)
        try:
            await rest.place_order(
                market=meta.market,
                side="buy" if is_buy else "sell",
                price="0",     # ignored for market
                size=size_str,
                client_order_id=_make_coid(st, 0, 0),
                post_only=False,
                reduce_only=True,
                order_type="market",
                time_in_force="IOC",
            )
        except OndoRestError as exc:
            log.error(f"close {mkt} failed: {exc}")
        except Exception as exc:  # noqa: BLE001
            log.error(f"close {mkt} raised: {type(exc).__name__}: {exc}")


# ---- main ----------------------------------------------------------------

async def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except (AttributeError, OSError):
        pass
    LOG_DIR = Path("logs"); LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / f"ondo_grid_mm_{int(time.time())}.log",
                                  encoding="utf-8"),
        ],
    )
    log = logging.getLogger("ondo_mm")

    # PAIRS_INCLUDE here is a CSV of ticker strings ("AAPL-USD.P,NVDA-USD.P").
    # Accept lower-case tickers too for convenience.
    markets_wanted = [
        s.strip().upper() for s in PAIRS_INCLUDE_RAW.split(",") if s.strip()
    ]
    if not markets_wanted:
        log.error("PAIRS_INCLUDE is empty — nothing to trade. "
                  "Set it to a CSV of OndoPerps tickers (e.g. AAPL-USD.P,NVDA-USD.P).")
        return

    log.info(
        f"config: pairs={len(markets_wanted)} order_size=${ORDER_SIZE_USD} "
        f"levels/side={LEVELS_PER_SIDE} step={GRID_STEP_BP}bp "
        f"max_per_pair=${MAX_NOTIONAL_PER_PAIR_USD} "
        f"max_total=${MAX_TOTAL_NOTIONAL_USD} "
        f"requote_drift={REQUOTE_DRIFT_BP}bp armed={TRADING}"
    )

    signer = OndoSigner(KEY_ID, API_SECRET)
    rest = OndoRest(REST_BASE_URL, signer)

    try:
        markets_resp = await rest.get_markets()
        metas = parse_markets_response(markets_resp, set(markets_wanted))
        missing = set(markets_wanted) - set(metas.keys())
        if missing:
            log.warning(f"requested markets not found: {sorted(missing)}")
        if not metas:
            log.error("no usable markets resolved — aborting")
            return
        log.info(
            "markets: " + ", ".join(
                f"{m.market}(sz_step={m.base_increment} px_step={m.quote_increment})"
                for m in metas.values()
            )
        )

        st = State()
        # Seed the coid counter with current epoch-seconds so client IDs
        # never collide across restarts. OndoPerps enforces coid
        # uniqueness across sessions and returns "[400] Client ID …
        # already used" otherwise. epoch-seconds fits in 32 bits until
        # 2106 and is monotonic across boots, so every new session starts
        # at a strictly larger counter than the previous one.
        st.coid_counter = int(time.time()) & 0xFFFFFFFF
        log.info(f"coid counter seeded at {st.coid_counter:#x}")
        for mkt in metas:
            st.pairs[mkt] = PairState()
        market_idx = {mkt: i for i, mkt in enumerate(sorted(metas.keys()))}

        expected_max_orders = len(metas) * LEVELS_PER_SIDE * 2
        log.info(
            f"expected max orders: {expected_max_orders} "
            f"({len(metas)} pairs × {LEVELS_PER_SIDE} levels × 2 sides)"
        )

        state_pub = BotState(
            venue="OndoPerps",
            markets=list(metas.keys()),
            data_dir=DATA_DIR,
            grid_params={
                "pairs":                     list(metas.keys()),
                "order_size_usd":            ORDER_SIZE_USD,
                "levels_per_side":           LEVELS_PER_SIDE,
                "grid_step_bp":              GRID_STEP_BP,
                "max_notional_per_pair":     MAX_NOTIONAL_PER_PAIR_USD,
                "max_total_notional":        MAX_TOTAL_NOTIONAL_USD,
                "requote_drift_bp":          REQUOTE_DRIFT_BP,
                "requote_max_age_sec":       REQUOTE_MAX_AGE_SEC,
                "session_dd_limit_usd":      SESSION_DD_LIMIT_USD,
                "pair_stop_loss_usd":        PAIR_STOP_LOSS_USD,
                "fill_imbalance_ratio":      FILL_IMBALANCE_RATIO,
                "fill_imbalance_min_fills":  FILL_IMBALANCE_MIN_FILLS,
                "trading":                   TRADING,
                "expected_max_orders":       expected_max_orders,
            },
            positions={mkt: 0.0 for mkt in metas},
        )

        # Live-tunables share the same dict as state_pub.grid_params, so
        # an HTTP POST /config writes there and strategy code picks it up
        # on the next tick. See _live_float / _live_int.
        global _LIVE
        _LIVE = state_pub.grid_params

        if START_PAUSED:
            state_pub.running = False
            log.info(
                "started PAUSED (START_PAUSED=true). WS + REST will wire up "
                "and the dashboard will populate, but no orders will be "
                "placed until you press Start on the dashboard."
            )

        stop = asyncio.Event()

        def _on_signal(signame: str) -> None:
            log.info(f"signal {signame} — shutting down gracefully")
            stop.set()

        loop = asyncio.get_running_loop()
        for sig_name in ("SIGTERM", "SIGINT"):
            sig = getattr(signal, sig_name, None)
            if sig is None: continue
            try:
                loop.add_signal_handler(sig, lambda n=sig_name: _on_signal(n))
            except (NotImplementedError, AttributeError, RuntimeError):
                pass  # Windows

        rest_for_orders = rest if TRADING else None

        control_task = asyncio.create_task(run_control_server(state_pub, CONTROL_PORT))
        tasks = [
            asyncio.create_task(consume_depth_book(st, metas, stop, log)),
            asyncio.create_task(consume_orders(st, signer, stop, log)),
            asyncio.create_task(consume_positions(st, signer, stop, log)),
            asyncio.create_task(consume_fills(st, signer, stop, log)),
            asyncio.create_task(orphan_cleanup_loop(st, rest_for_orders, metas, stop, log)),
            asyncio.create_task(equity_loop(st, rest, state_pub, log, stop)),
            asyncio.create_task(status_loop(st, state_pub, log, stop)),
        ]
        if TRADING:
            tasks.append(asyncio.create_task(
                rest_orphan_sweep_loop(st, metas, rest, stop, log)
            ))

        if TRADING:
            log.info("startup: cancel any pre-existing orders on this account")
            await cancel_all_orders_safe(rest, st, log)
            await asyncio.sleep(0.5)

        log.info("waiting for initial book + position snapshot...")
        for _ in range(40):
            ready = sum(1 for ps in st.pairs.values() if ps.bbo.mid is not None)
            if ready >= max(1, len(st.pairs) // 2):
                break
            await asyncio.sleep(0.5)
        log.info(f"book ready on {ready}/{len(st.pairs)} pairs")

        st.realized_baseline = {mkt: st.pairs[mkt].realized_pnl for mkt in st.pairs}
        st.realized_baseline_armed = True
        nz = {m: r for m, r in st.realized_baseline.items() if r}
        if nz:
            log.info(f"realized PnL baseline: {nz}")

        quote_task = asyncio.create_task(
            quote_loop(st, rest_for_orders, metas, market_idx,
                        state_pub, log, stop)
        )
        tasks.append(quote_task)

        try:
            while not stop.is_set():
                await asyncio.sleep(1.0)
                if state_pub.close_requested and state_pub.close_status in ("idle", "pending"):
                    state_pub.running = False
                    state_pub.close_status = "closing"
                    await asyncio.sleep(POLL_INTERVAL_SEC + 0.2)
                    flatten = bool(state_pub.flatten_on_close)
                    verb = "close + flatten" if flatten else "cancel-only (no flatten)"
                    log.info(f"close routine starting: {verb}")
                    try:
                        if TRADING:
                            if flatten:
                                await close_all_positions(st, rest, metas, log)
                            else:
                                await cancel_all_orders_safe(rest, st, log)
                                for ps in st.pairs.values():
                                    ps.levels.clear()
                        state_pub.close_status = "done"
                        log.info(f"close routine done ({verb})")
                    except Exception as exc:  # noqa: BLE001
                        state_pub.close_status = "error"
                        state_pub.close_error  = str(exc)
                        log.error(f"close routine errored: {exc}")
                    break
        except KeyboardInterrupt:
            pass
        finally:
            stop.set()
            if TRADING:
                try:
                    await cancel_all_orders_safe(rest, st, log)
                except Exception as exc:  # noqa: BLE001
                    log.warning(f"shutdown cancel failed: {exc}")
            for t in tasks + [control_task]:
                t.cancel()
            await asyncio.gather(*tasks, control_task, return_exceptions=True)
    finally:
        await rest.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
