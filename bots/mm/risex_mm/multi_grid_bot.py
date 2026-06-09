"""
Multi-pair grid MM on Rise (rise.trade) — port of the Lighter multi_grid_bot.

For each user-selected pair, the bot posts a symmetric ladder of POST_ONLY
limit orders around the live mid:

  bids at mid * (1 - k * GRID_STEP_BP / 1e4) for k = 1..LEVELS_PER_SIDE
  asks at mid * (1 + k * GRID_STEP_BP / 1e4) for k = 1..LEVELS_PER_SIDE

Each order's size is ORDER_SIZE_USD / mid, snapped to the market's step_size
and capped by `min_order_size`. When a level fills the inventory shifts;
the bot then either replaces the level (if the new exposure is below the
per-pair notional cap) or skips the side that would push the position further.

Differences from the Lighter port
---------------------------------
* Rise has no Python SDK — see `rise_client.py` for the hand-rolled
  EIP-712 signer + REST + WS wrappers.
* Per-request EIP-712 signing replaces Lighter's session JWT, so the
  `auth_refresh_loop` is gone — every signed call signs itself.
* Positions WS payload has no `realized_pnl` field; we now subscribe to
  the `trades` channel (Rise's per-fill feed) and accumulate
  `ps.realized_pnl` from each fill's `realized_pnl` field.
* Order status enum is OPEN / FILLED / CANCELLED only — there is no
  PENDING / IN-PROGRESS state on Rise, which simplifies the orphan
  bookkeeping.
* WS subscribe payloads follow Rise's `{method, params}` shape with
  per-channel `market_ids` / `makers` filters.

Env (set by supervisor on spawn; defaults shown):
  PAIRS_INCLUDE           CSV of Rise symbols to trade (required)
  ORDER_SIZE_USD          $50      target USD notional per limit order
  LEVELS_PER_SIDE         3        # of bids and # of asks per pair
  GRID_STEP_BP            8.0      spacing between rungs (basis points)
  MAX_NOTIONAL_PER_PAIR_USD  500   per-pair |position * mid| cap
  MAX_TOTAL_NOTIONAL_USD     2000  bot-wide |position| sum cap
  REQUOTE_DRIFT_BP        4.0      cancel/replace when mid moves > this
  REQUOTE_MAX_AGE_SEC     300      periodic refresh
  POLL_INTERVAL_SEC       1.0      per-pair eval cadence
  SESSION_DD_LIMIT_USD    -10.0    halt at this realized + unrealized drawdown
  TRADING                 false    arm switch (false = log-only dry run)
  PAIR_STOP_LOSS_USD      -3.0     pair's session realized PnL <= this -> halt
  FILL_IMBALANCE_RATIO    2.5      b/s skew halt threshold
  FILL_IMBALANCE_MIN_FILLS 8       min fills before imbalance check arms

Plus from .env:
  RISE_REST_BASE_URL      https://api.rise.trade
  RISE_WS_URL             wss://ws.rise.trade/ws
  RISE_ACCOUNT_ADDRESS    master wallet (registered the signer)
  RISE_SIGNER_ADDRESS     session key address
  RISE_SIGNER_PRIVATE_KEY session key private key
  RISE_EIP712_NAME / _VERSION / _CHAIN_ID / _VERIFYING_CONTRACT
                          EIP-712 domain (call /v1/auth/eip712-domain once
                          to obtain; bot fetches at startup if blank).
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

# Project root exposed so `from common.control_server import ...` works
# regardless of where this bot lives in bots/.
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.control_server import BotState, run_control_server  # noqa: E402

# Local rise client (same directory).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import (  # noqa: E402
    EIP712Domain, RiseSigner, RiseRest, RiseRestError,
    MarketMeta, parse_markets_response,
    wei_to_float, make_client_order_id,
    ws_authenticate, ws_keepalive,
    PING_INTERVAL_SEC,
)


load_dotenv(".env")

REST_BASE_URL    = os.environ["RISE_REST_BASE_URL"]
WS_URL           = os.environ["RISE_WS_URL"]
ACCOUNT_ADDRESS  = os.environ["RISE_ACCOUNT_ADDRESS"]
SIGNER_ADDRESS   = os.environ["RISE_SIGNER_ADDRESS"]
SIGNER_PRIVATE_KEY = os.environ["RISE_SIGNER_PRIVATE_KEY"]


# ---- env helpers ---------------------------------------------------------

def _env_float(k: str, default: float) -> float:
    v = os.environ.get(k)
    if v is None or v == "":
        return default
    try: return float(v)
    except ValueError: return default

def _env_int(k: str, default: int) -> int:
    v = os.environ.get(k)
    if v is None or v == "":
        return default
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
# Rise's verifying contract serializes signed ops via the on-chain Router.
# Empirical 350ms-ish gap between signed requests avoids nonce-anchor reuse
# races; widen if 429s appear, decay back down on sustained success.
PLACE_THROTTLE_SEC      = _env_float("PLACE_THROTTLE_SEC", 0.4)

# Per-pair guardrails — identical semantics to the Lighter port.
PAIR_STOP_LOSS_USD       = _env_float("PAIR_STOP_LOSS_USD", -3.0)
FILL_IMBALANCE_MIN_FILLS = _env_int("FILL_IMBALANCE_MIN_FILLS", 8)
FILL_IMBALANCE_RATIO     = _env_float("FILL_IMBALANCE_RATIO", 2.5)

# WS keepalive (server pings every 30s, 60s inactivity timeout).
WS_PING_INTERVAL_SEC    = _env_float("WS_PING_INTERVAL_SEC", 25.0)
WS_PING_TIMEOUT_SEC     = _env_float("WS_PING_TIMEOUT_SEC", 20.0)

# Accept both TRADING and TRADING_ENABLED for parity with sibling bots.
TRADING                 = _env_bool("TRADING", _env_bool("TRADING_ENABLED", False))
# 8150 by default so this bot doesn't collide with Ondo (8140) when both
# run via `python start.py --venue both`.
CONTROL_PORT            = _env_int("CONTROL_PORT", 8150)

# When true the bot boots with state_pub.running=False — WS feeds + REST
# wire up and the dashboard populates, but the quote_loop sits idle until
# you POST /start (i.e. click "Start" on the dashboard). Default off.
START_PAUSED            = _env_bool("START_PAUSED", False)

DATA_DIR   = Path("data")
TRADES_CSV = DATA_DIR / "wti_trades.csv"   # name kept for dashboard compat


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


# Per-(market, level_slot) we track the resting client_order_id plus the
# quoted price + last-place timestamp so the quote loop can detect drift.
# level_slot encoding: bids occupy 1..LEVELS_PER_SIDE, asks LEVELS_PER_SIDE+1..2N.
@dataclass
class LevelState:
    coid: int             # client_order_id currently resting
    is_buy: bool
    price_ticks: int      # Rise's integer-tick price
    size_steps: int       # Rise's integer-step size
    placed_at_ms: int


@dataclass
class PairState:
    bbo: Bbo = field(default_factory=Bbo)
    position: float = 0.0       # signed base-asset position (from WS)
    avg_entry: float = 0.0
    realized_pnl: float = 0.0   # cumulative; updated from trades channel
    unrealized_pnl: float = 0.0
    position_value: float = 0.0
    levels: dict[int, LevelState] = field(default_factory=dict)
    cap_warned: bool = False
    halt_reason: str | None = None
    # Per-pair fill counters used by the b/s imbalance halt.
    fills_buy: int = 0
    fills_sell: int = 0


@dataclass
class State:
    pairs: dict[str, PairState] = field(default_factory=dict)
    # Monotonic counter for unique client_order_id values; same encoding
    # scheme as the Lighter port — see make_client_order_id().
    coid_counter: int = 1
    # client_order_id -> Rise composite order_id (hex string). Lighter
    # used int order_index; Rise uses a 24-byte composite hex string.
    # cancel_order needs the composite, populated by consume_orders WS.
    coid_to_oid: dict[int, str] = field(default_factory=dict)
    # Every coid we've ever placed (until we know it's gone via the WS).
    placed_coids: dict[int, float] = field(default_factory=dict)
    # Queue of (market_id, coid, order_id) tuples that orphan_cleanup_loop
    # will cancel. Populated by consume_orders when it spots an OPEN order
    # we placed but no longer track locally.
    orphans_to_cancel: list[tuple[int, int, str]] = field(default_factory=list)
    # Shared throttle gate for signed ops (place + cancel).
    last_signed_op_ts: float = 0.0
    current_throttle_sec: float = 0.0
    consecutive_signed_ok: int = 0
    last_throttle_bump_log_ts: float = 0.0
    session_start_equity: float | None = None
    current_equity: float | None = None
    halt_reason: str | None = None
    halt_detail: str = ""
    size_too_small_warned: dict[str, bool] = field(default_factory=dict)
    # Per-pair realized PnL captured at launch so the dashboard's
    # `realized_session` row doesn't include lifetime PnL.
    realized_baseline: dict[str, float] = field(default_factory=dict)
    realized_baseline_armed: bool = False


# ---- WS consumers --------------------------------------------------------

async def consume_order_books(st: State, metas: dict[str, MarketMeta],
                                stop: asyncio.Event, log: logging.Logger):
    """Subscribe to the public orderbook channel for all configured markets;
    maintain top-of-book per pair.

    Rise sends prices+sizes as wei strings (18 decimals); we convert to
    floats once per update. The server throttles to 4 updates/sec/market.
    Deletes arrive as quantity="0".
    """
    id_to_sym = {m.market_id: m.symbol for m in metas.values()}
    market_ids = list(id_to_sym.keys())
    # Local mirrored book per market_id; price-string -> qty-float so
    # incremental updates can replace or delete a level by price match.
    books: dict[int, dict[str, dict[str, float]]] = {
        mid: {"bids": {}, "asks": {}} for mid in id_to_sym
    }

    def apply_levels(mid: int, side: str, levels: list[dict]):
        bk = books[mid][side]
        for lv in levels:
            px = lv.get("price")
            qty_str = lv.get("quantity") or lv.get("size") or "0"
            if px is None: continue
            # Rise mainnet sends DECIMAL strings on the WS feeds
            # ("61510.6", "1.234"), not wei-encoded integers as the
            # docs originally suggested. Both deletion sentinels "0"
            # and "0.0" should clear the level.
            try:
                qty_f = float(qty_str)
            except (TypeError, ValueError):
                continue
            if qty_f <= 0:
                bk.pop(px, None)
            else:
                bk[px] = qty_f

    def update_bbo(mid: int):
        sym = id_to_sym.get(mid)
        if sym is None: return
        bk = books[mid]
        if not bk["bids"] or not bk["asks"]: return
        # Compare in float space (decimal strings can't be sorted
        # lexicographically — "9.99" > "10.01" textually).
        try:
            best_bid_px_str = max(bk["bids"].keys(), key=lambda s: float(s))
            best_ask_px_str = min(bk["asks"].keys(), key=lambda s: float(s))
            b = st.pairs[sym].bbo
            b.bid_px = float(best_bid_px_str)
            b.ask_px = float(best_ask_px_str)
            b.updated_ms = int(time.time() * 1000)
        except (TypeError, ValueError):
            return

    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL,
                                            ping_interval=WS_PING_INTERVAL_SEC,
                                            ping_timeout=WS_PING_TIMEOUT_SEC,
                                            close_timeout=2,
                                            max_queue=128) as ws:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {"channel": "orderbook", "market_ids": market_ids},
                }))
                log.info(f"orderbook WS subscribed to {len(market_ids)} markets")
                async for raw in ws:
                    if stop.is_set(): break
                    msg = json.loads(raw)
                    # Rise also sends raw ping frames at the protocol level;
                    # also respond to app-level pings just in case.
                    if msg.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    if msg.get("channel") != "orderbook":
                        continue
                    mid_str = msg.get("market_id") or (msg.get("data") or {}).get("market_id")
                    if mid_str is None: continue
                    mid = int(mid_str)
                    if mid not in books: continue
                    data = msg.get("data") or {}
                    if msg.get("type") == "snapshot":
                        books[mid]["bids"].clear()
                        books[mid]["asks"].clear()
                    apply_levels(mid, "bids", data.get("bids") or [])
                    apply_levels(mid, "asks", data.get("asks") or [])
                    update_bbo(mid)
        except Exception as exc:  # noqa: BLE001
            log.warning(f"orderbook WS: {type(exc).__name__}: {exc} — reconnect 3s")
            for mid in books:
                books[mid] = {"bids": {}, "asks": {}}
            await asyncio.sleep(3)


async def consume_orders(st: State, signer: RiseSigner,
                            stop: asyncio.Event, log: logging.Logger):
    """Maintain st.coid_to_oid via the `orders` channel.

    Rise's order status enum is just OPEN / FILLED / CANCELLED. We treat
    OPEN as the tracking state and the rest as terminal (purge from the
    map and from any matching ps.levels slot).

    Also runs ORPHAN DETECTION: any OPEN order whose coid we placed but
    which is no longer in any ps.levels slot gets queued for cancel by
    orphan_cleanup_loop.
    """
    OPEN_STATUS  = "ORDER_STATUS_OPEN"
    TERMINAL     = {"ORDER_STATUS_FILLED", "ORDER_STATUS_CANCELLED"}

    def apply(orders_payload):
        # orders_payload is either a list of order objects (single market)
        # or a dict-of-list (multi-market). Normalise both shapes.
        if isinstance(orders_payload, list):
            orders_iter = orders_payload
        elif isinstance(orders_payload, dict):
            orders_iter = []
            for _, lst in orders_payload.items():
                if isinstance(lst, list):
                    orders_iter.extend(lst)
        else:
            return

        tracked_coids: set[int] = set()
        coid_to_slot: dict[int, tuple[str, int]] = {}
        for sym_, ps_ in st.pairs.items():
            for slot_, lvl_ in ps_.levels.items():
                tracked_coids.add(lvl_.coid)
                coid_to_slot[lvl_.coid] = (sym_, slot_)
        already_enqueued = {coid for (_mid, coid, _oid) in st.orphans_to_cancel}

        for o in orders_iter:
            try:
                # `client_order_id` on Rise wire is a string; coerce to int.
                coid = int(o.get("client_order_id") or 0)
                market_id = int(o.get("market_id") or 0)
            except (TypeError, ValueError):
                continue
            if coid == 0:
                continue
            order_id = o.get("id") or o.get("order_id") or ""
            status   = str(o.get("status") or "").upper()
            cancel_req = bool(o.get("cancel_requested", False))

            if status == OPEN_STATUS and not cancel_req:
                if order_id:
                    st.coid_to_oid[coid] = order_id
                if (coid not in tracked_coids
                        and coid in st.placed_coids
                        and coid not in already_enqueued
                        and order_id):
                    st.orphans_to_cancel.append((market_id, coid, order_id))
                    already_enqueued.add(coid)
            elif status in TERMINAL or cancel_req:
                st.coid_to_oid.pop(coid, None)
                st.placed_coids.pop(coid, None)
                slot_info = coid_to_slot.get(coid)
                if slot_info is not None:
                    sym_done, slot_done = slot_info
                    ps_done = st.pairs.get(sym_done)
                    if (ps_done is not None
                            and ps_done.levels.get(slot_done) is not None
                            and ps_done.levels[slot_done].coid == coid):
                        ps_done.levels.pop(slot_done, None)

    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL,
                                            ping_interval=WS_PING_INTERVAL_SEC,
                                            ping_timeout=WS_PING_TIMEOUT_SEC,
                                            close_timeout=2,
                                            max_queue=128) as ws:
                if not await ws_authenticate(ws, signer, log):
                    await asyncio.sleep(3)
                    continue
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {
                        "channel": "orders",
                        "makers":  [signer.account_address],
                    },
                }))
                log.info("orders WS subscribed")
                async for raw in ws:
                    if stop.is_set(): break
                    msg = json.loads(raw)
                    if msg.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    if msg.get("channel") != "orders":
                        continue
                    if msg.get("type") in ("snapshot", "update"):
                        apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"orders WS: {type(exc).__name__}: {exc} — reconnect 3s")
            await asyncio.sleep(3)


async def consume_positions(st: State, metas: dict[str, MarketMeta],
                             signer: RiseSigner, stop: asyncio.Event,
                             log: logging.Logger):
    """Maintain per-pair signed position + avg_entry via account_all_positions.

    Rise's `positions` channel sends size (wei), avg_entry_price (wei),
    quote_amount (wei), and side. There is NO realized_pnl field — that
    comes from the `trades` channel (see consume_trades). Unrealized PnL
    is derived locally from position * (mid - avg_entry).
    """
    id_to_sym = {m.market_id: m.symbol for m in metas.values()}
    market_ids = list(id_to_sym.keys())

    def apply(rows):
        if not isinstance(rows, list):
            return
        for p in rows:
            try:
                mid = int(p.get("market_id") or 0)
            except (TypeError, ValueError):
                continue
            sym = id_to_sym.get(mid)
            if sym is None: continue
            ps = st.pairs.get(sym)
            if ps is None: continue
            # Rise mainnet positions stream uses decimal strings, same
            # as the orderbook feed — not wei. side can be a string
            # (BUY/SELL) or integer enum (0=long, 1=short) depending on
            # endpoint; handle both.
            def _f(x) -> float:
                try: return float(x)
                except (TypeError, ValueError): return 0.0
            sz_raw   = p.get("size", "0")
            side_raw = p.get("side", "")
            avg_raw  = p.get("avg_entry_price", "0")
            quote_raw = p.get("quote_amount", "0")
            sz = _f(sz_raw)
            if isinstance(side_raw, int):
                sign = 1.0 if side_raw == 0 else -1.0
            else:
                s = str(side_raw).upper()
                sign = -1.0 if s == "SELL" else 1.0 if s == "BUY" else 0.0
            new_pos = sign * sz
            ps.position    = new_pos
            ps.avg_entry   = _f(avg_raw)
            ps.position_value = _f(quote_raw)
            # Recompute unrealized from live mid when we have both.
            if ps.bbo.mid is not None and new_pos != 0 and ps.avg_entry > 0:
                ps.unrealized_pnl = new_pos * (ps.bbo.mid - ps.avg_entry)

    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL,
                                            ping_interval=WS_PING_INTERVAL_SEC,
                                            ping_timeout=WS_PING_TIMEOUT_SEC,
                                            close_timeout=2,
                                            max_queue=64) as ws:
                if not await ws_authenticate(ws, signer, log):
                    await asyncio.sleep(3)
                    continue
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {
                        "channel":    "positions",
                        "makers":     [signer.account_address],
                        "market_ids": market_ids,
                    },
                }))
                log.info("positions WS subscribed")
                async for raw in ws:
                    if stop.is_set(): break
                    msg = json.loads(raw)
                    if msg.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    if msg.get("channel") != "positions":
                        continue
                    if msg.get("type") in ("snapshot", "update"):
                        apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"positions WS: {type(exc).__name__}: {exc} — reconnect 3s")
            await asyncio.sleep(3)


async def consume_trades(st: State, metas: dict[str, MarketMeta],
                          signer: RiseSigner, stop: asyncio.Event,
                          log: logging.Logger):
    """Accumulate per-pair realized PnL + fill counters via the `trades` feed.

    Rise's trades channel emits per fill: `realized_pnl` (set only when a
    position was reduced), `liquidity_indicator` (MAKER/TAKER), `side`,
    `price`, `size`. We sum realized_pnl into ps.realized_pnl and bump
    fills_buy / fills_sell so the per-pair imbalance halt can fire.
    """
    id_to_sym = {m.market_id: m.symbol for m in metas.values()}
    market_ids = list(id_to_sym.keys())

    def apply(rows):
        if not isinstance(rows, list):
            rows = [rows] if isinstance(rows, dict) else []
        for t in rows:
            try:
                mid = int(t.get("market_id") or 0)
            except (TypeError, ValueError):
                continue
            sym = id_to_sym.get(mid)
            if sym is None: continue
            ps = st.pairs.get(sym)
            if ps is None: continue
            side = str(t.get("side") or "").upper()
            if side == "BUY":  ps.fills_buy  += 1
            if side == "SELL": ps.fills_sell += 1
            rp = t.get("realized_pnl")
            if rp:
                try: ps.realized_pnl += float(rp)
                except (TypeError, ValueError): pass

            # Append to CSV for the dashboard's volume / per-fill widgets.
            price = t.get("price")
            size  = t.get("size")
            if price and size:
                try:
                    _csv_trade_log(sym, side, float(size), float(price), "ok")
                except (TypeError, ValueError):
                    pass
            log.info(
                f"{sym} FILL {side} {size}@{price} "
                f"liq={t.get('liquidity_indicator')} rp={rp}"
            )

    while not stop.is_set():
        try:
            async with websockets.connect(WS_URL,
                                            ping_interval=WS_PING_INTERVAL_SEC,
                                            ping_timeout=WS_PING_TIMEOUT_SEC,
                                            close_timeout=2,
                                            max_queue=128) as ws:
                if not await ws_authenticate(ws, signer, log):
                    await asyncio.sleep(3)
                    continue
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {
                        "channel":    "trades",
                        "market_ids": market_ids,
                    },
                }))
                log.info("trades WS subscribed")
                async for raw in ws:
                    if stop.is_set(): break
                    msg = json.loads(raw)
                    if msg.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    if msg.get("channel") != "trades":
                        continue
                    if msg.get("type") in ("snapshot", "update"):
                        apply(msg.get("data") or [])
        except Exception as exc:  # noqa: BLE001
            log.warning(f"trades WS: {type(exc).__name__}: {exc} — reconnect 3s")
            await asyncio.sleep(3)


async def orphan_cleanup_loop(st: State, rest: RiseRest | None,
                                metas: dict[str, MarketMeta],
                                stop: asyncio.Event, log: logging.Logger):
    """Cancel orders we PLACED but no longer TRACK locally."""
    if rest is None:
        return
    id_to_meta = {m.market_id: m for m in metas.values()}
    last_gc_ts = time.time()
    while not stop.is_set():
        await asyncio.sleep(ORPHAN_SCAN_INTERVAL_SEC)

        drained = 0
        while st.orphans_to_cancel and drained < ORPHAN_DRAIN_PER_PASS \
                and not stop.is_set():
            market_id, coid, oid = st.orphans_to_cancel.pop(0)
            meta = id_to_meta.get(market_id)
            if meta is None:
                continue
            # Re-check tracked — a fresh placement could have re-claimed it.
            tracked = False
            for ps in st.pairs.values():
                if any(lvl.coid == coid for lvl in ps.levels.values()):
                    tracked = True
                    break
            if tracked:
                continue
            log.warning(
                f"orphan cancel: {meta.symbol} coid={coid} oid={oid}"
            )
            await _throttle_signed_op(st)
            try:
                await rest.cancel_order(market_id=market_id, order_id=oid)
            except RiseRestError as exc:
                if exc.is_rate_limited:
                    _on_rate_limit(st, log, f"orphan cancel {meta.symbol}")
                log.warning(f"orphan cancel raised: {exc}")
            except Exception as exc:  # noqa: BLE001
                log.warning(f"orphan cancel raised: {type(exc).__name__}: {exc}")
            else:
                _on_signed_ok(st, log)
                st.coid_to_oid.pop(coid, None)
                st.placed_coids.pop(coid, None)
            drained += 1

        now = time.time()
        if now - last_gc_ts > 60.0:
            last_gc_ts = now
            cutoff = now - ORPHAN_PLACED_COID_TTL_SEC
            stale = [c for c, ts in st.placed_coids.items() if ts < cutoff]
            for c in stale:
                st.placed_coids.pop(c, None)
            if stale:
                log.info(f"orphan cleanup: GC'd {len(stale)} stale placed_coids")


async def rest_orphan_sweep_loop(st: State,
                                   metas: dict[str, MarketMeta],
                                   rest: RiseRest,
                                   stop: asyncio.Event, log: logging.Logger):
    """Periodic REST poll of open orders — bulletproof orphan detector.

    Heals coid_to_oid when the WS missed an OPEN push, and enqueues any
    OPEN order we placed but no longer track locally for cancel.
    """
    while not stop.is_set():
        await asyncio.sleep(REST_SWEEP_INTERVAL_SEC)
        tracked_coids: set[int] = set()
        for ps in st.pairs.values():
            for lvl in ps.levels.values():
                tracked_coids.add(lvl.coid)
        already_enqueued = {coid for (_mid, coid, _oid) in st.orphans_to_cancel}

        new_orphans = 0
        healed = 0
        for sym, meta in metas.items():
            if stop.is_set(): break
            try:
                resp = await asyncio.wait_for(
                    rest.get_open_orders(account=ACCOUNT_ADDRESS,
                                          market_id=meta.market_id),
                    timeout=REST_SWEEP_TIMEOUT_SEC,
                )
            except Exception as exc:  # noqa: BLE001
                log.warning(f"REST sweep {sym} failed: "
                            f"{type(exc).__name__}: {exc}")
                continue
            for o in (resp.get("orders") or []):
                try:
                    coid = int(o.get("client_order_id") or 0)
                except (TypeError, ValueError):
                    continue
                oid = o.get("id") or o.get("order_id") or ""
                if coid == 0 or not oid:
                    continue
                if coid not in st.coid_to_oid:
                    st.coid_to_oid[coid] = oid
                    healed += 1
                if (coid not in tracked_coids
                        and coid in st.placed_coids
                        and coid not in already_enqueued):
                    st.orphans_to_cancel.append((meta.market_id, coid, oid))
                    already_enqueued.add(coid)
                    new_orphans += 1

        if new_orphans or healed:
            log.info(
                f"REST sweep: +{new_orphans} orphans enqueued, "
                f"+{healed} coid->oid mappings healed, "
                f"queue_size={len(st.orphans_to_cancel)}"
            )


# ---- order helpers -------------------------------------------------------

def _make_coid(st: State, market_idx: int, slot: int) -> int:
    st.coid_counter = (st.coid_counter + 1) & 0xFFFFFFFF
    if st.coid_counter == 0:
        st.coid_counter = 1
    return make_client_order_id(st.coid_counter, market_idx, slot)


def _level_slot(level_idx: int, is_buy: bool) -> int:
    # N (levels_per_side) read live so changing it on the fly is safe;
    # see the Ondo bot's docstring for the cancel-replace transition cost.
    n = _live_int("levels_per_side", LEVELS_PER_SIDE)
    return level_idx if is_buy else (n + level_idx)


def _to_ticks(price: float, step_price: Decimal) -> int:
    return max(1, int((Decimal(str(price)) / step_price).to_integral_value()))


def _to_steps(size: float, step_size: Decimal) -> int:
    base = int((Decimal(str(size)) / step_size).to_integral_value())
    return max(0, base)


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


async def place_order(rest: RiseRest | None, meta: MarketMeta,
                       coid: int, size_steps: int, price_ticks: int,
                       is_buy: bool, st: State,
                       log: logging.Logger) -> tuple[bool, str, str]:
    """Submit a POST_ONLY limit order. Returns (ok, info, order_id).

    Hash is computed over (size_steps, price_ticks) per risex-client's
    encodeOrder. client_order_id is passed through so the contract can
    bind the signature to it (also enables V3_FLAG_CLIENT_ID).
    """
    if rest is None:
        return True, "dry_run", ""
    try:
        result = await rest.place_order(
            market_id=meta.market_id,
            side=0 if is_buy else 1,
            size_steps=size_steps,
            price_ticks=price_ticks,
            post_only=True,
            order_type=1,        # Limit
            time_in_force=0,     # GTC
            stp_mode=0,          # ExpireMaker
            ttl_units=0,
            client_order_id=coid,
        )
    except RiseRestError as exc:
        if exc.is_rate_limited:
            _on_rate_limit(st, log, f"place {meta.symbol}")
        return False, str(exc), ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}", ""
    _on_signed_ok(st, log)
    # Response shape: {"data": {"order_id": "0x...", ...}}
    data = result.get("data") or result
    return True, "placed", data.get("order_id", "")


async def cancel_one(rest: RiseRest, meta: MarketMeta, coid: int,
                      st: State, log: logging.Logger) -> bool:
    oid = st.coid_to_oid.get(coid)
    if oid is None:
        for _ in range(2):
            await asyncio.sleep(0.1)
            oid = st.coid_to_oid.get(coid)
            if oid is not None:
                break
    if oid is None:
        log.warning(
            f"{meta.symbol} cancel skipped: coid={coid} has no order_id yet "
            f"(WS not pushed). Will retry next reconcile or drop as phantom "
            f"after {UNCONFIRMED_TIMEOUT_SEC}s."
        )
        return False
    await _throttle_signed_op(st)
    try:
        await rest.cancel_order(market_id=meta.market_id, order_id=oid)
    except RiseRestError as exc:
        if exc.is_rate_limited:
            _on_rate_limit(st, log, f"cancel {meta.symbol}")
        log.warning(f"{meta.symbol} cancel coid={coid} oid={oid} raised: {exc}")
        return False
    except Exception as exc:  # noqa: BLE001
        log.warning(f"{meta.symbol} cancel coid={coid} oid={oid} raised: "
                    f"{type(exc).__name__}: {exc}")
        return False
    _on_signed_ok(st, log)
    st.coid_to_oid.pop(coid, None)
    return True


async def cancel_all_orders_safe(rest: RiseRest, metas: dict[str, MarketMeta],
                                   log: logging.Logger) -> None:
    """Per-market cancel-all (Rise cancel-all is per-market, not global).

    Rise's cancel-all endpoint may reject server-signing permits with
    'signature is required' — server-mode appears to only work for
    place_order. Per-market cancellation of individual orders during
    the normal reconcile loop still works because we cancel by orderId
    via the place_order signer path (or fall back to leaving orphans
    for the REST sweep). So this startup mass-cancel is best-effort:
    log once at INFO if it's the known server-mode error, never WARN.
    """
    known_server_mode = "signature is required"
    first_signature_warned = False
    for sym, meta in metas.items():
        try:
            await rest.cancel_all_orders(meta.market_id)
        except RiseRestError as exc:
            msg = str(exc)
            if known_server_mode in msg:
                if not first_signature_warned:
                    log.info(
                        "cancel_all: skipping mass-cancel — "
                        "Rise rejected server-mode permit "
                        "(needs client-side EIP-712). Bot proceeds normally; "
                        "individual cancels during reconcile still work."
                    )
                    first_signature_warned = True
            else:
                log.warning(f"cancel_all {sym}: {exc}")
        except Exception as exc:  # noqa: BLE001
            log.warning(f"cancel_all {sym}: {type(exc).__name__}: {exc}")


# ---- ladder logic --------------------------------------------------------

def desired_orders(meta: MarketMeta, ps: PairState, exit_only: bool = False,
                    ) -> list[tuple[int, bool, int, int, float]]:
    """Return list of (level_slot, is_buy, price_ticks, size_steps, price_dec)."""
    mid = ps.bbo.mid
    if mid is None or mid <= 0:
        return []
    order_size_usd = _live_float("order_size_usd",        ORDER_SIZE_USD)
    grid_step_bp   = _live_float("grid_step_bp",          GRID_STEP_BP)
    cap_per_pair   = _live_float("max_notional_per_pair", MAX_NOTIONAL_PER_PAIR_USD)
    size_dec = order_size_usd / mid
    steps = _to_steps(size_dec, meta.step_size)
    if steps <= 0:
        return []
    snapped_size_dec = float(Decimal(steps) * meta.step_size)
    if snapped_size_dec < meta.min_order_size:
        # bump to min_order_size if our target rounded below it.
        steps = _to_steps(meta.min_order_size * 1.05, meta.step_size)
        snapped_size_dec = float(Decimal(steps) * meta.step_size)

    out: list[tuple[int, bool, int, int, float]] = []
    cap_base = cap_per_pair / mid
    n_levels = _live_int("levels_per_side", LEVELS_PER_SIDE)
    for k in range(1, n_levels + 1):
        bp = k * grid_step_bp
        bid_px = mid * (1 - bp / 10_000.0)
        ask_px = mid * (1 + bp / 10_000.0)
        if ps.bbo.ask_px is not None and bid_px >= ps.bbo.ask_px:
            bid_px = ps.bbo.ask_px * (1 - 1e-4)
        if ps.bbo.bid_px is not None and ask_px <= ps.bbo.bid_px:
            ask_px = ps.bbo.bid_px * (1 + 1e-4)

        bid_projected = ps.position + k * snapped_size_dec
        ask_projected = ps.position - k * snapped_size_dec
        bid_ok = bid_projected <= cap_base
        ask_ok = ask_projected >= -cap_base
        if exit_only:
            if ps.position > 0:
                bid_ok = False
            elif ps.position < 0:
                ask_ok = False
            else:
                bid_ok = ask_ok = False

        if bid_ok:
            out.append((_level_slot(k, True), True,
                        _to_ticks(bid_px, meta.step_price),
                        steps, bid_px))
        if ask_ok:
            out.append((_level_slot(k, False), False,
                        _to_ticks(ask_px, meta.step_price),
                        steps, ask_px))
    return out


def total_notional_usd(st: State) -> float:
    return sum(abs(ps.position_value) for ps in st.pairs.values())


ASSET_CLASS_BY_SYMBOL: dict[str, str] = {
    "BTC": "crypto", "ETH": "crypto", "SOL": "crypto",
    "HYPE": "crypto", "ZEC": "crypto",
    "XAU": "commodity", "XAG": "commodity", "XCU": "commodity",
    "NATGAS": "commodity", "WTI": "commodity", "BRENTOIL": "commodity",
}


def classify_market(symbol: str) -> str:
    return ASSET_CLASS_BY_SYMBOL.get(symbol.upper(), "equity")


def compute_directional_delta(st: State) -> dict[str, Any]:
    out: dict[str, Any] = {
        "crypto": 0.0, "equity": 0.0, "commodity": 0.0,
        "total": 0.0, "by_market": {},
    }
    for sym, ps in st.pairs.items():
        mid = ps.bbo.mid
        if mid is None or mid <= 0 or ps.position == 0:
            out["by_market"][sym] = 0.0
            continue
        delta = ps.position * mid
        cls = classify_market(sym)
        out[cls] = out.get(cls, 0.0) + delta
        out["total"] += delta
        out["by_market"][sym] = delta
    for k in ("crypto", "equity", "commodity", "total"):
        out[k] = round(out[k], 2)
    out["by_market"] = {k: round(v, 2) for k, v in out["by_market"].items()}
    return out


def evaluate_pair_halt(ps: PairState, st: State, now_ms: int,
                        sym: str) -> str | None:
    if ps.bbo.mid is None:
        return "no_book"
    if (now_ms - ps.bbo.updated_ms) > MAX_FV_AGE_SEC * 1000:
        return "book_stale"
    if ps.halt_reason and ps.halt_reason.startswith(("pair_stop_loss", "fill_imbalance")):
        return ps.halt_reason
    pair_stop_loss  = _live_float("pair_stop_loss_usd",       PAIR_STOP_LOSS_USD)
    imbalance_ratio = _live_float("fill_imbalance_ratio",     FILL_IMBALANCE_RATIO)
    imbalance_min   = _live_int  ("fill_imbalance_min_fills", FILL_IMBALANCE_MIN_FILLS)
    baseline = st.realized_baseline.get(sym, 0.0)
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
    cap_total = _live_float("max_total_notional",   MAX_TOTAL_NOTIONAL_USD)
    dd_limit  = _live_float("session_dd_limit_usd", SESSION_DD_LIMIT_USD)
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
ORPHAN_DRAIN_PER_PASS      = 10
REST_SWEEP_INTERVAL_SEC    = 180.0
REST_SWEEP_TIMEOUT_SEC     = 8.0


def _resolve_unconfirmed(st: State, ps: PairState, slot: int,
                          log: logging.Logger, sym: str) -> str:
    lvl = ps.levels.get(slot)
    if lvl is None:
        return 'ok'
    if lvl.coid in st.coid_to_oid:
        return 'ok'
    age_s = (time.time() * 1000 - lvl.placed_at_ms) / 1000.0
    if age_s < UNCONFIRMED_TIMEOUT_SEC:
        return 'wait'
    log.warning(
        f"{sym} slot={slot} coid={lvl.coid} unconfirmed for {age_s:.1f}s "
        f"— assuming venue rejected, removing local state"
    )
    return 'drop'


async def reconcile_pair(sym: str, ps: PairState, meta: MarketMeta,
                          rest: RiseRest | None, market_idx: int,
                          st: State, exit_only: bool,
                          log: logging.Logger) -> int:
    now_ms = int(time.time() * 1000)
    ph = evaluate_pair_halt(ps, st, now_ms, sym)
    if ph is not None:
        if ps.halt_reason != ph:
            log.info(f"{sym} pair-halt: {ph}")
            ps.halt_reason = ph
        if rest is not None and ps.levels:
            for slot, lvl in list(ps.levels.items()):
                state = _resolve_unconfirmed(st, ps, slot, log, sym)
                if state == 'wait': continue
                if state == 'drop':
                    ps.levels.pop(slot, None)
                    continue
                ok_c = await cancel_one(rest, meta, lvl.coid, st, log)
                if ok_c:
                    ps.levels.pop(slot, None)
        return 0
    if ps.halt_reason:
        log.info(f"{sym} resume (was: {ps.halt_reason})")
        ps.halt_reason = None

    desired = desired_orders(meta, ps, exit_only=exit_only)
    desired_by_slot = {d[0]: d for d in desired}

    # Cancel resting levels no longer in target.
    for slot in list(ps.levels.keys()):
        if slot in desired_by_slot: continue
        if rest is None:
            ps.levels.pop(slot, None)
            continue
        state = _resolve_unconfirmed(st, ps, slot, log, sym)
        if state == 'wait': continue
        if state == 'drop':
            ps.levels.pop(slot, None)
            continue
        ok_c = await cancel_one(rest, meta, ps.levels[slot].coid, st, log)
        if ok_c:
            ps.levels.pop(slot, None)

    placed = 0
    for slot, is_buy, price_ticks, size_steps, price_dec in desired:
        existing = ps.levels.get(slot)
        if existing is not None and rest is not None:
            state = _resolve_unconfirmed(st, ps, slot, log, sym)
            if state == 'wait': continue
            if state == 'drop':
                ps.levels.pop(slot, None)
                existing = None

        max_age   = _live_float("requote_max_age_sec", REQUOTE_MAX_AGE_SEC)
        drift_lim = _live_float("requote_drift_bp",    REQUOTE_DRIFT_BP)
        need_place = True
        if existing is not None:
            age_s = (time.time() * 1000 - existing.placed_at_ms) / 1000.0
            if existing.price_ticks == price_ticks and age_s < max_age:
                need_place = False
            elif existing.price_ticks > 0:
                drift_bp = abs(price_ticks - existing.price_ticks) \
                           / existing.price_ticks * 10_000.0
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
            rest, meta, coid, size_steps, price_ticks, is_buy, st, log,
        )
        if ok:
            now_ms_p = int(time.time() * 1000)
            ps.levels[slot] = LevelState(
                coid=coid, is_buy=is_buy,
                price_ticks=price_ticks, size_steps=size_steps,
                placed_at_ms=now_ms_p,
            )
            st.placed_coids[coid] = now_ms_p / 1000.0
            if order_id:
                st.coid_to_oid[coid] = order_id
            placed += 1
            log.info(
                f"{sym} {'BUY' if is_buy else 'SELL'} slot={slot} "
                f"px={price_dec:.6f} sz_steps={size_steps} coid={coid} "
                f"oid={order_id or '?'}"
            )
        else:
            log.warning(
                f"{sym} {'BUY' if is_buy else 'SELL'} slot={slot} "
                f"px={price_dec:.6f} FAILED: {info_msg}"
            )
    return placed


async def quote_loop(st: State, rest: RiseRest | None,
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

        for sym, ps in st.pairs.items():
            meta = metas.get(sym)
            if meta is None: continue
            positions_snap[sym] = ps.position
            if ps.bbo.bid_px is not None and ps.bbo.ask_px is not None:
                bbo_snap[sym] = {"bid": ps.bbo.bid_px, "ask": ps.bbo.ask_px}
            baseline = st.realized_baseline.get(sym, 0.0)
            live_unreal = ps.unrealized_pnl
            if ps.bbo.mid is not None and ps.position != 0 and ps.avg_entry > 0:
                live_unreal = ps.position * (ps.bbo.mid - ps.avg_entry)
            ex_pnl[sym] = {
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
                    sym, ps, meta, rest, market_idx.get(sym, 0),
                    st, exit_only, log,
                )
            except Exception as exc:  # noqa: BLE001
                log.error(f"{sym} reconcile raised: {type(exc).__name__}: {exc}")

        state_pub.tick(positions=positions_snap, bbo=bbo_snap)
        state_pub.exchange_pnl = ex_pnl
        state_pub.directional_delta = compute_directional_delta(st)


# ---- equity loop ---------------------------------------------------------

async def equity_loop(st: State, rest: RiseRest, state_pub: BotState,
                       log: logging.Logger, stop: asyncio.Event):
    while not stop.is_set():
        try:
            resp = await rest.get_portfolio(account=ACCOUNT_ADDRESS)
            summary = resp.get("summary") or {}
            try:
                total = float(summary.get("total_account_value") or 0)
                avail = float(summary.get("usdc_balance") or
                              summary.get("free_collateral") or 0)
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
        # Recompute expected from live levels_per_side.
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
        if expected > 0 and resting > expected * 1.5:
            log.warning(
                f"resting_orders ({resting}) is >50% above expected ({expected}). "
                f"Possible orphan accumulation — check REST sweep logs."
            )


# ---- close routine -------------------------------------------------------

async def close_all_positions(st: State, rest: RiseRest,
                                metas: dict[str, MarketMeta], log: logging.Logger):
    """Cancel every resting order (per-market), then market-flatten every
    non-zero position via IOC market orders with reduce_only=True."""
    await cancel_all_orders_safe(rest, metas, log)
    for ps in st.pairs.values():
        ps.levels.clear()
    await asyncio.sleep(0.5)

    for sym, ps in st.pairs.items():
        meta = metas.get(sym)
        if meta is None: continue
        if abs(ps.position) * (ps.bbo.mid or 0.0) < 1.0:
            continue
        size_steps = _to_steps(abs(ps.position), meta.step_size)
        if size_steps <= 0: continue
        is_buy = ps.position < 0   # short -> buy to close, long -> sell to close
        # Market order: order_type=0 (Rise enum: 0=Market), IOC, reduce_only=True.
        log.info(f"close {sym}: pos={ps.position:+g} -> "
                 f"{'BUY' if is_buy else 'SELL'} {size_steps} steps (reduce_only)")
        await _throttle_signed_op(st)
        try:
            await rest.place_order(
                market_id=meta.market_id,
                side=0 if is_buy else 1,
                size_steps=size_steps,
                price_ticks=0,         # ignored for market orders
                post_only=False,
                reduce_only=True,
                order_type=0,          # Market
                time_in_force=3,       # IOC
                stp_mode=0,
                ttl_units=0,
                client_order_id=_make_coid(st, 0, 0),
            )
        except RiseRestError as exc:
            log.error(f"close {sym} failed: {exc}")
        except Exception as exc:  # noqa: BLE001
            log.error(f"close {sym} raised: {type(exc).__name__}: {exc}")


# ---- main ----------------------------------------------------------------

async def _resolve_eip712_domain(rest: RiseRest, log: logging.Logger) -> EIP712Domain:
    """Build the EIP-712 domain. Prefer env overrides if all four are set;
    otherwise call /v1/auth/eip712-domain to fetch live values."""
    name      = os.environ.get("RISE_EIP712_NAME") or ""
    version   = os.environ.get("RISE_EIP712_VERSION") or ""
    chain_id  = int(os.environ.get("RISE_EIP712_CHAIN_ID") or 0)
    contract  = os.environ.get("RISE_EIP712_VERIFYING_CONTRACT") or ""
    if name and version and chain_id and contract and \
            contract != "0x0000000000000000000000000000000000000000":
        log.info("EIP-712 domain: using env values")
        return EIP712Domain(name=name, version=version,
                             chain_id=chain_id, verifying_contract=contract)
    log.info("EIP-712 domain: fetching from /v1/auth/eip712-domain")
    resp = await rest.get_eip712_domain()
    dom = resp.get("domain") or resp
    return EIP712Domain(
        name=str(dom.get("name") or "RiseXAuthorization"),
        version=str(dom.get("version") or "1"),
        chain_id=int(dom.get("chainId") or dom.get("chain_id") or 0),
        verifying_contract=str(dom.get("verifyingContract") or
                                 dom.get("verifying_contract") or ""),
    )


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
            logging.FileHandler(LOG_DIR / f"risex_grid_mm_{int(time.time())}.log",
                                  encoding="utf-8"),
        ],
    )
    log = logging.getLogger("risex_mm")

    symbols = [s.strip().upper() for s in PAIRS_INCLUDE_RAW.split(",") if s.strip()]
    if not symbols:
        log.error("PAIRS_INCLUDE is empty — nothing to trade. Set it in the dashboard.")
        return

    log.info(
        f"config: pairs={len(symbols)} order_size=${ORDER_SIZE_USD} "
        f"levels/side={LEVELS_PER_SIDE} step={GRID_STEP_BP}bp "
        f"max_per_pair=${MAX_NOTIONAL_PER_PAIR_USD} "
        f"max_total=${MAX_TOTAL_NOTIONAL_USD} "
        f"requote_drift={REQUOTE_DRIFT_BP}bp armed={TRADING}"
    )

    # Build a temporary signer (with placeholder domain) just to call the
    # markets + domain endpoints, then rebuild with the real domain.
    placeholder_domain = EIP712Domain(name="placeholder", version="1",
                                        chain_id=0,
                                        verifying_contract="0x0000000000000000000000000000000000000000")
    signer = RiseSigner(ACCOUNT_ADDRESS, SIGNER_ADDRESS,
                          SIGNER_PRIVATE_KEY, placeholder_domain)
    rest = RiseRest(REST_BASE_URL, signer)
    try:
        domain = await _resolve_eip712_domain(rest, log)
        signer.domain = domain
        log.info(f"EIP-712 domain: name={domain.name} chain={domain.chain_id} "
                 f"contract={domain.verifying_contract}")
        target = await rest.fetch_and_set_target()
        log.info(f"orders_manager target: {target}")

        markets_resp = await rest.get_markets()
        metas = parse_markets_response(markets_resp, set(symbols))
        missing = set(symbols) - set(metas.keys())
        if missing:
            log.warning(f"requested symbols not found/inactive: {sorted(missing)}")
        if not metas:
            log.error("no usable markets resolved — aborting")
            return
        log.info(
            "markets: "
            + ", ".join(
                f"{m.symbol}(id={m.market_id} sd={m.size_decimals} pd={m.price_decimals} "
                f"min={m.min_order_size})"
                for m in metas.values()
            )
        )

        st = State()
        # Seed the coid counter with current epoch-seconds so client IDs
        # never collide across restarts. Rise's docs don't say whether it
        # enforces coid uniqueness, but it's free insurance and matches
        # the Ondo bot's handling.
        st.coid_counter = int(time.time()) & 0xFFFFFFFF
        log.info(f"coid counter seeded at {st.coid_counter:#x}")
        for sym in metas:
            st.pairs[sym] = PairState()
        market_idx = {sym: i for i, sym in enumerate(sorted(metas.keys()))}

        expected_max_orders = len(metas) * LEVELS_PER_SIDE * 2
        log.info(
            f"expected max orders: {expected_max_orders} "
            f"({len(metas)} pairs × {LEVELS_PER_SIDE} levels × 2 sides)"
        )
        deploy_time = expected_max_orders * PLACE_THROTTLE_SEC
        if deploy_time > 180:
            log.warning(
                f"initial deployment will take ~{deploy_time:.0f}s "
                f"({expected_max_orders} orders × {PLACE_THROTTLE_SEC}s throttle)."
            )

        state_pub = BotState(
            venue="Rise",
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
                "deploy_time_est_sec":       round(deploy_time, 1),
            },
            positions={sym: 0.0 for sym in metas},
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
            if sig is None:
                continue
            try:
                loop.add_signal_handler(sig, lambda n=sig_name: _on_signal(n))
            except (NotImplementedError, AttributeError, RuntimeError):
                pass  # Windows / non-main-thread asyncio loops

        rest_for_orders = rest if TRADING else None

        control_task = asyncio.create_task(run_control_server(state_pub, CONTROL_PORT))
        tasks = [
            asyncio.create_task(consume_order_books(st, metas, stop, log)),
            asyncio.create_task(consume_orders(st, signer, stop, log)),
            asyncio.create_task(consume_positions(st, metas, signer, stop, log)),
            asyncio.create_task(consume_trades(st, metas, signer, stop, log)),
            asyncio.create_task(orphan_cleanup_loop(st, rest_for_orders, metas, stop, log)),
            asyncio.create_task(equity_loop(st, rest, state_pub, log, stop)),
            asyncio.create_task(status_loop(st, state_pub, log, stop)),
        ]
        if TRADING:
            tasks.append(asyncio.create_task(
                rest_orphan_sweep_loop(st, metas, rest, stop, log)
            ))

        # Wipe any leftover orders from earlier runs (per-market cancel-all).
        if TRADING:
            log.info("startup: cancel any pre-existing orders on this account")
            await cancel_all_orders_safe(rest, metas, log)
            await asyncio.sleep(0.5)

        log.info("waiting for initial book + position snapshot...")
        for _ in range(40):
            ready = sum(1 for ps in st.pairs.values() if ps.bbo.mid is not None)
            if ready >= max(1, len(st.pairs) // 2):
                break
            await asyncio.sleep(0.5)
        log.info(f"book ready on {ready}/{len(st.pairs)} pairs")

        st.realized_baseline = {sym: st.pairs[sym].realized_pnl for sym in st.pairs}
        st.realized_baseline_armed = True
        nz = {s: r for s, r in st.realized_baseline.items() if r}
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
                                await cancel_all_orders_safe(rest, metas, log)
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
                    await cancel_all_orders_safe(rest, metas, log)
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
