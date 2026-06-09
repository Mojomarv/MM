"""
HTTP control + status server for oil grid bots.

Each bot imports this module, constructs a `BotState`, mutates it in the main
loop, and spawns `run_control_server(state, port)` as a background task. The
Next.js frontend polls `/status` every 2s and POSTs `/start` or `/stop` on
user action.

Endpoints
---------
GET  /status  → current snapshot (running, positions, bbo, level hits, PnL)
POST /start   → set running=True   (bot resumes level changes + reconciliation)
POST /stop    → set running=False  (bot freezes; positions stay open — close manually)
GET  /health  → liveness probe

Auth
----
If env `BOT_API_KEY` is set, all endpoints require `X-API-Key: <same>` header.
If unset, no auth (ok for local dev / private network).

CORS
----
Permissive (`*`) — intended for local/private deployment. Tighten via
`CORS_ORIGIN` env var if exposing publicly.

PnL computation
---------------
Walks `wti_trades.csv` chronologically per market:
  * BUYs extend long / reduce short; SELLs do the inverse.
  * On reduction, realise (close_price - avg_entry) × reduced_size.
  * Extensions update avg_entry as volume-weighted average.
Current position × (live mid − avg_entry) is unrealised for the open portion.

Trades CSV rows without a `price` column (older schema) are skipped — PnL
starts accruing from the first fill logged after the price-column upgrade.
"""

from __future__ import annotations

import csv
import json
import os
import time
import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aiohttp import web


API_KEY     = os.environ.get("BOT_API_KEY", "")
CORS_ORIGIN = os.environ.get("CORS_ORIGIN", "*")


# ---------------------------------------------------------------------------
# State shared between the bot's main loop and the HTTP server.
# Only the main loop writes; the HTTP server reads.
# ---------------------------------------------------------------------------
@dataclass
class BotState:
    venue: str
    markets: list[str]
    data_dir: Path = field(default_factory=lambda: Path("data"))
    grid_params: dict[str, Any] = field(default_factory=dict)
    running: bool = True
    target_level: int = 0
    positions: dict[str, float] = field(default_factory=dict)
    last_bbo: dict[str, dict[str, float]] = field(default_factory=dict)
    last_update_ts: float = 0.0
    # Account balance (refreshed on a separate cadence by each bot).
    # Shape: {"total": float, "available": float, "currency": str,
    #         "updated_ts": float, "error": str | None}
    balance: dict[str, Any] = field(default_factory=dict)
    # Graceful close-positions request from the dashboard. The main
    # loop checks this each tick; when set, runs close_callback (if
    # provided) and exits. close_status moves "idle" -> "closing" ->
    # "done" / "error" so the dashboard can show progress.
    close_requested: bool = False
    close_status: str = "idle"
    close_error: str | None = None
    # When False, the bot's close handler cancels resting orders and exits
    # WITHOUT market-flattening positions. Used by the supervisor's
    # "stop without close" path so we still cancel orders cleanly even
    # when the operator wants to keep positions open. Default True
    # preserves legacy behavior (cancel + flatten) for bots that don't
    # explicitly read this flag.
    flatten_on_close: bool = True
    # Exchange-reported per-market PnL. When set, _build_status uses
    # this instead of FIFO-walking the CSV. Shape:
    #   {market_name: {position, avg_entry, unrealized_pnl,
    #                  realized_pnl, realized_session}}
    # session = realized_pnl - <baseline at bot launch>.
    exchange_pnl: dict[str, dict[str, float]] = field(default_factory=dict)
    # Directional delta (signed USD notional) bucketed by asset class.
    # Set by bots that classify their markets; surfaced verbatim in the
    # /status response so the dashboard can render long/short bias by
    # crypto / equity / commodity. Shape:
    #   {"crypto": float, "equity": float, "commodity": float,
    #    "total": float, "by_market": {sym: signed_usd, ...}}
    # Bots that don't publish this leave it empty and the dashboard
    # hides the tiles. Position-less or no-mid pairs contribute zero.
    directional_delta: dict[str, Any] = field(default_factory=dict)

    def tick(self, *, target_level: int | None = None,
             positions: dict[str, float] | None = None,
             bbo: dict[str, dict[str, float]] | None = None) -> None:
        if target_level is not None:
            self.target_level = target_level
        if positions is not None:
            self.positions = dict(positions)
        if bbo is not None:
            self.last_bbo.update(bbo)
        self.last_update_ts = time.time()

    def set_balance(self, *, total: float | None = None,
                     available: float | None = None,
                     currency: str = "USD",
                     error: str | None = None) -> None:
        self.balance = {
            "total": total,
            "available": available,
            "currency": currency,
            "updated_ts": time.time(),
            "error": error,
        }


# ---------------------------------------------------------------------------
# CSV readers
# ---------------------------------------------------------------------------
def _read_csv(path: Path, max_rows: int = 20000) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f))
        return rows[-max_rows:]
    except Exception:
        return []


def _level_hits(data_dir: Path) -> dict[str, int]:
    """Count confirmed level transitions per new_level value."""
    hits: dict[str, int] = {}
    for row in _read_csv(data_dir / "wti_levels.csv"):
        lvl = row.get("new_level")
        if lvl is None:
            continue
        hits[lvl] = hits.get(lvl, 0) + 1
    return dict(sorted(hits.items(), key=lambda kv: int(kv[0])))


def _recent_fills(data_dir: Path, limit: int = 20) -> list[dict[str, Any]]:
    """Last `limit` fills including `dry_run` so the dashboard shows strategy
    activity during paper-trading (TRADING=false)."""
    rows = _read_csv(data_dir / "wti_trades.csv")
    ok = [r for r in rows if r.get("outcome") in ("ok", "dry_run")]
    out: list[dict[str, Any]] = []
    for r in ok[-limit:]:
        out.append({
            "ts":      _as_float(r.get("ts")),
            "market":  r.get("market"),
            "side":    r.get("side"),
            "size":    _as_float(r.get("size")),
            "price":   _as_float(r.get("price")),
            "outcome": r.get("outcome"),
        })
    return out


def _as_float(x: Any) -> float | None:
    try:
        return float(x) if x not in (None, "") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# PnL computation — walks fills chronologically, tracks avg entry per market.
# ---------------------------------------------------------------------------
def _pnl_for_market(fills: list[dict[str, Any]], current_mid: float | None
                     ) -> dict[str, float | None]:
    """
    Returns {"realized", "unrealized", "position", "avg_entry", "skipped",
             "realized_session"}.
    realized_session is the realized PnL accrued since SESSION_START_TS by
    walking all fills FIFO and snapshotting the realized total just before
    the first session fill is processed; that way pre-session opens that
    are closed during the session correctly count toward session PnL.
    """
    position = 0.0
    avg_entry = 0.0
    realized = 0.0
    skipped = 0
    realized_at_session_start: float | None = None

    for f in fills:
        size  = f.get("size")
        price = f.get("price")
        side  = f.get("side")
        ts    = f.get("ts")
        if size is None or side is None:
            continue
        # Defensive: rows with missing or non-positive price (the close
        # routine had a bug where it logged price=0.0) would otherwise
        # corrupt the FIFO walk — every BUY at 0 inflates avg_entry
        # downward, every SELL at 0 produces a fake huge realized
        # gain/loss against the prior open. Treat them as skipped.
        if price is None or price <= 0:
            skipped += 1
            continue
        # Snapshot realized PnL just before processing the first fill that
        # belongs to the current session. session_realized = realized_now -
        # realized_at_session_start.
        if (realized_at_session_start is None
                and SESSION_START_TS > 0
                and ts is not None
                and ts >= SESSION_START_TS):
            realized_at_session_start = realized
        signed = size if side == "BUY" else -size

        if position == 0 or (position > 0) == (signed > 0):
            # Opening or extending in same direction: weighted-avg entry
            new_pos = position + signed
            if new_pos == 0:
                avg_entry = 0.0
            else:
                avg_entry = (avg_entry * position + price * signed) / new_pos
            position = new_pos
            continue

        # Reducing or flipping
        closing = min(abs(signed), abs(position))
        direction = 1 if position > 0 else -1
        realized += closing * (price - avg_entry) * direction

        if abs(signed) <= abs(position):
            position += signed
            if position == 0:
                avg_entry = 0.0
        else:
            # Flipping
            remaining = signed + position   # signed size of new open position
            position = remaining
            avg_entry = price

    unrealized = (position * (current_mid - avg_entry)
                  if position != 0 and current_mid is not None
                  else 0.0)
    # If a session is configured but no session fills happened yet,
    # session-realized = 0 (nothing closed during the session).
    if realized_at_session_start is None:
        realized_at_session_start = realized
    realized_session = realized - realized_at_session_start
    return {"realized": round(realized, 4),
            "unrealized": round(unrealized, 4),
            "realized_session": round(realized_session, 4),
            "position": round(position, 6),
            "avg_entry": round(avg_entry, 4) if position != 0 else None,
            "skipped": skipped}


def _pnl_all(data_dir: Path, markets: list[str],
             last_bbo: dict[str, dict[str, float]],
             include_dry_run: bool = False,
             ) -> dict[str, Any]:
    """Walk wti_trades.csv chronologically per market and report realized +
    unrealized PnL.

    `include_dry_run`:
        * False (default — armed mode): use only `outcome == "ok"` rows so dry-run
          fills from earlier paper sessions don't poison real PnL.
        * True (set by the bot when grid_params.trading is False): include
          `dry_run` rows so the dashboard shows simulated PnL on paper trades.
    """
    valid = {"ok", "dry_run"} if include_dry_run else {"ok"}
    rows = _read_csv(data_dir / "wti_trades.csv")
    ok = [r for r in rows if r.get("outcome") in valid]
    fills_by_market: dict[str, list[dict[str, Any]]] = {m: [] for m in markets}
    # Chronological — rely on append order (backed by monotonic ts column).
    for r in ok:
        m = r.get("market")
        if m not in fills_by_market:
            continue
        fills_by_market[m].append({
            "ts":    _as_float(r.get("ts")),
            "side":  r.get("side"),
            "size":  _as_float(r.get("size")),
            "price": _as_float(r.get("price")),
        })

    per_market: dict[str, dict[str, float | None]] = {}
    total_realized = 0.0
    total_unrealized = 0.0
    total_realized_session = 0.0
    total_skipped = 0
    for m in markets:
        bbo = last_bbo.get(m) or {}
        mid = None
        if bbo.get("bid") is not None and bbo.get("ask") is not None:
            mid = (bbo["bid"] + bbo["ask"]) / 2
        res = _pnl_for_market(fills_by_market[m], mid)
        per_market[m] = res
        total_realized         += res["realized"]         or 0.0
        total_unrealized       += res["unrealized"]       or 0.0
        total_realized_session += res.get("realized_session") or 0.0
        total_skipped          += res.get("skipped", 0)

    return {
        "per_market":             per_market,
        "realized_total":         round(total_realized,         4),
        "unrealized_total":       round(total_unrealized,       4),
        "realized_session_total": round(total_realized_session, 4),
        "session_start_ts":       SESSION_START_TS or None,
        "skipped_rows":           total_skipped,
        "mode":                   "dry_run+ok" if include_dry_run else "ok_only",
    }


# ---------------------------------------------------------------------------
# Total volume — sum of |size × price| across fills. Only counts real fills
# regardless of mode (dry_run fills have no exchange impact, so they shouldn't
# inflate the venue's notional volume figure). Also produces a 7-day rolling
# window so the dashboard can show "lifetime" alongside "recent" activity.
# ---------------------------------------------------------------------------
_SEVEN_DAY_SEC = 7 * 24 * 3600

# Per-process session start. Supervisor injects this on spawn so the bot
# can report "since this run started" volume/PnL alongside lifetime
# numbers. 0 means "not provided" — session fields fall back to 0.
SESSION_START_TS = float(os.environ.get("SESSION_START_TS", "0") or "0")


def _volume_all(data_dir: Path, markets: list[str]) -> dict[str, Any]:
    rows = _read_csv(data_dir / "wti_trades.csv")
    per_market: dict[str, dict[str, float | int]] = {
        m: {"volume": 0.0, "fills": 0,
            "volume_7d": 0.0, "fills_7d": 0,
            "volume_session": 0.0, "fills_session": 0}
        for m in markets
    }
    total_volume = total_volume_7d = total_volume_session = 0.0
    total_fills  = total_fills_7d  = total_fills_session  = 0
    cutoff_7d = time.time() - _SEVEN_DAY_SEC
    for r in rows:
        if r.get("outcome") != "ok":
            continue
        m = r.get("market")
        if m not in per_market:
            continue
        sz = _as_float(r.get("size")) or 0.0
        px = _as_float(r.get("price")) or 0.0
        # Skip rows that lack a usable price (legacy 0.0 entries from a
        # close-routine bug, mostly). Counting them would inflate
        # fill-count without contributing notional.
        if px <= 0 or sz == 0:
            continue
        notional = abs(sz) * px
        per_market[m]["volume"] += notional
        per_market[m]["fills"]  += 1
        total_volume += notional
        total_fills  += 1
        ts = _as_float(r.get("ts"))
        if ts is not None and ts >= cutoff_7d:
            per_market[m]["volume_7d"] += notional
            per_market[m]["fills_7d"]  += 1
            total_volume_7d += notional
            total_fills_7d  += 1
        if SESSION_START_TS > 0 and ts is not None and ts >= SESSION_START_TS:
            per_market[m]["volume_session"] += notional
            per_market[m]["fills_session"]  += 1
            total_volume_session += notional
            total_fills_session  += 1
    for m in per_market:
        for k in ("volume", "volume_7d", "volume_session"):
            per_market[m][k] = round(per_market[m][k], 2)
    return {
        "per_market":           per_market,
        "total_volume":         round(total_volume,         2),
        "total_fills":          total_fills,
        "total_volume_7d":      round(total_volume_7d,      2),
        "total_fills_7d":       total_fills_7d,
        "total_volume_session": round(total_volume_session, 2),
        "total_fills_session":  total_fills_session,
        "session_start_ts":     SESSION_START_TS or None,
    }


# ---------------------------------------------------------------------------
# Account-level stats — computed straight from the CSV in an account
# folder, so the supervisor can serve them whether or not a bot is
# currently running. Mirrors what _build_status returns for live bots
# but doesn't need a BotState. Used by the dashboard's "By venue" cards
# so stats persist across bot restarts.
# ---------------------------------------------------------------------------
def compute_session_stats(account_folder: Path,
                           since_ts: float,
                           until_ts: float | None = None) -> dict[str, Any]:
    """Stats restricted to a time window — used by the bot-history
    feature to capture a single bot run's contribution to the
    account's CSV. since_ts is inclusive; until_ts is exclusive (None
    = up to now). FIFO realized PnL is walked over only the windowed
    fills, treating the window as starting from a flat position
    (i.e. attributes pre-window opens that were closed in-window
    as session realized — the right answer when a bot inherits a
    position; the wrong answer when it didn't, but conservative)."""
    candidates = [
        account_folder / "data"       / "wti_trades.csv",
        account_folder / "multi_data" / "wti_trades.csv",
    ]
    csv_path = next((p for p in candidates if p.exists()), None)
    if csv_path is None:
        return {"volume": 0.0, "fills": 0, "realized_pnl": 0.0,
                "since_ts": since_ts, "until_ts": until_ts}
    rows = _read_csv(csv_path)
    fills_by_market: dict[str, list[dict[str, Any]]] = {}
    total_v = 0.0
    total_f = 0
    for r in rows:
        if r.get("outcome") != "ok":
            continue
        m = r.get("market")
        if not m:
            continue
        sz = _as_float(r.get("size")) or 0.0
        px = _as_float(r.get("price")) or 0.0
        ts = _as_float(r.get("ts"))
        if px <= 0 or sz == 0 or ts is None:
            continue
        if ts < since_ts:
            continue
        if until_ts is not None and ts >= until_ts:
            continue
        notional = abs(sz) * px
        total_v += notional
        total_f += 1
        fills_by_market.setdefault(m, []).append({
            "ts": ts, "side": r.get("side"), "size": sz, "price": px,
        })
    total_realized = 0.0
    for m, fills in fills_by_market.items():
        # mid=None -> unrealized skipped, only FIFO realized counts.
        res = _pnl_for_market(fills, current_mid=None)
        total_realized += res.get("realized") or 0.0
    return {
        "volume":       round(total_v, 2),
        "fills":        total_f,
        "realized_pnl": round(total_realized, 2),
        "since_ts":     since_ts,
        "until_ts":     until_ts,
    }


def compute_account_stats(account_folder: Path) -> dict[str, Any]:
    # Probe both schema variants. Single-pair grids write to data/, the
    # multi-pair HIP3 arb writes to multi_data/. MM bots don't write a
    # wti_trades.csv at all — those return empty stats here.
    candidates = [
        account_folder / "data"       / "wti_trades.csv",
        account_folder / "multi_data" / "wti_trades.csv",
    ]
    csv_path = next((p for p in candidates if p.exists()), None)
    if csv_path is None:
        return {
            "csv_path": None,
            "volume": 0.0, "volume_7d": 0.0,
            "fills": 0, "fills_7d": 0,
            "realized_pnl": 0.0,
            "per_market": {},
        }

    rows = _read_csv(csv_path)
    cutoff_7d = time.time() - _SEVEN_DAY_SEC

    per_market: dict[str, dict[str, Any]] = {}
    total_v = 0.0
    total_v7 = 0.0
    total_f = 0
    total_f7 = 0
    fills_by_market: dict[str, list[dict[str, Any]]] = {}

    for r in rows:
        if r.get("outcome") != "ok":
            continue
        m = r.get("market")
        if not m:
            continue
        sz = _as_float(r.get("size")) or 0.0
        px = _as_float(r.get("price")) or 0.0
        # Skip price=0 rows (legacy bug); they corrupt the FIFO walk
        # downstream and contribute zero notional anyway.
        if px <= 0 or sz == 0:
            continue
        notional = abs(sz) * px
        d = per_market.setdefault(m, {
            "volume": 0.0, "volume_7d": 0.0,
            "fills": 0, "fills_7d": 0, "realized": 0.0,
        })
        d["volume"] += notional
        d["fills"]  += 1
        total_v += notional
        total_f += 1
        ts = _as_float(r.get("ts"))
        if ts is not None and ts >= cutoff_7d:
            d["volume_7d"] += notional
            d["fills_7d"]  += 1
            total_v7 += notional
            total_f7 += 1
        # _pnl_for_market needs floatified fields — pre-convert here.
        fills_by_market.setdefault(m, []).append({
            "ts":    ts,
            "side":  r.get("side"),
            "size":  sz,
            "price": px,
        })

    total_realized = 0.0
    for m, fills in fills_by_market.items():
        # mid=None => unrealized is None and skipped; realized is FIFO-walked.
        res = _pnl_for_market(fills, current_mid=None)
        per_market[m]["realized"] = round(res.get("realized") or 0.0, 4)
        total_realized += res.get("realized") or 0.0

    for d in per_market.values():
        d["volume"]    = round(d["volume"],    2)
        d["volume_7d"] = round(d["volume_7d"], 2)

    return {
        "csv_path":     str(csv_path),
        "volume":       round(total_v,  2),
        "volume_7d":    round(total_v7, 2),
        "fills":        total_f,
        "fills_7d":     total_f7,
        "realized_pnl": round(total_realized, 2),
        "per_market":   per_market,
    }


# ---------------------------------------------------------------------------
# aiohttp middleware + handlers
# ---------------------------------------------------------------------------
@web.middleware
async def _auth_middleware(request: web.Request, handler):
    if API_KEY and request.path != "/health":
        if request.headers.get("X-API-Key") != API_KEY:
            return web.json_response({"error": "unauthorized"}, status=401)
    return await handler(request)


@web.middleware
async def _cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        resp = web.Response(status=204)
    else:
        resp = await handler(request)
    resp.headers["Access-Control-Allow-Origin"]  = CORS_ORIGIN
    resp.headers["Access-Control-Allow-Headers"] = "X-API-Key, Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


def _build_status(state: BotState) -> dict[str, Any]:
    # Include dry_run fills in PnL only when the bot is paper-trading.
    armed = bool(state.grid_params.get("trading", True))

    # Authoritative PnL from the exchange when the bot has plumbed it
    # through (Lighter does); otherwise fall back to FIFO-walked CSV.
    if state.exchange_pnl:
        pm = state.exchange_pnl
        pnl = {
            "per_market":             pm,
            "realized_total":         round(sum(p.get("realized_pnl",      0.0) for p in pm.values()), 4),
            "unrealized_total":       round(sum(p.get("unrealized_pnl",    0.0) for p in pm.values()), 4),
            "realized_session_total": round(sum(p.get("realized_session", 0.0) for p in pm.values()), 4),
            "session_start_ts":       SESSION_START_TS or None,
            "source":                 "exchange",
        }
    else:
        pnl = _pnl_all(state.data_dir, state.markets, state.last_bbo,
                       include_dry_run=not armed)
        pnl["source"] = "csv"

    return {
        "venue":          state.venue,
        "running":        state.running,
        "target_level":   state.target_level,
        "positions":      state.positions,
        "last_bbo":       state.last_bbo,
        "grid":           state.grid_params,
        "markets":        state.markets,
        "last_update_ts": state.last_update_ts,
        "level_hits":     _level_hits(state.data_dir),
        "recent_fills":   _recent_fills(state.data_dir),
        "pnl":            pnl,
        "volume":         _volume_all(state.data_dir, state.markets),
        "balance":        state.balance,
        "directional_delta": state.directional_delta,
        "close":          {
            "requested": state.close_requested,
            "status":    state.close_status,
            "error":     state.close_error,
        },
    }


async def run_control_server(state: BotState, port: int, host: str = "0.0.0.0") -> None:
    """Spawn as `asyncio.create_task(run_control_server(state, port))`."""
    app = web.Application(middlewares=[_cors_middleware, _auth_middleware])

    async def status(request: web.Request) -> web.Response:
        return web.json_response(_build_status(state))

    async def start(request: web.Request) -> web.Response:
        state.running = True
        return web.json_response({"running": True})

    async def stop(request: web.Request) -> web.Response:
        state.running = False
        return web.json_response({"running": False})

    async def close(request: web.Request) -> web.Response:
        # Idempotent — repeated POSTs are safe; bot's main loop only
        # acts on the first transition and then exits.
        # Query param `flatten` (default true) tells the bot whether to
        # market-flatten positions or just cancel resting orders. Bots
        # that don't read state.flatten_on_close fall back to legacy
        # cancel+flatten behavior, which is what `flatten=true` means.
        flatten_str = (request.rel_url.query.get("flatten", "true") or "").lower()
        state.flatten_on_close = flatten_str not in ("0", "false", "no", "off")
        state.close_requested = True
        if state.close_status == "idle":
            state.close_status = "pending"
        return web.json_response({
            "ok": True,
            "close_requested": True,
            "flatten_on_close": state.flatten_on_close,
            "status": state.close_status,
        })

    async def health(request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "venue": state.venue})

    # ---- live config -------------------------------------------------
    # Subset of grid_params that can be safely changed at runtime. Each
    # entry is (key, coercer). The coercer normalises whatever the JSON
    # body sends (string or number) into a concrete Python type before
    # writing into state.grid_params; the bot's strategy code reads from
    # there on every reconcile tick.
    LIVE_CONFIG_KEYS: dict[str, type] = {
        "order_size_usd":             float,
        "levels_per_side":            int,
        "grid_step_bp":               float,
        # Per-level offset overrides as CSV string, e.g. "14,40".
        # Empty string falls back to GRID_STEP_BP × k uniform spacing.
        "level_offsets_bp":           str,
        "max_notional_per_pair":      float,
        "max_total_notional":         float,
        "requote_drift_bp":           float,
        "requote_max_age_sec":        float,
        "session_dd_limit_usd":       float,
        "pair_stop_loss_usd":         float,
        "fill_imbalance_ratio":       float,
        "fill_imbalance_min_fills":   int,
    }

    async def update_config(request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            return web.json_response(
                {"ok": False, "error": f"invalid JSON: {exc}"},
                status=400,
            )
        if not isinstance(data, dict):
            return web.json_response(
                {"ok": False, "error": "body must be a JSON object"},
                status=400,
            )
        updated: dict[str, Any] = {}
        errors:  dict[str, str] = {}
        for k, raw in data.items():
            if k not in LIVE_CONFIG_KEYS:
                errors[k] = "not a live-tunable key"
                continue
            coercer = LIVE_CONFIG_KEYS[k]
            try:
                v = coercer(raw)
            except (TypeError, ValueError):
                errors[k] = f"could not coerce {raw!r} to {coercer.__name__}"
                continue
            state.grid_params[k] = v
            updated[k] = v
        return web.json_response({
            "ok":          len(errors) == 0,
            "updated":     updated,
            "errors":      errors,
            "grid_params": dict(state.grid_params),
        })

    async def get_config(request: web.Request) -> web.Response:
        return web.json_response({
            "live_keys":   list(LIVE_CONFIG_KEYS.keys()),
            "grid_params": dict(state.grid_params),
        })

    app.router.add_get ("/status", status)
    app.router.add_post("/start",  start)
    app.router.add_post("/stop",   stop)
    app.router.add_post("/close",  close)
    app.router.add_get ("/health", health)
    app.router.add_get ("/config", get_config)
    app.router.add_post("/config", update_config)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"  Control server listening on {host}:{port} "
          f"(auth={'on' if API_KEY else 'off'})")
    # Block forever
    while True:
        await asyncio.sleep(3600)
