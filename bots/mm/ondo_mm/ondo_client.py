"""OndoPerps REST + WebSocket client for the multi-grid MM bot.

OndoPerps uses HMAC-SHA256 signing (much simpler than Rise's EIP-712).
This module wraps the pieces the bot needs:

  * REST signer: builds the three required headers
    (ONDO-KEY-ID, ONDO-TIMESTAMP, ONDO-SIGN).
  * REST wrapper: place, cancel, batch-cancel, positions, portfolio,
    contracts, markets.
  * WS auth: HMAC over "ondo_perps_ws_login" + timestamp.
  * WS subscribe helpers + reconnect-safe consumer scaffolding.

Public docs reference index:
  https://docs.ondoperps.xyz/llms.txt

Wire conventions
----------------
* Market identifiers are STRINGS like "AAPL-USD.P" — not numeric IDs.
* Prices and sizes are DECIMAL STRINGS aligned to per-market increments.
  `/v1/markets` returns `baseIncrement` (size step) and `quoteIncrement`
  (price tick) per market.
* Order/position/fill status enums:
    - order.status: open / fullyfilled / canceled
    - position.direction: long / short / neutral
* `clientOrderId` is an alphanumeric string with `_-` (max 64 chars).

Auth signature
--------------
* REST:  HMAC_SHA256(secret, timestamp_ms_str + METHOD + path + body) hex
* WS:    HMAC_SHA256(secret, "ondo_perps_ws_login" + timestamp_ms_str) hex
* Server enforces a ±30s window on timestamp.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal, getcontext
from typing import Any

import aiohttp
import websockets

getcontext().prec = 60

# Debug flag: print the exact (message, signature) pairs we generate so a
# signature_mismatch can be diagnosed without guessing. Enable with
# ONDO_DEBUG_SIGN=1 in .env.
_DEBUG_SIGN = (os.environ.get("ONDO_DEBUG_SIGN") or "").strip().lower() in (
    "1", "true", "yes", "on",
)


# ---- HMAC signer ---------------------------------------------------------

class OndoSigner:
    """Builds REST headers + WS auth payload for an OndoPerps API key.

    The key id is sent as-is in the ONDO-KEY-ID header (including the
    "ondoKeyId_" prefix). The api_secret is used verbatim as the HMAC key
    (including the "ondoApiSecret_" prefix).
    """

    WS_LOGIN_PREFIX = "ondo_perps_ws_login"

    def __init__(self, key_id: str, api_secret: str):
        if not key_id or not api_secret:
            raise ValueError("OndoSigner needs both key_id and api_secret")
        self.key_id = key_id
        # HMAC keys must be bytes; encode once for reuse on every request.
        self._secret_bytes = api_secret.encode("utf-8")

    # ---- REST signing ------------------------------------------------
    @staticmethod
    def _now_ms() -> str:
        return str(int(time.time() * 1000))

    def rest_headers(self, method: str, path_with_query: str,
                       body: str = "") -> dict[str, str]:
        """Build the three required headers for a single REST call.

        `path_with_query` must include leading slash + any query string.
        `body` is the exact request body bytes/string sent on the wire
        (empty string for GET/DELETE with no body). The signature scheme
        is order-sensitive: timestamp + METHOD + path + body.
        """
        ts = self._now_ms()
        method = method.upper()
        msg = ts + method + path_with_query + body
        sig = hmac.new(self._secret_bytes, msg.encode("utf-8"),
                         hashlib.sha256).hexdigest()
        if _DEBUG_SIGN:
            print(f"[ondo-sign REST] msg={msg!r} sig={sig[:16]}…{sig[-8:]}",
                  flush=True)
        return {
            "ONDO-KEY-ID":   self.key_id,
            "ONDO-TIMESTAMP": ts,
            "ONDO-SIGN":     sig,
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

    # ---- WS login ----------------------------------------------------
    def _ws_sign(self, ts: str) -> str:
        # NB: the public docs (as of 2026-06) say the message is
        #   "ondo_perps_ws_login" + time
        # but probing the live API with bots/mm/ondo_mm/_test_ws.py shows
        # the order is REVERSED — the server signs
        #   time + "ondo_perps_ws_login"
        # Verified against api.ondoperps.xyz mainnet on 2026-06-07.
        msg = ts + self.WS_LOGIN_PREFIX
        sig = hmac.new(self._secret_bytes, msg.encode("utf-8"),
                         hashlib.sha256).hexdigest()
        if _DEBUG_SIGN:
            print(f"[ondo-sign WS]   msg={msg!r} sig={sig[:16]}…{sig[-8:]}",
                  flush=True)
        return sig

    def ws_login_payload(self) -> dict[str, Any]:
        ts = self._now_ms()
        return {
            "op": "login",
            "args": {
                "key":  self.key_id,
                "time": ts,                # string ms — int triggers a silent
                                            # server hangup (1006 no close frame)
                "sign": self._ws_sign(ts),
            },
        }


# ---- market metadata -----------------------------------------------------

@dataclass
class MarketMeta:
    """OndoPerps market descriptor.

    `baseIncrement` is the size step (lot size); `quoteIncrement` is the
    price tick. Both are decimal strings (e.g. "0.01" or "0.0001"). The
    bot snaps every order's price/size to multiples of these.
    """
    market: str                # ticker, e.g. "AAPL-USD.P"
    base_increment: Decimal    # size step
    quote_increment: Decimal   # price tick
    size_decimals: int         # derived from base_increment
    price_decimals: int        # derived from quote_increment

    @property
    def size_unit(self) -> float:
        return float(self.base_increment)


def _step_decimals(step: str) -> int:
    d = Decimal(step).normalize()
    return max(0, -d.as_tuple().exponent)


def parse_markets_response(data: dict, wanted: set[str]) -> dict[str, MarketMeta]:
    """Build {market: MarketMeta} from /v1/markets response, filtered to
    `wanted` (e.g. {"AAPL-USD.P", "NVDA-USD.P"})."""
    out: dict[str, MarketMeta] = {}
    result = data.get("result") or {}
    perps = result.get("perps") or {}
    for tp in perps.get("tradingPairs") or []:
        mkt = tp.get("market") or ""
        if mkt not in wanted:
            continue
        try:
            base_inc = Decimal(str(tp["baseIncrement"]))
            quote_inc = Decimal(str(tp["quoteIncrement"]))
        except (KeyError, TypeError, ValueError):
            continue
        if base_inc <= 0 or quote_inc <= 0:
            continue
        out[mkt] = MarketMeta(
            market=mkt,
            base_increment=base_inc,
            quote_increment=quote_inc,
            size_decimals=_step_decimals(str(tp["baseIncrement"])),
            price_decimals=_step_decimals(str(tp["quoteIncrement"])),
        )
    return out


# ---- snap helpers --------------------------------------------------------

def snap_price(px: float, quote_increment: Decimal) -> str:
    """Snap a float price to the nearest quote_increment, return decimal string."""
    q = (Decimal(str(px)) / quote_increment).to_integral_value() * quote_increment
    return format(q.normalize(), 'f')


def snap_size(sz: float, base_increment: Decimal) -> str:
    """Snap a float size to the nearest base_increment, return decimal string."""
    q = (Decimal(str(sz)) / base_increment).to_integral_value() * base_increment
    return format(q.normalize(), 'f')


def to_float(s: str | None) -> float:
    if s is None or s == "":
        return 0.0
    try: return float(s)
    except (TypeError, ValueError): return 0.0


# ---- REST client --------------------------------------------------------

class OndoRestError(Exception):
    def __init__(self, code: int, message: str, raw: Any = None):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.raw = raw

    @property
    def is_rate_limited(self) -> bool:
        return self.code == 429


class OndoRest:
    """Thin async REST client. One aiohttp session; caller owns close()."""

    def __init__(self, base_url: str, signer: OndoSigner,
                 timeout_sec: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.signer   = signer
        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(total=timeout_sec)

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _request(self, method: str, path: str,
                         *, query: dict | None = None,
                         body: dict | None = None) -> dict:
        # Build path + query so the signature covers exactly what the
        # server sees on the wire.
        qs = ""
        if query:
            qs = "?" + "&".join(
                f"{k}={v}" for k, v in query.items() if v is not None
            )
        full_path = path + qs
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        headers = self.signer.rest_headers(method, full_path, body_str)
        sess = await self._sess()
        url = f"{self.base_url}{full_path}"
        kwargs = {"headers": headers}
        if body is not None:
            kwargs["data"] = body_str   # send the EXACT bytes we signed
        async with sess.request(method.upper(), url, **kwargs) as r:
            text = await r.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"raw": text}
            if r.status >= 400 or (isinstance(data, dict) and data.get("success") is False):
                msg = (data.get("error") or data.get("message")
                       or text or "").strip()
                code = r.status if r.status >= 400 else 400
                raise OndoRestError(code, msg or f"HTTP {r.status}", data)
            return data

    # ---- market data --------------------------------------------------
    async def get_markets(self) -> dict:
        return await self._request("GET", "/v1/markets")

    async def get_contracts(self) -> dict:
        return await self._request("GET", "/v1/perps/contracts")

    # ---- account ------------------------------------------------------
    async def get_portfolio_summary(self) -> dict:
        return await self._request("GET", "/v1/portfolio/summary")

    async def get_positions(self) -> dict:
        return await self._request("GET", "/v1/perps/positions")

    async def get_open_orders(self, market: str | None = None) -> dict:
        query = {"market": market} if market else None
        return await self._request("GET", "/v1/perps/orders", query=query)

    # ---- orders -------------------------------------------------------
    async def place_order(self, *, market: str, side: str,
                            price: str, size: str,
                            client_order_id: str,
                            post_only: bool = True,
                            reduce_only: bool = False,
                            order_type: str = "limit",
                            time_in_force: str = "GTC") -> dict:
        body = {
            "market":        market,
            "side":          side,                  # "buy" | "sell"
            "type":          order_type,            # "limit" | "market"
            "price":         price,
            "size":          size,
            "clientOrderId": client_order_id,
            "postOnly":      post_only,
            "reduceOnly":    reduce_only,
        }
        if order_type == "limit":
            body["timeInForce"] = time_in_force
        return await self._request("POST", "/v1/perps/orders", body=body)

    async def cancel_order(self, order_id: str,
                             by_client_id: bool = False) -> dict:
        """Cancel by server orderId OR by clientOrderId. Path scheme:
        DELETE /v1/perps/orders/{orderId} for server id;
        DELETE /v1/perps/orders/client:{clientOrderId} for client id."""
        path_id = f"client:{order_id}" if by_client_id else order_id
        return await self._request("DELETE", f"/v1/perps/orders/{path_id}")

    async def batch_cancel(self, order_ids: list[str]) -> dict:
        """Comma-separated query param. Each entry may be a server id or
        client:{clientOrderId}."""
        if not order_ids:
            return {"success": True, "result": {"successfulCancels": [],
                                                  "failedCancels": []}}
        return await self._request(
            "DELETE", "/v1/perps/orders/batch",
            query={"orderIDs": ",".join(order_ids)},
        )


# ---- WebSocket helpers --------------------------------------------------

WS_PING_INTERVAL_SEC = 30.0   # docs: 180s idle timeout, so ping every 30s
INACTIVITY_TIMEOUT_SEC = 180.0


async def ws_login(ws, signer: OndoSigner, log: logging.Logger) -> bool:
    """Send the {op:login,args:{key,time,sign}} frame and wait for
    {type:"loggedIn"}. Returns True on success."""
    await ws.send(json.dumps(signer.ws_login_payload()))
    try:
        for _ in range(50):
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            msg = json.loads(raw)
            if msg.get("type") == "loggedIn":
                log.info("WS login ok")
                return True
            if msg.get("type") == "error":
                log.error(f"WS login error: {msg}")
                return False
    except asyncio.TimeoutError:
        log.error("WS login timed out")
        return False
    return False


async def ws_ping_loop(ws, stop: asyncio.Event) -> None:
    """Send {op:ping} every WS_PING_INTERVAL_SEC. Server replies {type:pong}.
    Connections idle for 180s are closed by the server, so this also acts
    as our NAT-keepalive."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=WS_PING_INTERVAL_SEC)
            return
        except asyncio.TimeoutError:
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:  # noqa: BLE001
                return


# ---- client_order_id encoding -------------------------------------------
# OndoPerps clientOrderId is a string up to 64 chars from [A-Za-z0-9_-].
# We encode the same (counter, market_idx, slot) triplet as on Lighter/Rise
# so the bot's orphan-detection logic ports over verbatim, but expose it
# as a stable string format.

def make_client_order_id(counter: int, market_idx: int, slot: int) -> str:
    return f"mm_{counter & 0xFFFFFFFF:08x}_{market_idx & 0xFF:02x}_{slot & 0xFF:02x}"


def parse_client_order_id(coid: str) -> tuple[int, int, int] | None:
    """Inverse of make_client_order_id; returns None on bad input."""
    parts = coid.split("_")
    if len(parts) != 4 or parts[0] != "mm":
        return None
    try:
        return int(parts[1], 16), int(parts[2], 16), int(parts[3], 16)
    except ValueError:
        return None
