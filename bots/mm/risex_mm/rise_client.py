"""Rise (rise.trade) REST + WebSocket client for the multi-grid MM bot.

Rise has no Python SDK, so this module hand-rolls the pieces the bot needs:

  * EIP-712 signing of order / cancel / cancel-all permits via eth_account.
  * Personal-sign of the WS auth challenge string.
  * Tiny REST wrapper (place_order, cancel_order, cancel_all_orders,
    portfolio_details, markets, active_orders).
  * Authenticated WS subscriber framework (orders, positions, orderbook).

Public docs reference index:
  https://developer.rise.trade/llms.txt

Coordinate units
----------------
Rise uses 18-decimal "wei" strings for prices and sizes in the WS feeds.
The REST place_order endpoint uses two integer scales instead:
  * price_ticks  = price / step_price          (uint24)
  * size_steps   = size  / step_size           (uint32)
Both `step_price` and `step_size` come from /v1/markets as decimal strings
(e.g. "0.01"). MarketMeta below derives the integer scales once on startup.

Two permit modes
----------------
Rise's `permit` envelope accepts EITHER a `signature` (EIP-712 signed
client-side) OR a `signer_private_key` (server signs for you). We support
both via the RISE_PERMIT_MODE env var:

  * RISE_PERMIT_MODE=server  (default)  — send the signer's private key;
    Rise's API signs every order on the wire. Trust trade-off: your
    session key crosses the network. Acceptable because it's only the
    registered subkey, not the master wallet; revoke + re-register if it
    ever leaks.

  * RISE_PERMIT_MODE=client             — sign EIP-712 locally with
    eth_account. Requires the exact `_*_PERMIT_TYPES` schemas to be
    correct; they're currently educated-guess placeholders. Until the
    real type strings are confirmed, this mode produces signatures Rise
    will reject as "Invalid signature."

Default is server-mode so the bot is functional today; flip to client
mode once you have the verified EIP-712 schemas.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import Any, Callable

import aiohttp
import websockets
from eth_account import Account
from eth_account.messages import encode_defunct, encode_typed_data


# Permit mode: "server" sends signer_private_key in the permit (Rise's
# server signs on our behalf); "client" signs EIP-712 typed data locally.
# Server-mode is the working default; client-mode needs the real
# `_*_PERMIT_TYPES` schemas plugged in below.
PERMIT_MODE = (os.environ.get("RISE_PERMIT_MODE") or "server").strip().lower()

# More than enough precision for size_steps / price_ticks math without
# floating-point drift on small-tick markets.
getcontext().prec = 60


# ---- market metadata -----------------------------------------------------

@dataclass
class MarketMeta:
    """Rise market descriptor — analogous to Lighter's MarketMeta.

    Parses /v1/markets response into the integer scales the place_order
    endpoint needs. step_size and step_price arrive as decimal strings
    (e.g. "0.0001"); we convert each to (decimals, integer_scale)."""
    symbol: str
    market_id: int
    size_decimals: int        # e.g. step_size "0.0001" -> 4
    price_decimals: int       # e.g. step_price "0.01"  -> 2
    min_order_size: float     # decimal value, not steps
    step_size: Decimal        # raw decimal
    step_price: Decimal       # raw decimal
    available: bool
    post_only_only: bool
    max_leverage: float

    @property
    def size_unit(self) -> float:
        return float(self.step_size)


def _step_decimals(step_str: str) -> int:
    """Return number of decimal places implied by a step string ('0.0001' -> 4)."""
    d = Decimal(step_str).normalize()
    # Decimal normalization can leave exponent positive for whole numbers.
    return max(0, -d.as_tuple().exponent)


def parse_markets_response(data: dict, wanted: set[str]) -> dict[str, MarketMeta]:
    """Build {symbol: MarketMeta} from the markets JSON, filtered to `wanted`."""
    out: dict[str, MarketMeta] = {}
    for m in data.get("markets") or []:
        sym = (m.get("base_asset_symbol") or "").upper()
        if sym not in wanted:
            continue
        if not m.get("available", False):
            continue
        cfg = m.get("config") or {}
        try:
            step_size  = Decimal(str(cfg.get("step_size") or "0"))
            step_price = Decimal(str(cfg.get("step_price") or "0"))
            min_sz     = float(cfg.get("min_order_size") or 0)
        except (TypeError, ValueError):
            continue
        if step_size <= 0 or step_price <= 0:
            continue
        out[sym] = MarketMeta(
            symbol=sym,
            market_id=int(m["market_id"]),
            size_decimals=_step_decimals(str(cfg.get("step_size"))),
            price_decimals=_step_decimals(str(cfg.get("step_price"))),
            min_order_size=min_sz,
            step_size=step_size,
            step_price=step_price,
            available=True,
            post_only_only=bool(m.get("post_only", False)),
            max_leverage=float(cfg.get("max_leverage") or 0),
        )
    return out


# ---- wei / decimal helpers ----------------------------------------------

WEI = Decimal(10) ** 18


def wei_to_float(s: str | int) -> float:
    """Convert wei-encoded string (18 decimals) to float price/size."""
    if s is None or s == "":
        return 0.0
    try:
        return float(Decimal(str(s)) / WEI)
    except Exception:  # noqa: BLE001
        return 0.0


def float_to_wei(x: float) -> int:
    """Convert float to integer wei (18 decimals)."""
    return int(Decimal(str(x)) * WEI)


# ---- EIP-712 signing -----------------------------------------------------

# === PLACEHOLDER — these need exact schemas from Rise ===
#
# The Rise public docs describe permits as Permit2-style "VerifyWitness"
# payloads but never spell out the type strings. The shapes below are
# educated guesses based on the wire fields in the REST docs; signatures
# generated with them will FAIL verification on Rise's verifying contract
# until the real types are dropped in.
#
# Where to find the real definitions:
#   1) DevTools -> Sources on app.rise.trade — grep for 'EIP712Domain'
#      and 'PermitSingle' in the JS bundle.
#   2) The verifying contract source on the RISE Chain explorer (verified
#      bytecode), if open.
#   3) dev@rise.trade — request type definitions for Order, Cancel,
#      CancelAll permits.
#
# Replace _ORDER_PERMIT_TYPES / _CANCEL_PERMIT_TYPES / _CANCEL_ALL_PERMIT_TYPES
# with the actual schemas; nothing else needs to change.

# Each entry is the EIP-712 `types` dict. The primary type the signer hashes
# is the FIRST key after EIP712Domain.

_EIP712_DOMAIN_TYPE = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]

_ORDER_PERMIT_TYPES = {
    "EIP712Domain": _EIP712_DOMAIN_TYPE,
    # TODO(rise-eip712): replace with real Permit + Order witness types.
    "PermitWitnessOrder": [
        {"name": "signer", "type": "address"},
        {"name": "nonce_anchor", "type": "uint64"},
        {"name": "nonce_bitmap_index", "type": "uint8"},
        {"name": "deadline", "type": "uint40"},
        {"name": "witness", "type": "Order"},
    ],
    "Order": [
        {"name": "market_id", "type": "uint16"},
        {"name": "side", "type": "uint8"},
        {"name": "size_steps", "type": "uint32"},
        {"name": "price_ticks", "type": "uint24"},
        {"name": "post_only", "type": "bool"},
        {"name": "reduce_only", "type": "bool"},
        {"name": "stp_mode", "type": "uint8"},
        {"name": "order_type", "type": "uint8"},
        {"name": "time_in_force", "type": "uint8"},
        {"name": "client_order_id", "type": "uint64"},
        {"name": "ttl_units", "type": "uint16"},
        {"name": "builder_id", "type": "uint64"},
    ],
}
_ORDER_PERMIT_PRIMARY = "PermitWitnessOrder"

_CANCEL_PERMIT_TYPES = {
    "EIP712Domain": _EIP712_DOMAIN_TYPE,
    # TODO(rise-eip712): replace with real Cancel witness type.
    "PermitWitnessCancel": [
        {"name": "signer", "type": "address"},
        {"name": "nonce_anchor", "type": "uint64"},
        {"name": "nonce_bitmap_index", "type": "uint8"},
        {"name": "deadline", "type": "uint40"},
        {"name": "witness", "type": "Cancel"},
    ],
    "Cancel": [
        {"name": "market_id", "type": "uint16"},
        {"name": "order_id", "type": "bytes"},
    ],
}
_CANCEL_PERMIT_PRIMARY = "PermitWitnessCancel"

_CANCEL_ALL_PERMIT_TYPES = {
    "EIP712Domain": _EIP712_DOMAIN_TYPE,
    # TODO(rise-eip712): replace with real CancelAll witness type.
    "PermitWitnessCancelAll": [
        {"name": "signer", "type": "address"},
        {"name": "nonce_anchor", "type": "uint64"},
        {"name": "nonce_bitmap_index", "type": "uint8"},
        {"name": "deadline", "type": "uint40"},
        {"name": "witness", "type": "CancelAll"},
    ],
    "CancelAll": [
        {"name": "market_id", "type": "uint16"},
    ],
}
_CANCEL_ALL_PERMIT_PRIMARY = "PermitWitnessCancelAll"


@dataclass
class EIP712Domain:
    name: str
    version: str
    chain_id: int
    verifying_contract: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "chainId": self.chain_id,
            "verifyingContract": self.verifying_contract,
        }


class RiseSigner:
    """Holds the registered signer (session-key) private key and signs
    Rise's EIP-712 typed-data permits + WS sign-in messages.

    The account_address is the MASTER wallet (registered the signer key).
    The private_key is the SIGNER's key — never the master key.
    """

    def __init__(self, account_address: str, signer_address: str,
                 signer_private_key: str, domain: EIP712Domain,
                 permit_mode: str = PERMIT_MODE):
        self.account_address = Account.to_checksum_address(account_address)
        self.signer_address  = Account.to_checksum_address(signer_address)
        self._account = Account.from_key(signer_private_key)
        if self._account.address.lower() != self.signer_address.lower():
            raise ValueError(
                f"signer_address {self.signer_address} doesn't match "
                f"private key's address {self._account.address}"
            )
        # Store the raw key string so server-mode can forward it; ensure
        # 0x-prefixed lowercase for wire consistency.
        k = signer_private_key.strip()
        if not k.startswith("0x"):
            k = "0x" + k
        self._signer_private_key_hex = k.lower()
        self.domain = domain
        if permit_mode not in ("server", "client"):
            raise ValueError(f"permit_mode must be 'server' or 'client', got {permit_mode!r}")
        self.permit_mode = permit_mode

    def _base_permit(self) -> dict[str, Any]:
        """Return the common permit envelope fields. Caller adds either
        `signature` (client mode) or `signer_private_key` (server mode)."""
        return {
            "account":            self.account_address,
            "signer":             self.signer_address,
            "nonce_anchor":       str(self.fresh_nonce_anchor()),
            "nonce_bitmap_index": 0,
            "deadline":           self.deadline_in(60),
        }

    # ---- nonce management --------------------------------------------
    # Rise uses a Permit2-style nonce: each signer owns 256-bit bitmaps
    # indexed by `nonce_bitmap_index`; a `nonce_anchor` then picks one bit.
    # The simplest correct approach: pick a fresh random uint64 anchor per
    # permit. Collisions across 2^64 are negligible for a single client.
    @staticmethod
    def fresh_nonce_anchor() -> int:
        return secrets.randbits(64)

    @staticmethod
    def deadline_in(seconds: int = 60) -> int:
        return int(time.time()) + seconds

    # ---- WS auth (personal_sign) -------------------------------------
    def sign_ws_auth(self) -> dict[str, Any]:
        """Build the auth frame for the WS handshake. The `message` is a
        plain human-readable string (EIP-191 personal_sign style)."""
        nonce = int(time.time())
        now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        message = (
            "Please sign in with your wallet to access rise.trade. "
            f"You are signing in on {now} (GMT). This message is exclusively "
            "signed with rise.trade for security."
        )
        signed = self._account.sign_message(encode_defunct(text=message))
        return {
            "method": "auth",
            "params": {
                "account": self.account_address,
                "signer": self.signer_address,
                "message": message,
                "nonce": nonce,
                "signature": signed.signature.hex(),
            },
        }

    # ---- EIP-712 typed-data permit signing ---------------------------
    def _sign_typed(self, types: dict, primary: str,
                    message: dict) -> str:
        """Sign an EIP-712 typed-data payload and return 0x-prefixed sig hex."""
        full = {
            "types": types,
            "domain": self.domain.as_dict(),
            "primaryType": primary,
            "message": message,
        }
        signable = encode_typed_data(full_message=full)
        return self._account.sign_message(signable).signature.hex()

    def _witness_message(self, anchor: int, bitmap: int, deadline: int,
                           witness: dict) -> dict[str, Any]:
        return {
            "signer":             self.signer_address,
            "nonce_anchor":       anchor,
            "nonce_bitmap_index": bitmap,
            "deadline":           deadline,
            "witness":            witness,
        }

    def sign_order_permit(self, order_fields: dict[str, Any]) -> dict[str, Any]:
        """Build the order-placement permit. Server-mode just returns the
        envelope + signer_private_key; client-mode produces an EIP-712 sig."""
        permit = self._base_permit()
        if self.permit_mode == "server":
            permit["signer_private_key"] = self._signer_private_key_hex
            return permit
        witness = {
            "market_id":       int(order_fields["market_id"]),
            "side":            int(order_fields["side"]),
            "size_steps":      int(order_fields["size_steps"]),
            "price_ticks":     int(order_fields["price_ticks"]),
            "post_only":       bool(order_fields.get("post_only", True)),
            "reduce_only":     bool(order_fields.get("reduce_only", False)),
            "stp_mode":        int(order_fields.get("stp_mode", 0)),
            "order_type":      int(order_fields.get("order_type", 1)),
            "time_in_force":   int(order_fields.get("time_in_force", 0)),
            "client_order_id": int(order_fields.get("client_order_id", 0)),
            "ttl_units":       int(order_fields.get("ttl_units", 0)),
            "builder_id":      int(order_fields.get("builder_id", 0)),
        }
        msg = self._witness_message(
            int(permit["nonce_anchor"]), permit["nonce_bitmap_index"],
            permit["deadline"], witness,
        )
        permit["signature"] = self._sign_typed(
            _ORDER_PERMIT_TYPES, _ORDER_PERMIT_PRIMARY, msg,
        )
        return permit

    def sign_cancel_permit(self, market_id: int,
                            order_id: str) -> dict[str, Any]:
        permit = self._base_permit()
        if self.permit_mode == "server":
            permit["signer_private_key"] = self._signer_private_key_hex
            return permit
        oid_hex = order_id[2:] if order_id.startswith("0x") else order_id
        witness = {"market_id": int(market_id), "order_id": bytes.fromhex(oid_hex)}
        msg = self._witness_message(
            int(permit["nonce_anchor"]), permit["nonce_bitmap_index"],
            permit["deadline"], witness,
        )
        permit["signature"] = self._sign_typed(
            _CANCEL_PERMIT_TYPES, _CANCEL_PERMIT_PRIMARY, msg,
        )
        return permit

    def sign_cancel_all_permit(self, market_id: int) -> dict[str, Any]:
        permit = self._base_permit()
        if self.permit_mode == "server":
            permit["signer_private_key"] = self._signer_private_key_hex
            return permit
        witness = {"market_id": int(market_id)}
        msg = self._witness_message(
            int(permit["nonce_anchor"]), permit["nonce_bitmap_index"],
            permit["deadline"], witness,
        )
        permit["signature"] = self._sign_typed(
            _CANCEL_ALL_PERMIT_TYPES, _CANCEL_ALL_PERMIT_PRIMARY, msg,
        )
        return permit


# ---- REST client --------------------------------------------------------

class RiseRestError(Exception):
    """Wraps a non-2xx Rise REST response. `code` mirrors the HTTP status,
    `message` is the server's `message` field, `raw` is the full body."""

    def __init__(self, code: int, message: str, raw: Any = None):
        super().__init__(f"[{code}] {message}")
        self.code = code
        self.message = message
        self.raw = raw

    @property
    def is_rate_limited(self) -> bool:
        return self.code == 429 or "rate limit" in (self.message or "").lower()


class RiseRest:
    """Thin async REST client for Rise. One aiohttp session shared across
    all calls; caller owns the lifecycle via close()."""

    def __init__(self, base_url: str, signer: RiseSigner,
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

    async def _post(self, path: str, body: dict) -> dict:
        sess = await self._sess()
        async with sess.post(f"{self.base_url}{path}", json=body) as r:
            text = await r.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"raw": text}
            if r.status >= 400:
                msg = (data.get("message") or text or "").strip()
                raise RiseRestError(r.status, msg or f"HTTP {r.status}", data)
            return data

    async def _get(self, path: str, params: dict | None = None) -> dict:
        sess = await self._sess()
        async with sess.get(f"{self.base_url}{path}", params=params or {}) as r:
            text = await r.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"raw": text}
            if r.status >= 400:
                msg = (data.get("message") or text or "").strip()
                raise RiseRestError(r.status, msg or f"HTTP {r.status}", data)
            return data

    # ---- market metadata ---------------------------------------------
    async def get_markets(self) -> dict:
        return await self._get("/v1/markets")

    async def get_eip712_domain(self) -> dict:
        return await self._get("/v1/auth/eip712-domain")

    # ---- account ----------------------------------------------------
    async def get_portfolio(self, account: str | None = None) -> dict:
        params = {"account": account} if account else None
        return await self._get("/v1/portfolio/details", params=params)

    async def get_open_orders(self, account: str | None = None,
                                market_id: int | None = None) -> dict:
        params: dict[str, Any] = {}
        if account: params["account"] = account
        if market_id is not None: params["market_id"] = market_id
        return await self._get("/v1/orders/open", params=params or None)

    # ---- orders -----------------------------------------------------
    async def place_order(self, *, market_id: int, side: int,
                            size_steps: int, price_ticks: int,
                            client_order_id: int,
                            post_only: bool = True,
                            reduce_only: bool = False,
                            time_in_force: int = 0,    # 0=GTC
                            order_type: int = 1,        # 1=Limit
                            stp_mode: int = 0,          # 0=ExpireMaker
                            ttl_units: int = 0,
                            builder_id: int = 0,
                            no_retry: bool = False) -> dict:
        order_fields = {
            "market_id":       market_id,
            "side":            side,
            "size_steps":      size_steps,
            "price_ticks":     price_ticks,
            "post_only":       post_only,
            "reduce_only":     reduce_only,
            "stp_mode":        stp_mode,
            "order_type":      order_type,
            "time_in_force":   time_in_force,
            "client_order_id": client_order_id,
            "ttl_units":       ttl_units,
            "builder_id":      builder_id,
        }
        permit = self.signer.sign_order_permit(order_fields)
        # Wire shape: per Rise's example payload, client_order_id is a
        # STRING on the wire even though it's an int in the typed-data.
        body = dict(order_fields)
        body["client_order_id"] = str(client_order_id)
        body["permit"] = permit
        body["no_retry"] = no_retry
        return await self._post("/v1/orders/place", body)

    async def cancel_order(self, market_id: int, order_id: str,
                            no_retry: bool = False) -> dict:
        permit = self.signer.sign_cancel_permit(market_id, order_id)
        return await self._post("/v1/orders/cancel", {
            "market_id": market_id,
            "order_id":  order_id,
            "permit":    permit,
            "no_retry":  no_retry,
        })

    async def cancel_all_orders(self, market_id: int) -> dict:
        permit = self.signer.sign_cancel_all_permit(market_id)
        return await self._post("/v1/orders/cancel-all", {
            "market_id": str(market_id),
            "permit":    permit,
        })


# ---- WebSocket helpers --------------------------------------------------

# Rise's WS protocol uses {method, params, ...} JSON frames. The bot opens
# one connection PER private subscription (orders, positions) and one for
# the public orderbook stream. Authentication is per-connection: send the
# `auth` frame, wait for {status:success}, then subscribe.

PING_INTERVAL_SEC = 25.0   # docs say server pings every 30s; we beat it
INACTIVITY_TIMEOUT = 60.0  # docs say 60s of silence closes the conn


async def ws_authenticate(ws, signer: RiseSigner,
                            log: logging.Logger) -> bool:
    """Send the `auth` frame and wait for {status: "success"}.
    Returns True on success, logs+returns False on rejection."""
    auth_frame = signer.sign_ws_auth()
    await ws.send(json.dumps(auth_frame))
    # Wait up to 5 seconds for the auth response.
    try:
        for _ in range(50):
            raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            msg = json.loads(raw)
            if msg.get("method") == "auth":
                if msg.get("status") == "success":
                    log.info("WS auth ok")
                    return True
                log.error(f"WS auth failed: {msg.get('message')}")
                return False
            # Ignore any other early frames (rare).
    except asyncio.TimeoutError:
        log.error("WS auth timed out")
        return False
    return False


async def ws_keepalive(ws, stop: asyncio.Event) -> None:
    """Send {op:'ping'} every PING_INTERVAL_SEC. Rise sends its own
    pings — we also reply pong inside the message loop — but we proactively
    ping to keep the connection warm against NAT idle-kills."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=PING_INTERVAL_SEC)
            return
        except asyncio.TimeoutError:
            try:
                await ws.send(json.dumps({"op": "ping"}))
            except Exception:  # noqa: BLE001
                return


# ---- order ID encoding for client_order_id -----------------------------
# Mirror Lighter's coid encoding scheme so the bot's existing slot/market
# pack-into-uint64 logic ports unchanged. The local 16-bit counter wraps;
# market index (8 bits) + slot (8 bits) keeps collisions impossible across
# market × level slots.

def make_client_order_id(counter: int, market_idx: int, slot: int) -> int:
    return ((counter & 0xFFFFFFFF) << 16) | ((market_idx & 0xFF) << 8) | (slot & 0xFF)
