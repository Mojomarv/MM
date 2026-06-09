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
import base64
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
# Account.to_checksum_address was removed from eth_account in newer
# releases — the helper moved into eth_utils, which is a transitive
# dependency. Import from the new home so the signer works regardless
# of which eth_account version is installed.
from eth_utils import to_checksum_address, keccak as eth_keccak


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


# ---- EIP-712 signing (port of risex-client TS SDK) ----------------------
#
# Reference: https://github.com/SmoothBot/risex-ts (MIT)
# Files mirrored: src/signing/{domain,encoder,permit,signer}.ts
#
# Signing has two layers:
#   1) Action-data hash: keccak256(abi.encode(actionTypeHash, ...packed
#      order data...)). Each action type (PLACE, CANCEL, CANCEL_ALL) has
#      its own constant string typehash and abi.encode layout.
#   2) EIP-712 typed-data over a VerifyWitness struct that bundles the
#      account + target contract + the bytes32 action hash + Permit2-style
#      nonce_anchor / nonce_bitmap_index + deadline.
#
# The signature is a 65-byte (r||s||v) hex string, base64-encoded — NOT
# EIP-2098 compact.

from eth_abi import encode as abi_encode  # noqa: E402

_EIP712_DOMAIN_TYPE = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]

# ─── Action typehashes ─────────────────────────────────────────────
ACTION_PLACE_ORDER       = b"RISE_PERPS_PLACE_ORDER_V1"
ACTION_CANCEL_ORDER      = b"RISE_PERPS_CANCEL_ORDER_V1"
ACTION_CANCEL_ALL_ORDERS = b"RISE_PERPS_CANCEL_ALL_ORDERS_V1"

_ACTION_PLACE_ORDER_HASH       = eth_keccak(ACTION_PLACE_ORDER)
_ACTION_CANCEL_ORDER_HASH      = eth_keccak(ACTION_CANCEL_ORDER)
_ACTION_CANCEL_ALL_ORDERS_HASH = eth_keccak(ACTION_CANCEL_ALL_ORDERS)

# ─── Protocol header flags (computed by computeHeaderFlags) ─────────
V3_FLAG_PERMIT         = 0x01
V3_FLAG_BUILDER        = 0x02
V3_FLAG_CLIENT_ID      = 0x04
V3_FLAG_PERMIT_ERC1271 = 0x09
V3_FLAG_TTL            = 0x10


def _compute_header_flags(*, builder_id: int, client_order_id: int,
                            ttl_units: int, is_erc1271: bool = False) -> int:
    """Set V3 protocol header bits based on which optional fields are present."""
    flags = V3_FLAG_PERMIT_ERC1271 if is_erc1271 else V3_FLAG_PERMIT
    if builder_id      != 0: flags |= V3_FLAG_BUILDER
    if client_order_id != 0: flags |= V3_FLAG_CLIENT_ID
    if ttl_units       != 0: flags |= V3_FLAG_TTL
    return flags


def _encode_order_data(*, market_id: int, size_steps: int, price_ticks: int,
                         side: int, post_only: bool, reduce_only: bool,
                         stp_mode: int, order_type: int,
                         time_in_force: int) -> int:
    """Compress order fields into a single 88-bit big-endian uint.

    Bit layout (matching risex-ts encodeOrderData):
      [87:70] marketId      (16 bits)
      [69:38] sizeSteps     (32 bits)
      [37:14] priceTicks    (24 bits)
      [13:6]  orderFlags    (8 bits)
      [5:1]   headerVersion (5 bits, always 1)
      [0]     reserved      (1 bit, 0)

    orderFlags byte:
      bit 0: side          (0=long, 1=short)
      bit 1: postOnly
      bit 2: reduceOnly
      bit 3-4: stpMode     (uint2)
      bit 5: orderType     (0=Market, 1=Limit)
      bit 6-7: timeInForce (uint2: 0=GTC, 1=IOC?, etc.)
    """
    order_flags = 0
    if side & 1:    order_flags |= 0x01
    if post_only:   order_flags |= 0x02
    if reduce_only: order_flags |= 0x04
    order_flags |= (stp_mode      & 0x03) << 3
    order_flags |= (order_type    & 0x01) << 5
    order_flags |= (time_in_force & 0x03) << 6

    header_version = 1
    data = 0
    data |= (market_id    & 0xFFFF)     << 70
    data |= (size_steps   & 0xFFFFFFFF) << 38
    data |= (price_ticks  & 0xFFFFFF)   << 14
    data |= (order_flags  & 0xFF)       << 6
    data |= (header_version & 0x1F)     << 1
    return data


def encode_place_order_hash(*, market_id: int, size_steps: int,
                              price_ticks: int, side: int, post_only: bool,
                              reduce_only: bool, stp_mode: int, order_type: int,
                              time_in_force: int, ttl_units: int = 0,
                              builder_id: int = 0, client_order_id: int = 0,
                              is_erc1271: bool = False) -> bytes:
    """Compute the bytes32 action hash that gets embedded in VerifyWitness.

    hash = keccak256(abi.encode(
        actionTypeHash,            // bytes32
        headerFlags,               // uint8
        orderData,                 // uint256 (88-bit packed)
        builderId,                 // uint16
        clientOrderId,             // uint64
        ttlUnits                   // uint16
    ))
    """
    order_data = _encode_order_data(
        market_id=market_id, size_steps=size_steps, price_ticks=price_ticks,
        side=side, post_only=post_only, reduce_only=reduce_only,
        stp_mode=stp_mode, order_type=order_type, time_in_force=time_in_force,
    )
    header_flags = _compute_header_flags(
        builder_id=builder_id, client_order_id=client_order_id,
        ttl_units=ttl_units, is_erc1271=is_erc1271,
    )
    encoded = abi_encode(
        ["bytes32", "uint8", "uint256", "uint16", "uint64", "uint16"],
        [_ACTION_PLACE_ORDER_HASH, header_flags, order_data,
         int(builder_id), int(client_order_id), int(ttl_units)],
    )
    return eth_keccak(encoded)


def encode_cancel_order_hash(*, market_id: int, resting_order_id: int) -> bytes:
    """keccak256(abi.encode(actionTypeHash, uint256(marketId), uint256(restingOrderId)))"""
    encoded = abi_encode(
        ["bytes32", "uint256", "uint256"],
        [_ACTION_CANCEL_ORDER_HASH, int(market_id), int(resting_order_id)],
    )
    return eth_keccak(encoded)


def encode_cancel_all_hash(*, market_id: int) -> bytes:
    """keccak256(abi.encode(actionTypeHash, uint256(marketId)))"""
    encoded = abi_encode(
        ["bytes32", "uint256"],
        [_ACTION_CANCEL_ALL_ORDERS_HASH, int(market_id)],
    )
    return eth_keccak(encoded)


# ─── VerifyWitness EIP-712 typed-data ───────────────────────────────
_VERIFY_WITNESS_TYPES = {
    "EIP712Domain": _EIP712_DOMAIN_TYPE,
    "VerifyWitness": [
        {"name": "account",     "type": "address"},
        {"name": "target",      "type": "address"},   # router/orders_manager
        {"name": "hash",        "type": "bytes32"},
        {"name": "nonceAnchor", "type": "uint48"},
        {"name": "nonceBitmap", "type": "uint8"},
        {"name": "deadline",    "type": "uint32"},
    ],
}
_MAX_BITMAP_INDEX = 207


# WS-login typed-data — confirmed from the official Rise docs (the
# /connection/login page documents both v1 and v2 auth flows):
#
#   v1 (method: "auth", nonce = unix seconds):
#     Register(address signer, string message, uint64 nonce)
#     message: fixed string "WebSocket Authentication"
#
#   v2 (method: "auth_v2", nonce = server-issued 32-byte hex):
#     RegisterV2(address signer, string message, uint256 nonce)
#     fetch nonce via GET /v1/auth/nonce
#
# Note: the `account` address goes into the outer JSON wire payload
# (so the server knows which on-chain account to look up the signer for)
# but is NOT one of the EIP-712 typed-data fields. We had this wrong
# previously — that's why ecrecover produced an unexpected address.
_WS_AUTH_TYPES = {
    "EIP712Domain": _EIP712_DOMAIN_TYPE,
    "Register": [
        {"name": "signer",  "type": "address"},
        {"name": "message", "type": "string"},
        {"name": "nonce",   "type": "uint64"},
    ],
}
_WS_AUTH_PRIMARY = "Register"
_WS_AUTH_MESSAGE = "WebSocket Authentication"


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
                 permit_mode: str = PERMIT_MODE,
                 target_address: str | None = None):
        self.account_address = to_checksum_address(account_address)
        self.signer_address  = to_checksum_address(signer_address)
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
        # `target` is the router / orders_manager contract address that
        # the VerifyWitness permit authorizes calls to. Fetched from
        # /v1/system-config and set via set_target(); permits cannot be
        # signed before it's known.
        self.target_address: str | None = (
            to_checksum_address(target_address) if target_address else None
        )
        if permit_mode not in ("server", "client"):
            raise ValueError(f"permit_mode must be 'server' or 'client', got {permit_mode!r}")
        self.permit_mode = permit_mode

    def set_target(self, target: str) -> None:
        self.target_address = to_checksum_address(target)

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

    # ---- WS auth (EIP-712 v1 — Register typed-data) ------------------
    def sign_ws_auth(self) -> dict[str, Any]:
        """Build the v1 auth frame for the WS handshake.

        Per Rise's docs (/connection/login), the WS v1 auth signs the
        EIP-712 primary type `Register(address signer, string message,
        uint64 nonce)` with the fixed message string `"WebSocket
        Authentication"` and nonce = current unix seconds. The outer
        `params.account` field rides on the JSON wire payload but is
        NOT part of the signed typed data.
        """
        nonce = int(time.time())
        sig_hex = self._sign_typed(_WS_AUTH_TYPES, _WS_AUTH_PRIMARY, {
            "signer":  self.signer_address,
            "message": _WS_AUTH_MESSAGE,
            "nonce":   nonce,
        })
        return {
            "method": "auth",
            "params": {
                "account":   self.account_address,
                "signer":    self.signer_address,
                "message":   _WS_AUTH_MESSAGE,
                "nonce":     nonce,
                "signature": sig_hex,
            },
        }

    # ---- EIP-712 typed-data permit signing ---------------------------
    def _sign_typed(self, types: dict, primary: str,
                    message: dict) -> str:
        """Sign an EIP-712 typed-data payload and return 0x-prefixed sig hex.
        Used for WebSocket auth (Rise expects hex with 0x prefix there)."""
        full = {
            "types": types,
            "domain": self.domain.as_dict(),
            "primaryType": primary,
            "message": message,
        }
        signable = encode_typed_data(full_message=full)
        return "0x" + self._account.sign_message(signable).signature.hex()

    def _sign_typed_b64_compact(self, types: dict, primary: str,
                                  message: dict) -> str:
        """Sign EIP-712 typed-data and return a 64-byte EIP-2098 compact
        signature, base64-encoded. This is the format Rise's REST
        endpoints accept for `permit.signature` (captured from the
        frontend network trace)."""
        full = {
            "types": types,
            "domain": self.domain.as_dict(),
            "primaryType": primary,
            "message": message,
        }
        signable = encode_typed_data(full_message=full)
        signed = self._account.sign_message(signable)
        # EIP-2098 compact: r (32 bytes) || yParityAndS (32 bytes).
        # eth_account gives us r, s, v separately; we need to encode
        # yParityAndS = s | (yParity << 255), where yParity = v - 27.
        r_bytes = signed.r.to_bytes(32, "big")
        s_int = signed.s
        y_parity = (signed.v - 27) & 1
        yps_int = s_int | (y_parity << 255)
        yps_bytes = yps_int.to_bytes(32, "big")
        compact = r_bytes + yps_bytes
        return base64.b64encode(compact).decode("ascii")

    def _sign_witness_b64(self, action_hash: bytes, *, nonce_anchor: int,
                            nonce_bitmap_index: int, deadline: int) -> str:
        """Sign the VerifyWitness typed-data over an action hash and
        return a base64-encoded 65-byte (r||s||v) signature."""
        if self.target_address is None:
            raise RuntimeError(
                "RiseSigner.target_address not set — call set_target() "
                "with the router/orders_manager address from /v1/system-config "
                "before signing any permit."
            )
        msg = {
            "account":     self.account_address,
            "target":      self.target_address,
            "hash":        action_hash,
            "nonceAnchor": int(nonce_anchor),
            "nonceBitmap": int(nonce_bitmap_index),
            "deadline":    int(deadline),
        }
        full = {
            "types":        _VERIFY_WITNESS_TYPES,
            "domain":       self.domain.as_dict(),
            "primaryType":  "VerifyWitness",
            "message":      msg,
        }
        signable = encode_typed_data(full_message=full)
        signed   = self._account.sign_message(signable)
        # 65-byte r||s||v (with v fixed up to 27/28 if needed).
        v = signed.v if signed.v >= 27 else signed.v + 27
        raw = signed.r.to_bytes(32, "big") + signed.s.to_bytes(32, "big") + bytes([v])
        return base64.b64encode(raw).decode("ascii")

    def _v3_permit(self, *, nonce_anchor: int, nonce_bitmap_index: int,
                     deadline: int, signature_b64: str) -> dict[str, Any]:
        """V3 permit envelope sent on the wire body's `permit` field.
        Note: nonce_anchor sent as int per the TS SDK (not str)."""
        return {
            "account":            self.account_address,
            "signer":             self.signer_address,
            "nonce_anchor":       int(nonce_anchor),
            "nonce_bitmap_index": int(nonce_bitmap_index),
            "deadline":           int(deadline),
            "signature":          signature_b64,
        }

    @staticmethod
    def adjust_nonce_for_bitmap_rollover(nonce_anchor: int, bitmap_index: int,
                                          ) -> tuple[int, int]:
        """If the bitmap is exhausted, advance the anchor and reset the index.
        Mirrors createPermitParams in the TS SDK."""
        if bitmap_index > _MAX_BITMAP_INDEX:
            return nonce_anchor + 1, 0
        return nonce_anchor, bitmap_index

    def sign_order_permit(self, *, market_id: int, size_steps: int,
                            price_ticks: int, side: int, post_only: bool,
                            reduce_only: bool, stp_mode: int, order_type: int,
                            time_in_force: int, ttl_units: int,
                            builder_id: int, client_order_id: int,
                            nonce_anchor: int, nonce_bitmap_index: int,
                            deadline_secs: int = 7 * 24 * 3600,
                          ) -> dict[str, Any]:
        deadline = self.deadline_in(deadline_secs)
        anchor, bitmap = self.adjust_nonce_for_bitmap_rollover(
            int(nonce_anchor), int(nonce_bitmap_index),
        )
        h = encode_place_order_hash(
            market_id=market_id, size_steps=size_steps, price_ticks=price_ticks,
            side=side, post_only=post_only, reduce_only=reduce_only,
            stp_mode=stp_mode, order_type=order_type,
            time_in_force=time_in_force, ttl_units=ttl_units,
            builder_id=builder_id, client_order_id=client_order_id,
        )
        sig_b64 = self._sign_witness_b64(
            h, nonce_anchor=anchor, nonce_bitmap_index=bitmap, deadline=deadline,
        )
        return self._v3_permit(
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
            deadline=deadline, signature_b64=sig_b64,
        )

    def sign_cancel_permit(self, *, market_id: int, resting_order_id: int,
                             nonce_anchor: int, nonce_bitmap_index: int,
                             deadline_secs: int = 7 * 24 * 3600,
                           ) -> dict[str, Any]:
        deadline = self.deadline_in(deadline_secs)
        anchor, bitmap = self.adjust_nonce_for_bitmap_rollover(
            int(nonce_anchor), int(nonce_bitmap_index),
        )
        h = encode_cancel_order_hash(
            market_id=market_id, resting_order_id=resting_order_id,
        )
        sig_b64 = self._sign_witness_b64(
            h, nonce_anchor=anchor, nonce_bitmap_index=bitmap, deadline=deadline,
        )
        return self._v3_permit(
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
            deadline=deadline, signature_b64=sig_b64,
        )

    def sign_cancel_all_permit(self, *, market_id: int,
                                 nonce_anchor: int, nonce_bitmap_index: int,
                                 deadline_secs: int = 7 * 24 * 3600,
                               ) -> dict[str, Any]:
        deadline = self.deadline_in(deadline_secs)
        anchor, bitmap = self.adjust_nonce_for_bitmap_rollover(
            int(nonce_anchor), int(nonce_bitmap_index),
        )
        h = encode_cancel_all_hash(market_id=market_id)
        sig_b64 = self._sign_witness_b64(
            h, nonce_anchor=anchor, nonce_bitmap_index=bitmap, deadline=deadline,
        )
        return self._v3_permit(
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
            deadline=deadline, signature_b64=sig_b64,
        )


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
    all calls; caller owns the lifecycle via close().

    Optional X-Auth-Token header (browser-grabbed session token) unlocks
    every endpoint EXCEPT /v1/orders/place — that one still requires a
    signed permit. Useful for monitoring + cancels even without the
    permit schema."""

    def __init__(self, base_url: str, signer: RiseSigner,
                 timeout_sec: float = 10.0,
                 session_token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.signer   = signer
        self.session_token = session_token
        self._session: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(total=timeout_sec)

    def _auth_headers(self) -> dict[str, str]:
        return {"X-Auth-Token": self.session_token} if self.session_token else {}

    async def _sess(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def _post(self, path: str, body: dict) -> dict:
        sess = await self._sess()
        async with sess.post(f"{self.base_url}{path}", json=body,
                              headers=self._auth_headers()) as r:
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
        async with sess.get(f"{self.base_url}{path}", params=params or {},
                              headers=self._auth_headers()) as r:
            text = await r.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"raw": text}
            if r.status >= 400:
                msg = (data.get("message") or text or "").strip()
                raise RiseRestError(r.status, msg or f"HTTP {r.status}", data)
            return data

    @staticmethod
    def _unwrap(resp: dict) -> dict:
        """Rise wraps every REST response in {'data': {...}, 'request_id': '...'}.
        Strip the envelope so callers can read fields directly."""
        if isinstance(resp, dict) and isinstance(resp.get("data"), dict):
            return resp["data"]
        return resp

    # ---- market metadata ---------------------------------------------
    async def get_markets(self) -> dict:
        return self._unwrap(await self._get("/v1/markets"))

    async def get_eip712_domain(self) -> dict:
        return self._unwrap(await self._get("/v1/auth/eip712-domain"))

    async def get_system_config(self) -> dict:
        """Fetch contract addresses, including the router/orders_manager
        used as `target` in VerifyWitness permits."""
        return self._unwrap(await self._get("/v1/system/config"))

    async def fetch_and_set_target(self) -> str:
        """Fetch /v1/system-config and set signer.target_address to the
        router (or orders_manager / perps_manager as fallback). Returns
        the resolved address."""
        cfg = await self.get_system_config()
        addrs = cfg.get("addresses") or {}
        target = (
            addrs.get("router")
            or (addrs.get("perp_v2") or {}).get("orders_manager")
            or (cfg.get("contract_addresses") or {}).get("perps_manager")
        )
        if not target:
            raise RiseRestError(
                500, "Could not find router/orders_manager in system config",
                cfg,
            )
        self.signer.set_target(target)
        return target

    async def get_nonce_state(self, account: str) -> tuple[int, int]:
        """Fetch the current (nonce_anchor, current_bitmap_index) for an
        account from Rise's auth contract via the REST proxy. Each
        permit consumes one bit at (anchor, bitmap_index); the frontend
        increments bitmap_index for successive permits and bumps anchor
        when the bitmap fills (every 208 orders).
        Path tried: /v1/auth/nonce-state/{account} (per the
        PermitSingle approval doc). Returns (anchor, bitmap_index)."""
        try:
            resp = self._unwrap(
                await self._get(f"/v1/auth/nonce-state/{account}")
            )
        except RiseRestError as exc:
            # Fall back to /v1/nonce-state/{account} if the auth-scoped
            # path doesn't exist on this deployment.
            if exc.code == 404:
                resp = self._unwrap(
                    await self._get(f"/v1/nonce-state/{account}")
                )
            else:
                raise
        anchor = int(resp.get("nonce_anchor") or
                     resp.get("anchor") or 0)
        bitmap = int(resp.get("current_bitmap_index") or
                     resp.get("nonce_bitmap_index") or
                     resp.get("bitmap_index") or 0)
        return anchor, bitmap

    # ---- account ----------------------------------------------------
    async def get_portfolio(self, account: str | None = None) -> dict:
        params = {"account": account} if account else None
        return self._unwrap(await self._get("/v1/portfolio/details", params=params))

    async def get_open_orders(self, account: str | None = None,
                                market_id: int | None = None) -> dict:
        params: dict[str, Any] = {}
        if account: params["account"] = account
        if market_id is not None: params["market_id"] = market_id
        return self._unwrap(await self._get("/v1/orders/open", params=params or None))

    # ---- orders -----------------------------------------------------
    async def place_order(self, *, market_id: int, side: int,
                            size_steps: int, price_ticks: int,
                            post_only: bool = True,
                            reduce_only: bool = False,
                            time_in_force: int = 0,    # 0=GTC
                            order_type: int = 1,        # 0=Market, 1=Limit
                            stp_mode: int = 0,
                            ttl_units: int = 0,
                            client_order_id: int = 0,
                            builder_id: int = 0,
                            no_retry: bool = False) -> dict:
        """Place an order. Hash is computed over (size_steps, price_ticks)
        directly per the risex-client SDK encodeOrder."""
        anchor, bitmap = await self.get_nonce_state(self.signer.account_address)
        permit = self.signer.sign_order_permit(
            market_id=market_id, size_steps=size_steps, price_ticks=price_ticks,
            side=side, post_only=post_only, reduce_only=reduce_only,
            stp_mode=stp_mode, order_type=order_type,
            time_in_force=time_in_force, ttl_units=ttl_units,
            builder_id=builder_id, client_order_id=client_order_id,
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
        )
        body = {
            "market_id":       int(market_id),
            "side":            int(side),
            "size_steps":      int(size_steps),
            "price_ticks":     int(price_ticks),
            "post_only":       bool(post_only),
            "reduce_only":     bool(reduce_only),
            "stp_mode":        int(stp_mode),
            "order_type":      int(order_type),
            "time_in_force":   int(time_in_force),
            "ttl_units":       int(ttl_units),
            "client_order_id": str(client_order_id),
            "builder_id":      int(builder_id),
            "permit":          permit,
        }
        if no_retry:
            body["no_retry"] = True
        return await self._post("/v1/orders/place", body)

    async def cancel_order(self, *, market_id: int, order_id: int | str,
                            resting_order_id: int | None = None,
                            no_retry: bool = False) -> dict:
        """Cancel an order. resting_order_id is the uint64 the signature
        commits to. If omitted, looked up via /v1/orders/open by matching
        order_id — exactly how risex-client does it."""
        if resting_order_id is None:
            open_resp = await self.get_open_orders(
                account=self.signer.account_address, market_id=market_id,
            )
            orders = open_resp.get("orders") or open_resp.get("data", {}).get("orders") or []
            order_id_str = str(order_id)
            match = next((o for o in orders if str(o.get("order_id")) == order_id_str), None)
            if not match or not match.get("resting_order_id"):
                raise RiseRestError(
                    400,
                    f"Could not find resting_order_id for order {order_id_str}. "
                    f"Pass it explicitly or ensure the order is still open.",
                    {"open_count": len(orders)},
                )
            resting_order_id = int(match["resting_order_id"])
        anchor, bitmap = await self.get_nonce_state(self.signer.account_address)
        permit = self.signer.sign_cancel_permit(
            market_id=market_id, resting_order_id=resting_order_id,
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
        )
        body = {
            "market_id": int(market_id),
            "order_id":  str(order_id),
            "permit":    permit,
            "no_retry":  bool(no_retry),
        }
        return await self._post("/v1/orders/cancel", body)

    async def cancel_all_orders(self, market_id: int) -> dict:
        anchor, bitmap = await self.get_nonce_state(self.signer.account_address)
        permit = self.signer.sign_cancel_all_permit(
            market_id=market_id,
            nonce_anchor=anchor, nonce_bitmap_index=bitmap,
        )
        body = {
            "market_id": int(market_id),
            "permit":    permit,
        }
        return await self._post("/v1/orders/cancel-all", body)


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
