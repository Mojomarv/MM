"""Probe /v1/auth/login POST shapes — the 501 'Method Not Allowed' on GET
suggests the path exists and accepts another method. Try POST with
various body shapes to see if it issues a JWT we can use as Bearer.

Run from accounts/risex1/:
    python ../../bots/mm/risex_mm/_probe_login.py
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import json
import os
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import (  # noqa: E402
    RiseSigner, RiseRest, EIP712Domain,
)


async def main() -> None:
    load_dotenv(".env")
    base     = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

    placeholder = EIP712Domain(name="x", version="1", chain_id=0,
                                 verifying_contract="0x0000000000000000000000000000000000000000")
    signer = RiseSigner(account, signer_a, signer_k, placeholder)
    rest = RiseRest(base, signer)

    # Fetch real EIP-712 domain so we can sign properly
    domain_resp = await rest.get_eip712_domain()
    real_domain = EIP712Domain(
        name=str(domain_resp.get("name") or "RISEx"),
        version=str(domain_resp.get("version") or "1"),
        chain_id=int(domain_resp.get("chain_id") or 4153),
        verifying_contract=str(domain_resp.get("verifying_contract") or
                                 "0x0D919DAA3f12AE715744Eb648c00066c5DBd66f0"),
    )
    signer.domain = real_domain
    print(f"Domain: {real_domain.as_dict()}\n")

    # Build the SAME RegisterV2 sign-in payload that worked for WS auth
    nonce_ts = int(time.time())
    message = "WebSocket Authentication"

    # Build an EIP-712 typed-data signature using the same Register schema
    # that the WS auth uses — many APIs accept this for REST too.
    from rise_client import _WS_AUTH_TYPES, _WS_AUTH_PRIMARY
    sig_hex_ws = signer._sign_typed(_WS_AUTH_TYPES, _WS_AUTH_PRIMARY, {
        "signer":  signer.signer_address,
        "message": message,
        "nonce":   nonce_ts,
    })

    async def post(path: str, body: dict, label: str = "") -> str:
        async with aiohttp.ClientSession() as s:
            async with s.post(f"{base}{path}", json=body) as r:
                text = await r.text()
                tag = f" ({label})" if label else ""
                return f"  POST {path}{tag}\n    [{r.status}] {text[:600]}"

    async def get(path: str, headers: dict | None = None,
                   label: str = "") -> str:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}{path}", headers=headers or {}) as r:
                text = await r.text()
                tag = f" ({label})" if label else ""
                return f"  GET {path}{tag}\n    [{r.status}] {text[:600]}"

    # ─────────────────────────────────────────────────────────────
    # Try POST /v1/auth/login with various shapes
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("POST /v1/auth/login shapes")
    print("=" * 72)
    print(await post("/v1/auth/login", {}, "empty"))
    print()
    print(await post("/v1/auth/login",
                       {"account": account, "signer": signer_a},
                       "account+signer only"))
    print()
    print(await post("/v1/auth/login", {
        "account":   account,
        "signer":    signer_a,
        "message":   message,
        "nonce":     nonce_ts,
        "signature": sig_hex_ws,
    }, "full WS-auth-style payload"))
    print()
    print(await post("/v1/auth/login", {
        "account":   account,
        "signer":    signer_a,
        "message":   message,
        "nonce":     str(nonce_ts),       # nonce as string
        "signature": sig_hex_ws,
    }, "string nonce"))
    print()

    # ─────────────────────────────────────────────────────────────
    # Try the v2 nonce flow if /v1/auth/nonce returns a server nonce
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Try v2 nonce flow")
    print("=" * 72)
    print(await get(f"/v1/auth/nonce?account={account}", label="get server nonce"))
    print()

    # Sign that with RegisterV2 if we got one
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{base}/v1/auth/nonce?account={account}") as r:
                nonce_resp = await r.json()
        nonce_str = (nonce_resp.get("data") or {}).get("nonce")
        if nonce_str:
            if not nonce_str.startswith("0x"):
                nonce_str = "0x" + nonce_str
            # Sign RegisterV2 with this nonce
            v2_types = {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "RegisterV2": [
                    {"name": "signer", "type": "address"},
                    {"name": "message", "type": "string"},
                    {"name": "nonce", "type": "uint256"},
                ],
            }
            sig_v2 = signer._sign_typed(v2_types, "RegisterV2", {
                "signer":  signer.signer_address,
                "message": message,
                "nonce":   nonce_str,
            })
            print(await post("/v1/auth/login", {
                "account":   account,
                "signer":    signer_a,
                "message":   message,
                "nonce":     nonce_str,
                "signature": sig_v2,
            }, "v2 with server nonce + RegisterV2 sig"))
    except Exception as exc:  # noqa: BLE001
        print(f"  v2 flow setup failed: {exc}")
    print()

    # Also try POST /v1/auth/token, /v1/auth/session as alternates
    print("=" * 72)
    print("Alternates")
    print("=" * 72)
    full_payload = {
        "account":   account,
        "signer":    signer_a,
        "message":   message,
        "nonce":     nonce_ts,
        "signature": sig_hex_ws,
    }
    for p in ["/v1/auth/token", "/v1/auth/session", "/v1/auth/jwt"]:
        print(await post(p, full_payload, "full payload"))
    print()

    await rest.close()


if __name__ == "__main__":
    asyncio.run(main())
