"""Probe Rise mainnet to figure out the actual order-placement auth flow.

Tests a series of hypotheses about how orders should be submitted —
each one is a real REST call to Rise. We send tiny BTC orders at
silly prices ($1) so even if anything succeeds, nothing will fill.

Run from accounts/risex1/:
    python ../../bots/mm/risex_mm/_probe_auth_flow.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import (  # noqa: E402
    RiseSigner, RiseRest, EIP712Domain,
)


# Test order: tiny BTC buy at $1 — would never fill even if accepted.
TEST_ORDER_FIELDS = {
    "market_id":       1,
    "side":            0,
    "size_steps":      1,         # 0.000001 BTC ≈ $0.06
    "price_ticks":     10,        # $1.0
    "post_only":       True,
    "reduce_only":     False,
    "stp_mode":        0,
    "order_type":      1,
    "time_in_force":   0,
    "ttl_units":       0,
    "builder_id":      0,
    "client_order_id": "0",
}


async def main() -> None:
    load_dotenv(".env")
    base = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

    placeholder = EIP712Domain(name="x", version="1", chain_id=0,
                                 verifying_contract="0x0000000000000000000000000000000000000000")
    signer = RiseSigner(account, signer_a, signer_k, placeholder)
    rest = RiseRest(base, signer)

    async def post_raw(path: str, body: dict, headers: dict | None = None) -> str:
        async with aiohttp.ClientSession() as s:
            url = f"{base}{path}"
            async with s.post(url, json=body,
                                headers=headers or {"Content-Type": "application/json"}) as r:
                text = await r.text()
                return f"[{r.status}] {text[:400]}"

    async def get_raw(path: str, headers: dict | None = None) -> str:
        async with aiohttp.ClientSession() as s:
            url = f"{base}{path}"
            async with s.get(url, headers=headers or {}) as r:
                text = await r.text()
                return f"[{r.status}] {text[:400]}"

    print(f"Base URL: {base}")
    print(f"Account:  {account}")
    print(f"Signer:   {signer_a}")
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 1: POST /v1/orders/place with NO permit field at all
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 1: POST /v1/orders/place WITHOUT permit field")
    print("Hypothesis: 'no permit needed' — maybe OperatorHub does everything")
    print("=" * 72)
    body = {**TEST_ORDER_FIELDS, "no_retry": False}
    print(await post_raw("/v1/orders/place", body))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 2: With empty permit {}
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 2: POST /v1/orders/place with permit={} (empty object)")
    print("=" * 72)
    body = {**TEST_ORDER_FIELDS, "permit": {}, "no_retry": False}
    print(await post_raw("/v1/orders/place", body))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 3: With just account + signer in permit (no signature)
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 3: Permit with just account+signer, no signature")
    print("=" * 72)
    body = {**TEST_ORDER_FIELDS,
             "permit": {"account": account, "signer": signer_a},
             "no_retry": False}
    print(await post_raw("/v1/orders/place", body))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 4: Probe for system config — finds OperatorHub address
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 4: GET /v1/system/config — find OperatorHub address")
    print("=" * 72)
    print(await get_raw("/v1/system/config"))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 5: Check approve-single status for our account
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 5: GET /v1/auth/allowances?account=... — is OperatorHub approved?")
    print("=" * 72)
    print(await get_raw(f"/v1/auth/allowances?account={account}"))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 6: Try JWT login endpoints (common paths)
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 6: Probe JWT login endpoints")
    print("=" * 72)
    for path in ["/v1/auth/login", "/v1/auth/token", "/v1/login",
                  "/v1/auth/jwt", "/auth/login"]:
        print(f"  GET {path}: {(await get_raw(path))[:200]}")
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 7: Send order with Authorization: Bearer <signer-as-token>
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 7: POST /v1/orders/place with Bearer signer-address header")
    print("=" * 72)
    body = {**TEST_ORDER_FIELDS, "no_retry": False}
    print(await post_raw(
        "/v1/orders/place", body,
        headers={"Content-Type": "application/json",
                  "Authorization": f"Bearer {signer_a}"},
    ))
    print()

    # ─────────────────────────────────────────────────────────────
    # Test 8: Probe for batch order / TPSL endpoints (these mention
    # OperatorHub in their docs)
    # ─────────────────────────────────────────────────────────────
    print("=" * 72)
    print("Test 8: Alternate order endpoints")
    print("=" * 72)
    for path in ["/v1/orders/place-batch", "/v1/perps/orders",
                  "/v1/orders", "/v1/orders/create"]:
        body = {**TEST_ORDER_FIELDS}
        result = await post_raw(path, body)
        print(f"  POST {path}: {result[:200]}")
    print()

    await rest.close()

    print("=" * 72)
    print("Interpret the results:")
    print("=" * 72)
    print("  - [400] 'invalid permit / signature' → permit IS required")
    print("  - [400] 'invalid market_id'           → permit NOT required, order shape OK")
    print("  - [200] order_id returned              → IT WORKED, paste the result here")
    print("  - [401] / [403]                       → JWT flow likely")
    print("  - [404] on system/config or allowances → not those paths")
    print()


if __name__ == "__main__":
    asyncio.run(main())
