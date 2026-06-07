"""Test the captured browser session token against Rise REST.

Token format observed: <32-byte hex>|<32-byte hex>  (URL-encoded with %7C
between the two halves). Likely a session-id|hmac pair, not a JWT.

Run from accounts/risex1:
    python ../../bots/mm/risex_mm/_probe_token.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import urllib.parse
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

# Paste the captured token here (URL-encoded form is fine):
TOKEN_RAW = "eac11eb6cbdb4b5c6e02139a355d54bb4dcba713d47f91552b5075f87f06b54d%7Ca9c166f30733b486508e0b4bfc53705db6a0cb3b336e8caa43d981057845e68b"
TOKEN_DECODED = urllib.parse.unquote(TOKEN_RAW)


# A tiny test order — won't fill (price = $1, BTC trades at ~$60k)
TEST_ORDER = {
    "market_id":       1,
    "side":            0,
    "size_steps":      1,
    "price_ticks":     10,
    "post_only":       True,
    "reduce_only":     False,
    "stp_mode":        0,
    "order_type":      1,
    "time_in_force":   0,
    "client_order_id": "0",
    "ttl_units":       0,
    "builder_id":      0,
    "no_retry":        False,
}


async def main() -> None:
    load_dotenv(".env")
    base    = os.environ["RISE_REST_BASE_URL"]
    account = os.environ["RISE_ACCOUNT_ADDRESS"]

    print(f"Base URL:        {base}")
    print(f"Account:         {account}")
    print(f"Token (raw):     {TOKEN_RAW[:50]}…{TOKEN_RAW[-10:]}")
    print(f"Token (decoded): {TOKEN_DECODED[:50]}…{TOKEN_DECODED[-10:]}")
    print()

    # Header / cookie variants to try
    variants = [
        ("Authorization: Bearer <decoded>",     {"Authorization": f"Bearer {TOKEN_DECODED}"}),
        ("Authorization: Bearer <raw>",         {"Authorization": f"Bearer {TOKEN_RAW}"}),
        ("X-Auth-Token: <decoded>",             {"X-Auth-Token": TOKEN_DECODED}),
        ("X-Auth-Token: <raw>",                 {"X-Auth-Token": TOKEN_RAW}),
        ("X-Session-Token: <decoded>",          {"X-Session-Token": TOKEN_DECODED}),
        ("X-Session: <decoded>",                {"X-Session": TOKEN_DECODED}),
        ("Cookie: session=<raw>",               {"Cookie": f"session={TOKEN_RAW}"}),
        ("Cookie: rise-session=<raw>",          {"Cookie": f"rise-session={TOKEN_RAW}"}),
        ("Cookie: __session=<raw>",             {"Cookie": f"__session={TOKEN_RAW}"}),
        ("Cookie: auth=<raw>",                  {"Cookie": f"auth={TOKEN_RAW}"}),
        ("Cookie: token=<raw>",                 {"Cookie": f"token={TOKEN_RAW}"}),
    ]

    async with aiohttp.ClientSession() as s:
        print("=" * 72)
        print("TEST 1: GET /v1/portfolio/details (auth required — easy verify)")
        print("=" * 72)
        for label, headers in variants:
            async with s.get(f"{base}/v1/portfolio/details?account={account}",
                              headers=headers) as r:
                text = await r.text()
                short = text[:120].replace("\n", " ")
                marker = " <-- LIKELY WORKING" if r.status == 200 else ""
                print(f"  [{r.status}] {label:45s} {short}{marker}")
        print()

        print("=" * 72)
        print("TEST 2: POST /v1/orders/place — the real target")
        print("=" * 72)
        for label, headers in variants:
            h = {**headers, "Content-Type": "application/json"}
            async with s.post(f"{base}/v1/orders/place",
                                json=TEST_ORDER, headers=h) as r:
                text = await r.text()
                short = text[:120].replace("\n", " ")
                marker = " <-- WORKED" if r.status == 200 else ""
                if "permit" not in short.lower() and r.status != 401:
                    marker = " <-- different error than 'permit required'!"
                print(f"  [{r.status}] {label:45s} {short}{marker}")


if __name__ == "__main__":
    asyncio.run(main())
