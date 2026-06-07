"""With session auth working, try minimal permit shapes for /v1/orders/place."""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import urllib.parse
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))

TOKEN_RAW = "eac11eb6cbdb4b5c6e02139a355d54bb4dcba713d47f91552b5075f87f06b54d%7Ca9c166f30733b486508e0b4bfc53705db6a0cb3b336e8caa43d981057845e68b"

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
    signer  = os.environ["RISE_SIGNER_ADDRESS"]
    now_s = int(time.time())

    h = {"X-Auth-Token": TOKEN_RAW, "Content-Type": "application/json"}

    permit_variants = [
        ("empty {}", {}),
        ("account only", {"account": account}),
        ("account+signer", {"account": account, "signer": signer}),
        ("use_operator true", {"use_operator": True}),
        ("operator: true", {"operator": True}),
        ("via OperatorHub flag",
            {"account": account, "use_operator_hub": True}),
        ("account+deadline",
            {"account": account, "deadline": now_s + 3600}),
        ("with empty sig",
            {"account": account, "signer": signer, "signature": ""}),
        ("with empty sig + nonce",
            {"account": account, "signer": signer,
             "nonce_anchor": "0", "nonce_bitmap_index": 0,
             "deadline": now_s + 3600, "signature": ""}),
        ("use_session=true",
            {"account": account, "signer": signer, "use_session": True}),
        ("via_session=true",
            {"account": account, "signer": signer, "via_session": True}),
    ]

    async with aiohttp.ClientSession() as s:
        for label, permit in permit_variants:
            body = {**TEST_ORDER, "permit": permit}
            async with s.post(f"{base}/v1/orders/place",
                                json=body, headers=h) as r:
                text = await r.text()
                try:
                    parsed = json.loads(text)
                    err = parsed.get("error", {}).get("message", text)[:160]
                except Exception:
                    err = text[:160]
                marker = "  <-- DIFFERENT ERROR" if "permit is required" not in err else ""
                print(f"  [{r.status}] {label:30s} {err}{marker}")


if __name__ == "__main__":
    asyncio.run(main())
