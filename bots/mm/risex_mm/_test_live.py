"""One-shot live test: place a single tiny BTC bid far from market through
the V3 signing path. Prints the Rise response (or error) verbatim.

Run from accounts/risex1:
    python ../../bots/mm/risex_mm/_test_live.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import (  # noqa: E402
    RiseSigner, RiseRest, EIP712Domain, parse_markets_response,
)


async def main() -> None:
    load_dotenv(".env")
    base     = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

    # 1. Fetch the live EIP-712 domain
    tmp_signer = RiseSigner(
        account, signer_a, signer_k,
        EIP712Domain(name="x", version="1", chain_id=0,
                       verifying_contract="0x0000000000000000000000000000000000000000"),
    )
    tmp_rest = RiseRest(base, tmp_signer)
    dom_resp = await tmp_rest.get_eip712_domain()
    print(f"domain: {dom_resp}")
    domain = EIP712Domain(
        name=str(dom_resp.get("name") or "RISEx"),
        version=str(dom_resp.get("version") or "1"),
        chain_id=int(dom_resp.get("chain_id") or 4153),
        verifying_contract=str(dom_resp.get("verifying_contract") or ""),
    )
    await tmp_rest.close()

    # 2. Fetch markets and pick BTC, plus system config target
    real_signer = RiseSigner(account, signer_a, signer_k, domain)
    rest = RiseRest(base, real_signer)
    target = await rest.fetch_and_set_target()
    print(f"target (orders_manager): {target}")
    raw_markets = await rest.get_markets()
    print(f"first market entry: {json.dumps((raw_markets.get('markets') or [None])[0], indent=2)[:600]}")
    markets = parse_markets_response(raw_markets, {"BTC/USDC"})
    print(f"matched markets: {list(markets.keys())}")
    btc = markets["BTC/USDC"]
    print(f"market: BTC id={btc.market_id} step_size={btc.step_size} step_price={btc.step_price}")

    # 3. Place a far-from-market post-only bid (~$1000 BTC), 0.0002 BTC
    # step_size on BTC is typically 0.000001 → 0.0002 = 200 steps
    # step_price is typically 0.1 → $1000 = 10000 ticks
    size_dec  = 0.0002
    price_dec = 1000.0

    from decimal import Decimal
    def to_wei(d): return int(Decimal(str(d)) * (Decimal(10) ** 18))

    size_steps  = int(Decimal(str(size_dec))  / btc.step_size)
    price_ticks = int(Decimal(str(price_dec)) / btc.step_price)
    step_size_wei  = to_wei(btc.step_size)
    step_price_wei = to_wei(btc.step_price)

    print(f"order: size_steps={size_steps} price_ticks={price_ticks}")
    print(f"       step_size_wei={step_size_wei}")
    print(f"       step_price_wei={step_price_wei}")

    try:
        result = await rest.place_order(
            market_id=btc.market_id,
            side=0,                      # 0 = Buy
            size_steps=size_steps,
            price_ticks=price_ticks,
            post_only=True,
            order_type=1,                # Limit
            time_in_force=0,             # GTC
            stp_mode=0,
            ttl_units=0,
        )
        print(f"\n[OK] result: {json.dumps(result, indent=2)}")
    except Exception as exc:  # noqa: BLE001
        print(f"\n[ERROR] {type(exc).__name__}: {exc}")
        # If RiseRestError carry .raw, print it
        raw = getattr(exc, "raw", None)
        if raw:
            print(f"raw: {json.dumps(raw, indent=2)}")

    await rest.close()


if __name__ == "__main__":
    asyncio.run(main())
