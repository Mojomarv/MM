"""Rise diagnostic probe — call from accounts/risex1/.

Dumps everything we need to unblock the Rise bot in one run:
  1. Raw /v1/markets response (so we see the actual field shapes)
  2. Clean list of available base_asset_symbols (drop these into
     PAIRS_INCLUDE)
  3. Raw /v1/auth/eip712-domain response (so we see why our parser
     produced chain=0 contract='' last time)
  4. /v1/portfolio/details snapshot (confirms REST auth works even on
     an unfunded account — should return zero balances, not 401)

Run from accounts/risex1:
    python ../../bots/mm/risex_mm/_probe.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import RiseSigner, RiseRest, EIP712Domain  # noqa: E402


def dump(label: str, obj) -> None:
    print(f"\n{'─' * 78}")
    print(f"  {label}")
    print(f"{'─' * 78}")
    print(json.dumps(obj, indent=2, default=str)[:4000])


async def main() -> None:
    load_dotenv(".env")

    base_url = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

    # Use a placeholder domain — none of the unsigned REST calls need a
    # real one. (Portfolio uses an account query, not a signature.)
    placeholder = EIP712Domain(name="x", version="1", chain_id=0,
                                 verifying_contract="0x0000000000000000000000000000000000000000")
    signer = RiseSigner(account, signer_a, signer_k, placeholder)
    rest = RiseRest(base_url, signer)

    print(f"REST base URL: {base_url}")
    print(f"Account:       {account}")
    print(f"Signer:        {signer_a}")

    # ---- 1. Markets ------------------------------------------------
    try:
        markets_resp = await rest.get_markets()
    except Exception as exc:  # noqa: BLE001
        markets_resp = {"_error": f"{type(exc).__name__}: {exc}"}
    dump("Raw /v1/markets response (first 4 KB)", markets_resp)

    # Extract symbols cleanly
    print(f"\n{'─' * 78}")
    print(f"  AVAILABLE TICKERS (paste into PAIRS_INCLUDE)")
    print(f"{'─' * 78}")
    symbols = []
    for m in (markets_resp.get("markets") or []):
        sym = (m.get("base_asset_symbol") or "").upper()
        avail = m.get("available", False)
        if sym:
            symbols.append((sym, avail, m.get("market_id")))
    if not symbols:
        print("  (none found — check raw response above for field name)")
    else:
        active = sorted(s for s, a, _ in symbols if a)
        inactive = sorted(s for s, a, _ in symbols if not a)
        print(f"  Active ({len(active)}):    {','.join(active) if active else '(none)'}")
        print(f"  Inactive ({len(inactive)}): {','.join(inactive) if inactive else '(none)'}")
        print(f"\n  PAIRS_INCLUDE={','.join(active[:10])}")
        if len(active) > 10:
            print(f"  (truncated to 10; full active set = {len(active)} markets)")

    # ---- 2. EIP-712 domain ----------------------------------------
    try:
        domain_resp = await rest.get_eip712_domain()
    except Exception as exc:  # noqa: BLE001
        domain_resp = {"_error": f"{type(exc).__name__}: {exc}"}
    dump("Raw /v1/auth/eip712-domain response", domain_resp)

    # ---- 3. Portfolio (auth check) --------------------------------
    try:
        portfolio_resp = await rest.get_portfolio(account=account)
    except Exception as exc:  # noqa: BLE001
        portfolio_resp = {"_error": f"{type(exc).__name__}: {exc}"}
    dump("Raw /v1/portfolio/details response", portfolio_resp)

    await rest.close()

    print(f"\n{'─' * 78}")
    print("  Next steps")
    print(f"{'─' * 78}")
    print("  1. Pick symbols from the active list and put them in")
    print("     accounts/risex1/.env -> PAIRS_INCLUDE=<comma-separated>")
    print("  2. If the EIP-712 domain above has real chain/contract values,")
    print("     copy them into the four RISE_EIP712_* env vars.")
    print("  3. python start.py --venue rise --paused")


if __name__ == "__main__":
    asyncio.run(main())
