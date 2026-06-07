"""One-shot helper: fetches Rise's EIP-712 domain from
/v1/auth/eip712-domain and prints the values to drop into .env.

Run from accounts/risex1:
    python ../../bots/mm/risex_mm/_fetch_domain.py
"""
from __future__ import annotations
import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import RiseSigner, RiseRest, EIP712Domain  # noqa: E402


async def main():
    load_dotenv(".env")
    base_url = os.environ["RISE_REST_BASE_URL"]
    # Need a signer to construct the REST client, but we won't sign anything.
    placeholder = EIP712Domain(name="x", version="1", chain_id=0,
                                 verifying_contract="0x0000000000000000000000000000000000000000")
    signer = RiseSigner(
        os.environ["RISE_ACCOUNT_ADDRESS"],
        os.environ["RISE_SIGNER_ADDRESS"],
        os.environ["RISE_SIGNER_PRIVATE_KEY"],
        placeholder,
    )
    rest = RiseRest(base_url, signer)
    try:
        resp = await rest.get_eip712_domain()
    finally:
        await rest.close()

    dom = resp.get("domain") or resp
    print("=== Rise EIP-712 domain ===")
    print(f"RISE_EIP712_NAME={dom.get('name') or 'RiseXAuthorization'}")
    print(f"RISE_EIP712_VERSION={dom.get('version') or '1'}")
    print(f"RISE_EIP712_CHAIN_ID={dom.get('chainId') or dom.get('chain_id') or '0'}")
    print(f"RISE_EIP712_VERIFYING_CONTRACT={dom.get('verifyingContract') or dom.get('verifying_contract') or '0x0'}")
    print()
    print("Paste these lines into accounts/risex1/.env (replacing the placeholders).")


if __name__ == "__main__":
    asyncio.run(main())
