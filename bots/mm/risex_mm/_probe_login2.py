"""Probe /v1/auth/login with correct nonce handling.

Previous probe got 'nonce expired or already used' — that error means
the signature was accepted and validated, but the nonce wasn't accepted.
The fix: when signing RegisterV2 with the hex-string nonce, convert
hex → int FIRST so it gets hashed as a uint256 number (not as string
bytes).

Run:
    python ../../bots/mm/risex_mm/_probe_login2.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import RiseSigner, RiseRest, EIP712Domain  # noqa: E402


async def fresh_nonce(s: aiohttp.ClientSession, base: str, account: str) -> str:
    async with s.get(f"{base}/v1/auth/nonce?account={account}") as r:
        body = await r.json()
    return (body.get("data") or {}).get("nonce") or ""


async def main() -> None:
    load_dotenv(".env")
    base     = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

    # Fetch real EIP-712 domain
    rest = RiseRest(base, RiseSigner(
        account, signer_a, signer_k,
        EIP712Domain(name="x", version="1", chain_id=0,
                       verifying_contract="0x0000000000000000000000000000000000000000"),
    ))
    domain_resp = await rest.get_eip712_domain()
    domain = EIP712Domain(
        name=str(domain_resp.get("name") or "RISEx"),
        version=str(domain_resp.get("version") or "1"),
        chain_id=int(domain_resp.get("chain_id") or 4153),
        verifying_contract=str(domain_resp.get("verifying_contract") or ""),
    )
    signer = RiseSigner(account, signer_a, signer_k, domain)
    await rest.close()

    print(f"Domain: {domain.as_dict()}")
    print(f"Account: {account}")
    print(f"Signer:  {signer_a}\n")

    EIP712_DOMAIN = [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ]

    def types_for(primary: str, fields: list[tuple[str, str]]) -> dict:
        return {
            "EIP712Domain": EIP712_DOMAIN,
            primary: [{"name": n, "type": t} for n, t in fields],
        }

    async with aiohttp.ClientSession() as s:
        message_str = "Login"  # we'll iterate this too
        expiration = int(time.time()) + 365 * 24 * 3600   # 1 year future

        # Candidate primary types and field sets that include `expiration`
        # in the signed typed data:
        candidates = [
            ("RegisterV2", [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint256")]),
            ("RegisterV2", [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint64")]),
            ("Login",      [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint256")]),
            ("Login",      [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint64")]),
            ("LoginV2",    [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint256")]),
            ("LoginV2",    [("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint64")]),
            ("Login",      [("account","address"),("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint256")]),
            ("Login",      [("account","address"),("signer","address"),("message","string"),
                            ("nonce","uint256"),("expiration","uint64")]),
        ]
        messages = ["sign in with RISEx", "Login", "WebSocket Authentication"]

        for msg_str in messages:
            print(f"\n--- message = {msg_str!r} ---")
            for primary, fields in candidates:
                nonce_hex = await fresh_nonce(s, base, account)
                nonce_int = int(nonce_hex, 16)
                types = types_for(primary, fields)
                td_msg = {
                    "signer":     signer.signer_address,
                    "message":    msg_str,
                    "nonce":      nonce_int,
                    "expiration": expiration,
                }
                if any(f[0] == "account" for f in fields):
                    td_msg["account"] = signer.account_address
                try:
                    sig = signer._sign_typed(types, primary, td_msg)
                except Exception as exc:  # noqa: BLE001
                    print(f"  {primary:12s} {len(fields)}fields: SIGN-ERR {exc}")
                    continue
                body = {
                    "account":    account,
                    "signer":     signer_a,
                    "message":    msg_str,
                    "nonce":      nonce_hex,
                    "expiration": expiration,
                    "signature":  sig,
                }
                async with s.post(f"{base}/v1/auth/login", json=body) as r:
                    text = await r.text()
                    err = ""
                    try:
                        err = json.loads(text).get("error", {}).get("message", text)[:120]
                    except Exception:  # noqa: BLE001
                        err = text[:120]
                    print(f"  {primary:9s} fields={[f[0] for f in fields]} -> [{r.status}] {err}")
                    if r.status == 200:
                        print(f"\n  >>> WORKED <<<\n  Body: {text}")
                        return


if __name__ == "__main__":
    asyncio.run(main())
