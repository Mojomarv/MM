"""Test different expiration units / field names for /v1/auth/login.
"""
from __future__ import annotations
import asyncio, json, os, sys, time
from pathlib import Path
import aiohttp
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from rise_client import RiseSigner, RiseRest, EIP712Domain  # noqa: E402


async def main() -> None:
    load_dotenv(".env")
    base     = os.environ["RISE_REST_BASE_URL"]
    account  = os.environ["RISE_ACCOUNT_ADDRESS"]
    signer_a = os.environ["RISE_SIGNER_ADDRESS"]
    signer_k = os.environ["RISE_SIGNER_PRIVATE_KEY"]

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

    V2_TYPES = {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        "RegisterV2": [
            {"name": "signer",  "type": "address"},
            {"name": "message", "type": "string"},
            {"name": "nonce",   "type": "uint256"},
        ],
    }

    async def fresh_nonce(s):
        async with s.get(f"{base}/v1/auth/nonce?account={account}") as r:
            body = await r.json()
        return (body.get("data") or {}).get("nonce") or ""

    now_s = int(time.time())
    candidates = [
        # (label, expiration field NAME on wire, expiration VALUE on wire)
        ("expiration=seconds(+1yr)",         "expiration", now_s + 365*24*3600),
        ("expiration=seconds(+1day)",        "expiration", now_s + 24*3600),
        ("expiration=seconds(+1h)",          "expiration", now_s + 3600),
        ("expiration=ms(+1yr)",              "expiration", (now_s + 365*24*3600)*1000),
        ("expiration=ms(+1day)",             "expiration", (now_s + 24*3600)*1000),
        ("expiration=ns(+1yr)",              "expiration", (now_s + 365*24*3600)*1_000_000_000),
        ("deadline=seconds(+1yr)",           "deadline",   now_s + 365*24*3600),
        ("deadline=ms(+1yr)",                "deadline",   (now_s + 365*24*3600)*1000),
        ("deadline=seconds(+1h)",            "deadline",   now_s + 3600),
        ("expires_at=seconds(+1yr)",         "expires_at", now_s + 365*24*3600),
        ("expires_at=ms(+1yr)",              "expires_at", (now_s + 365*24*3600)*1000),
        ("expiry=seconds(+1yr)",             "expiry",     now_s + 365*24*3600),
    ]

    async with aiohttp.ClientSession() as s:
        for label, key, value in candidates:
            nonce_hex = await fresh_nonce(s)
            sig = signer._sign_typed(V2_TYPES, "RegisterV2", {
                "signer":  signer.signer_address,
                "message": "sign in with RISEx",
                "nonce":   int(nonce_hex, 16),
            })
            body = {
                "account":   account,
                "signer":    signer_a,
                "message":   "sign in with RISEx",
                "nonce":     nonce_hex,
                key:         value,
                "signature": sig,
            }
            async with s.post(f"{base}/v1/auth/login", json=body) as r:
                text = await r.text()
                try:
                    err = json.loads(text).get("error", {}).get("message", text[:200])
                except Exception:
                    err = text[:200]
                print(f"  {label:38s} [{r.status}] {err[:200]}")


if __name__ == "__main__":
    asyncio.run(main())
