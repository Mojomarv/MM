"""WS-login diagnostic: tries 8 plausible OndoPerps auth variants and
reports which one(s) the live server accepts.

Run from accounts/ondo1/:
    python ../../bots/mm/ondo_mm/_test_ws.py

It uses your existing ONDO_KEY_ID / ONDO_API_SECRET / ONDO_WS_URL from
.env. No orders placed; the script just connects, sends a login frame,
waits for {"type":"loggedIn"} or {"type":"error"}, then disconnects.

When one variant works, paste the line marked `>>> PASS` back here and
I'll patch ondo_client.py to use that exact scheme.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
import websockets

# Load .env from current working dir (accounts/ondo1/.env).
load_dotenv(".env")

KEY_ID  = os.environ["ONDO_KEY_ID"]
SECRET  = os.environ["ONDO_API_SECRET"]
WS_URL  = os.environ["ONDO_WS_URL"]

# Stripped variants (without prefix).
KEY_RAW    = KEY_ID.split("_", 1)[-1] if "_" in KEY_ID else KEY_ID
SECRET_RAW = SECRET.split("_", 1)[-1] if "_" in SECRET else SECRET


def hmac_hex(secret: str, msg: str) -> str:
    return hmac.new(secret.encode("utf-8"), msg.encode("utf-8"),
                      hashlib.sha256).hexdigest()


def hmac_hex_rawkey(secret_hex: str, msg: str) -> str:
    """HMAC with the secret hex-decoded to raw bytes. Common gotcha when an
    exchange displays the secret as hex but uses raw bytes internally."""
    try:
        key_bytes = bytes.fromhex(secret_hex)
    except ValueError:
        # secret_hex isn't valid hex — fall through to utf-8 encode so the
        # variant still completes (just won't match anything).
        key_bytes = secret_hex.encode("utf-8")
    return hmac.new(key_bytes, msg.encode("utf-8"),
                      hashlib.sha256).hexdigest()


def build_variants() -> list[tuple[str, dict]]:
    """Each variant returns (name, login_payload). Time-sensitive values
    are computed fresh inside the lambda when each variant runs."""
    ts_ms = str(int(time.time() * 1000))
    ts_s  = str(int(time.time()))

    return [
        # Format hint:        time-in-json | hmac-key       | hmac-msg
        ("01 ms-str/full-sec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login{ts_ms}")}}),
        ("02 ms-int/full-sec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":int(ts_ms),
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login{ts_ms}")}}),
        ("03 ms-str/raw-sec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET_RAW, f"ondo_perps_ws_login{ts_ms}")}}),
        ("04 ms-int/raw-sec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":int(ts_ms),
          "sign":hmac_hex(SECRET_RAW, f"ondo_perps_ws_login{ts_ms}")}}),
        ("05 sec-str/full-sec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_s,
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login{ts_s}")}}),
        ("06 ms-str/full-sec/key+time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"{KEY_ID}{ts_ms}")}}),
        ("07 ms-str/full-sec/time+prefix",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"{ts_ms}ondo_perps_ws_login")}}),
        ("08 raw-key/ms-str/full-sec/prefix-time",
         {"op":"login","args":{"key":KEY_RAW,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login{ts_ms}")}}),
        # Hex-decoded raw secret variants — common gotcha when the secret
        # is displayed as hex but used as raw bytes inside the HMAC.
        ("09 ms-str/hexbytes-rawsec/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex_rawkey(SECRET_RAW, f"ondo_perps_ws_login{ts_ms}")}}),
        ("10 ms-str/hexbytes-rawsec/time-only",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex_rawkey(SECRET_RAW, ts_ms)}}),
        # Just the timestamp (mirrors REST where there's no fixed prefix).
        ("11 ms-str/full-sec/time-only",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, ts_ms)}}),
        # Separator variants — the docs may have stripped a colon or
        # newline from the prefix.
        ("12 ms-str/full-sec/prefix:time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login:{ts_ms}")}}),
        ("13 ms-str/full-sec/prefix\\n-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"ondo_perps_ws_login\n{ts_ms}")}}),
        # REST-style: timestamp + METHOD + path
        ("14 ms-str/full-sec/REST-style GET-path",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"{ts_ms}GET/v1/auth/ws-login")}}),
        # The "auth" word with key
        ("15 ms-str/full-sec/key+time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex(SECRET, f"{KEY_RAW}{ts_ms}")}}),
        # Just the prefix string under raw-bytes key (in case docs are
        # right about the message but wrong about the key encoding).
        ("16 ms-str/hexbytes-full/prefix-time",
         {"op":"login","args":{"key":KEY_ID,"time":ts_ms,
          "sign":hmac_hex_rawkey(SECRET, f"ondo_perps_ws_login{ts_ms}")}}),
    ]


async def probe(name: str, payload: dict) -> str:
    """Returns 'PASS', 'FAIL: <reason>', or a connection error string."""
    try:
        async with websockets.connect(WS_URL, close_timeout=2) as ws:
            await ws.send(json.dumps(payload))
            # Up to 4s for a response.
            try:
                for _ in range(8):
                    raw = await asyncio.wait_for(ws.recv(), timeout=4.0)
                    msg = json.loads(raw)
                    t = msg.get("type")
                    if t == "loggedIn":
                        return "PASS"
                    if t == "error":
                        return f"FAIL: {msg.get('msg') or msg}"
                    # Ignore non-auth-related early frames (rare).
                return "FAIL: no auth response after 8 frames"
            except asyncio.TimeoutError:
                return "FAIL: no response (timeout 4s)"
    except websockets.exceptions.ConnectionClosedError as exc:
        # Server closed without a frame — usually means rejected auth
        # silently. Surface the close code/reason if any.
        code   = getattr(exc, "code", None)
        reason = getattr(exc, "reason", None) or "no close frame"
        return f"FAIL: ConnectionClosed code={code} reason={reason!r}"
    except Exception as exc:  # noqa: BLE001
        return f"FAIL: {type(exc).__name__}: {exc}"


async def main() -> None:
    print(f"WS URL:  {WS_URL}")
    print(f"KEY_ID:  {KEY_ID}")
    print(f"  raw:   {KEY_RAW}")
    print(f"SECRET:  {SECRET[:25]}…  (prefix len: {len(SECRET) - len(SECRET_RAW)})")
    print(f"  raw:   {SECRET_RAW[:8]}…{SECRET_RAW[-4:]}")
    print()

    results: list[tuple[str, str]] = []
    for name, payload in build_variants():
        # Truncate signature in the printed payload for readability.
        printable = json.loads(json.dumps(payload))   # deep copy
        sig = printable["args"]["sign"]
        printable["args"]["sign"] = f"{sig[:12]}…{sig[-6:]}"
        print(f"--- {name} ---")
        print(f"  send: {printable}")
        result = await probe(name, payload)
        print(f"  {'>>> PASS' if result == 'PASS' else f'    {result}'}")
        results.append((name, result))
        # Don't blow the 25 req/s WS rate limit.
        await asyncio.sleep(0.5)
        print()

    print("=== SUMMARY ===")
    any_pass = False
    for name, res in results:
        marker = ">>> PASS  " if res == "PASS" else "    FAIL  "
        print(f"  {marker}{name}    {'' if res == 'PASS' else f'({res})'}")
        if res == "PASS":
            any_pass = True
    if not any_pass:
        print("\nNo variant succeeded. Possible causes:")
        print("  * The API key may not have a 'websocket' permission scope")
        print("    on it. Check the OndoPerps web UI -> API Keys.")
        print("  * The signing scheme is different from anything tried here.")
        print("    Paste this output back and we'll iterate.")


if __name__ == "__main__":
    asyncio.run(main())
