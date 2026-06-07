"""Deterministic Rise VerifyWitness schema probe.

Takes the captured frontend POST /v1/orders/place payload (signature +
order data + permit envelope) and tries N candidate EIP-712 schemas.
The schema where ecrecover(hash, signature) returns the captured signer
address is the correct one.

Run:
    python bots/mm/risex_mm/_test_witness.py
"""
from __future__ import annotations

import base64
import json
from typing import Any

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import to_checksum_address, keccak

# ─── Captured wire payload from the rise.trade frontend ────────────────
# (chain 4153 / mainnet api.rise.trade)
DOMAIN = {
    "name":              "RISEx",
    "version":           "1",
    "chainId":           4153,
    "verifyingContract": to_checksum_address("0x0D919DAA3f12AE715744Eb648c00066c5DBd66f0"),
}

ORDER_BODY = {
    "market_id":       1,
    "size_steps":      333,
    "price_ticks":     300000,
    "side":            0,
    "post_only":       False,
    "reduce_only":     False,
    "stp_mode":        0,
    "order_type":      1,
    "time_in_force":   0,
    "client_order_id": 0,
    "ttl_units":       0,
    "builder_id":      0,
}

PERMIT_ENVELOPE = {
    "account":            to_checksum_address("0x820A276DE42f7C727b7A925635E72B018E56e02c"),
    "signer":             to_checksum_address("0x329234baCA3466442022140b9299475C968abCC7"),
    "nonce_anchor":       3,
    "nonce_bitmap_index": 0,
    "deadline":           1781469845,
}

SIGNATURE_B64 = "Mb61JG7+fCHoZqeXcufFeONCaMiqHNj6XZuCdIi8p3pIaFA4WV25Sl1S4prHH2VBwX72Amm5zSb9A+lrAXGjQg=="

EXPECTED_SIGNER = PERMIT_ENVELOPE["signer"]


# ─── Recover the 65-byte (r,s,v) signature from base64 EIP-2098 compact ──
def decode_compact_sig(b64: str) -> bytes:
    raw = base64.b64decode(b64)
    assert len(raw) == 64, f"expected 64-byte compact sig, got {len(raw)}"
    r = raw[:32]
    yps_int = int.from_bytes(raw[32:], "big")
    y_parity = (yps_int >> 255) & 1
    s = yps_int & ((1 << 255) - 1)
    v = 27 + y_parity
    return r + s.to_bytes(32, "big") + bytes([v])


SIG_BYTES = decode_compact_sig(SIGNATURE_B64)


# ─── EIP-712 domain type (constant across all variants) ──────────────
EIP712_DOMAIN_TYPE = [
    {"name": "name",              "type": "string"},
    {"name": "version",           "type": "string"},
    {"name": "chainId",           "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]


# ─── Helpers for variant construction ────────────────────────────────
ORDER_FIELDS_DOC_ORDER = [
    ("market_id",       "uint16"),
    ("side",            "uint8"),
    ("size_steps",      "uint32"),
    ("price_ticks",     "uint24"),
    ("post_only",       "bool"),
    ("reduce_only",     "bool"),
    ("stp_mode",        "uint8"),
    ("order_type",      "uint8"),
    ("time_in_force",   "uint8"),
    ("client_order_id", "uint64"),
    ("ttl_units",       "uint16"),
    ("builder_id",      "uint16"),
]


def envelope_fields(deadline_type: str = "uint40",
                      nonce_type: str = "uint48",
                      include_account: bool = True) -> list[dict]:
    out = []
    if include_account:
        out.append({"name": "account", "type": "address"})
    out += [
        {"name": "signer",             "type": "address"},
        {"name": "nonce_anchor",       "type": nonce_type},
        {"name": "nonce_bitmap_index", "type": "uint8"},
        {"name": "deadline",           "type": deadline_type},
    ]
    return out


def envelope_message(include_account: bool = True) -> dict[str, Any]:
    msg = {
        "signer":             PERMIT_ENVELOPE["signer"],
        "nonce_anchor":       PERMIT_ENVELOPE["nonce_anchor"],
        "nonce_bitmap_index": PERMIT_ENVELOPE["nonce_bitmap_index"],
        "deadline":           PERMIT_ENVELOPE["deadline"],
    }
    if include_account:
        msg["account"] = PERMIT_ENVELOPE["account"]
    return msg


def order_fields_types() -> list[dict]:
    return [{"name": n, "type": t} for n, t in ORDER_FIELDS_DOC_ORDER]


def order_message() -> dict[str, Any]:
    return dict(ORDER_BODY)


# ─── Variant definitions ─────────────────────────────────────────────
def variants() -> list[tuple[str, dict, str, dict]]:
    """Return [(name, types, primary, message)]."""
    out = []

    # V1 — VerifyWitness, account included, order inlined, uint40 deadline, uint48 nonce
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint40", "uint48", True) + order_fields_types()}
    out.append(("V1: VerifyWitness inline, uint40/uint48, account",
                 types, "VerifyWitness",
                 {**envelope_message(True), **order_message()}))

    # V2 — VerifyWitness, no account, order inlined
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint40", "uint48", False) + order_fields_types()}
    out.append(("V2: VerifyWitness inline, uint40/uint48, no account",
                 types, "VerifyWitness",
                 {**envelope_message(False), **order_message()}))

    # V3 — VerifyWitness, uint48 deadline + uint48 nonce, account
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint48", "uint48", True) + order_fields_types()}
    out.append(("V3: VerifyWitness inline, uint48/uint48, account",
                 types, "VerifyWitness",
                 {**envelope_message(True), **order_message()}))

    # V4 — VerifyWitness, uint256 deadline + uint256 nonce, account
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint256", "uint256", True) + order_fields_types()}
    out.append(("V4: VerifyWitness inline, uint256/uint256, account",
                 types, "VerifyWitness",
                 {**envelope_message(True), **order_message()}))

    # V5 — Nested Order witness pattern
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint40", "uint48", True) + [
                 {"name": "witness", "type": "Order"}],
             "Order": order_fields_types()}
    out.append(("V5: VerifyWitness nested Order, uint40/uint48, account",
                 types, "VerifyWitness",
                 {**envelope_message(True), "witness": order_message()}))

    # V6 — Nested Order, no account, uint48 deadline
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": envelope_fields("uint48", "uint48", False) + [
                 {"name": "witness", "type": "Order"}],
             "Order": order_fields_types()}
    out.append(("V6: VerifyWitness nested Order, uint48/uint48, no account",
                 types, "VerifyWitness",
                 {**envelope_message(False), "witness": order_message()}))

    # V7 — Primary type "Permit" instead
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "Permit": envelope_fields("uint40", "uint48", True) + order_fields_types()}
    out.append(("V7: Permit inline, uint40/uint48, account",
                 types, "Permit",
                 {**envelope_message(True), **order_message()}))

    # V8 — Primary type "PermitWitnessOrder"
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "PermitWitnessOrder": envelope_fields("uint40", "uint48", True) + order_fields_types()}
    out.append(("V8: PermitWitnessOrder inline, uint40/uint48, account",
                 types, "PermitWitnessOrder",
                 {**envelope_message(True), **order_message()}))

    # V9 — Order BEFORE envelope fields (reversed)
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": order_fields_types() + envelope_fields("uint40", "uint48", True)}
    out.append(("V9: VerifyWitness reversed (order first), uint40/uint48",
                 types, "VerifyWitness",
                 {**order_message(), **envelope_message(True)}))

    # V10 — order_id stays as int not bytes for cancel (irrelevant here, skip)

    # V11 — Try uint8 side to int+side mapping (frontend might 0=Buy/1=Sell encoded differently)
    # already covered

    # V12 — alphabetical order (just in case)
    alpha = sorted(ORDER_FIELDS_DOC_ORDER + [
        ("account",            "address"),
        ("signer",             "address"),
        ("nonce_anchor",       "uint48"),
        ("nonce_bitmap_index", "uint8"),
        ("deadline",           "uint40"),
    ], key=lambda x: x[0])
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "VerifyWitness": [{"name": n, "type": t} for n, t in alpha]}
    msg = {**envelope_message(True), **order_message()}
    out.append(("V12: alphabetical field order", types, "VerifyWitness", msg))

    # ── TradeOrder primary type variants (per other devs' reports that
    # the docs originally described `TradeOrder` not `VerifyWitness`) ─
    for dl_t, nc_t in [("uint40", "uint48"), ("uint48", "uint48"),
                        ("uint256", "uint256")]:
        for acc in [True, False]:
            types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
                     "TradeOrder": envelope_fields(dl_t, nc_t, acc) +
                                    order_fields_types()}
            msg = ({**envelope_message(acc), **order_message()})
            out.append((f"V_TO: TradeOrder {dl_t}/{nc_t} account={acc}",
                         types, "TradeOrder", msg))

    # Nested Order witness under TradeOrder
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "TradeOrder": envelope_fields("uint40", "uint48", True) + [
                 {"name": "witness", "type": "Order"}],
             "Order": order_fields_types()}
    out.append(("V_TO: TradeOrder nested Order, uint40/uint48, account",
                 types, "TradeOrder",
                 {**envelope_message(True), "witness": order_message()}))

    # Plain TradeOrder without permit envelope (just order fields)
    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "TradeOrder": [{"name": "account", "type": "address"}] + order_fields_types()}
    out.append(("V_TO: TradeOrder bare (account + order only)",
                 types, "TradeOrder",
                 {"account": PERMIT_ENVELOPE["account"], **order_message()}))

    types = {"EIP712Domain": EIP712_DOMAIN_TYPE,
             "TradeOrder": order_fields_types()}
    out.append(("V_TO: TradeOrder bare (order only, no account)",
                 types, "TradeOrder", order_message()))

    return out


# ─── Run all variants ────────────────────────────────────────────────
def main() -> None:
    print(f"Expected signer:  {EXPECTED_SIGNER}")
    print(f"Captured sig:     {SIGNATURE_B64}")
    print(f"Captured order:   {json.dumps(ORDER_BODY)}")
    print(f"Captured permit:  {json.dumps({k: v for k, v in PERMIT_ENVELOPE.items()})}\n")

    found = False
    for name, types, primary, message in variants():
        try:
            signable = encode_typed_data(full_message={
                "types": types,
                "domain": DOMAIN,
                "primaryType": primary,
                "message": message,
            })
            recovered = Account.recover_message(signable, signature=SIG_BYTES)
            recovered_cs = to_checksum_address(recovered)
            match = "PASS" if recovered_cs == EXPECTED_SIGNER else "fail"
            print(f"  [{match}] {name}")
            print(f"          recovered = {recovered_cs}")
            if recovered_cs == EXPECTED_SIGNER:
                found = True
                print(f"\n  >>> SCHEMA FOUND <<<")
                print(f"  primaryType: {primary}")
                print(f"  types[{primary}]:")
                for f in types[primary]:
                    print(f"    {f['name']:30s}  {f['type']}")
                break
        except Exception as exc:  # noqa: BLE001
            print(f"  [ERR ] {name}: {type(exc).__name__}: {exc}")

    if not found:
        print("\nNo variant matched. The schema is something we haven't tried.")
        print("Likely needs: contract source inspection OR an additional capture.")


if __name__ == "__main__":
    main()
