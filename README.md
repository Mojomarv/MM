# OndoPerpsMM — Multi-pair grid MM port to OndoPerps

Port of the same multi-grid MM bot to OndoPerps (`api.ondoperps.xyz`).
Same ladder strategy, throttle, orphan tracking, exit-only / per-pair
halts as the Lighter and Rise builds; new client layer for OndoPerps's
HMAC-SHA256 auth scheme.

## Layout

```
OndoPerpsMM/
├── accounts/ondo1/
│   ├── .env.template          # copy to .env, fill in ONDO_KEY_ID / ONDO_API_SECRET
│   ├── data/                  # CSV trade log
│   └── logs/                  # one logfile per run
├── bots/mm/ondo_mm/
│   ├── multi_grid_bot.py      # main bot — strategy ports 1:1
│   └── ondo_client.py         # HMAC-SHA256 signer + REST + WS helpers
├── common/
│   └── control_server.py      # bot's HTTP /status + /start + /stop + /close
├── dashboard/
│   ├── index.html             # single-page console (vanilla JS, no build)
│   └── serve.py               # tiny http server: python dashboard/serve.py
└── requirements.txt
```

## Dashboard

The bot embeds a control server on `CONTROL_PORT` (default `8140`). A
single-page console at `dashboard/index.html` polls `/status` every 2s
and offers Start / Stop / Cancel-orders / Close+flatten controls.

Normally you start it via `start.py` (above). If you want to run it
standalone against a bot that's already running:

```powershell
python dashboard\serve.py
# Then open http://127.0.0.1:8141  in your browser
```

If you point the dashboard at a remote bot, pass query params:
`http://127.0.0.1:8141?host=192.168.1.10&port=8140`.

The dashboard shows:
- Equity, session PnL, realized + unrealized totals, net notional cap usage
- Per-pair table with position, mid, avg-entry, BBO spread (bp), realized
  session & all-time, halt reason — sortable by any column
- Recent fills tape
- Read-only config snapshot (grid params, caps, trading-armed flag)
- Halt banner when the bot trips a stop-loss, fill-imbalance, or
  session-DD guard

## Setup

```powershell
# 1. Create the venv
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2. Provision .env
cp accounts\ondo1\.env.template accounts\ondo1\.env
# Edit: paste ONDO_KEY_ID (with ondoKeyId_ prefix) and ONDO_API_SECRET
# (with ondoApiSecret_ prefix) from the OndoPerps web UI.

# 3. One-command start (bot + dashboard + browser):
python start.py
```

`start.py` launches the bot and the dashboard static-server in their own
process groups, opens http://127.0.0.1:8141 in your browser, and streams
both processes' output to one terminal with colored prefixes:

```
[   bot    ] 2026-06-02 19:30:01,234 INFO config: pairs=...
[ dashboard ] Dashboard serving at http://127.0.0.1:8141
```

Ctrl+C in that terminal does a graceful shutdown — on Windows it sends
CTRL_BREAK to the bot's process group so its asyncio `finally` block
runs (which cancels resting orders if `TRADING=true`). Force-kill is a
15s safety net.

Useful flags:

```powershell
python start.py --account ondo2          # different accounts/ subfolder
python start.py --no-browser             # skip auto-open
python start.py --dashboard-port 9000    # override dashboard port
python start.py --flatten-on-stop        # POST /close?flatten=true on Ctrl+C
```

## OndoPerps specifics worth knowing

| Thing | Lighter | Rise | OndoPerps |
|---|---|---|---|
| Auth | JWT (8 min refresh) | EIP-712 per request | HMAC-SHA256 over `ts+method+path+body` |
| Market identifier | Integer ID | Integer ID | **Ticker string** (`AAPL-USD.P`) |
| Price/size encoding | Integer scale | Integer scale | **Decimal strings** snapped to per-market increments |
| Order ID | Integer client_order_index | uint64 + composite hex | **String client_order_id** (≤64 chars `[A-Za-z0-9_-]`) |
| POST_ONLY | TIF enum | Separate bool | Separate `postOnly: bool` |
| Order status | open / pending / canceled / filled | OPEN / FILLED / CANCELLED | open / fullyfilled / canceled |
| Realized PnL | Position field | Per-fill | Per-fill (`fillsPerps`) |
| Cancel-all | One atomic tx | Per-market | **Batch DELETE** with comma-separated IDs |
| Fees | 0 | 0 | **Non-zero**: maker 1.5 bp / taker 3.5 bp |

## Markets

OndoPerps lists 22 perpetuals (no crypto majors):
- **Equities (16)**: AAPL, AMD, AMZN, COIN, CRCL, GOOGL, HOOD, INTC, META, MSFT, MSTR, NFLX, NVDA, ORCL, PLTR, TSLA (all `-USD.P`)
- **Indices (2)**: US500-USD.P, US100-USD.P
- **Commodities (3)**: XAU-USD.P, XAG-USD.P, WTI-USD.P
- **ETF (1)**: DRAM-USD.P

Max position size per symbol per account: $500k. Leverage up to 10-20× depending on asset class.

## Calibration notes

Because OndoPerps charges 1.5 bp maker, the bot's grid step needs to
clear at least ~3 bp per round-trip just to break even on fees:

  realized per round-trip ≈ 2 × GRID_STEP_BP − (2 × 1.5 bp fee) − adverse_selection

A `GRID_STEP_BP` < 4 will lose to fees alone even before adverse
selection. A reasonable starting config on this venue:

```
GRID_STEP_BP=15
LEVELS_PER_SIDE=1
ORDER_SIZE_USD=20
MAX_NOTIONAL_PER_PAIR_USD=100
REQUOTE_DRIFT_BP=20
PAIR_STOP_LOSS_USD=-1.5
```

## Config knobs

All the same risk env vars as the Lighter/Rise bots
(`PAIRS_INCLUDE`, `ORDER_SIZE_USD`, `LEVELS_PER_SIDE`, `GRID_STEP_BP`,
`MAX_NOTIONAL_PER_PAIR_USD`, `REQUOTE_DRIFT_BP`, `SESSION_DD_LIMIT_USD`,
`PAIR_STOP_LOSS_USD`, `FILL_IMBALANCE_RATIO`, etc.). Plus OndoPerps-specific:

* `ONDO_REST_BASE_URL` — `https://api.ondoperps.xyz`
* `ONDO_WS_URL` — `wss://api.ondoperps.xyz/ws`
* `ONDO_KEY_ID` — `ondoKeyId_...` (from the web UI)
* `ONDO_API_SECRET` — `ondoApiSecret_...`

## OndoPerps API gotchas (docs vs. live reality)

These were discovered during initial wiring against `api.ondoperps.xyz`
mainnet. Listed here so future ports / debugging skip the round-trip:

| Gotcha | What the docs say | What works live |
|---|---|---|
| WS-login HMAC message order | `"ondo_perps_ws_login" + time` | **`time + "ondo_perps_ws_login"`** (reversed) |
| WS-login `time` field | `"<unix_ms>"` (string-looking placeholder) | Must be **string**; sending int causes silent close with 1006 / no close frame |
| depth-book subscribe params | `depthLevels`, `limit` listed as optional | **Required in practice** — bare subscribe returns no updates |
| depth-book level encoding | `{"price": "...", "size": "..."}` (object) | **`["price", "size"]`** (array tuple) |
| batch-cancel ID count | not documented | Hard cap **20 ids per call** (`[400] Too many orderIDs, max is 20`) |
| client_order_id uniqueness | not documented | Enforced **across sessions** — coid_counter must be seeded with a monotonic value (we use epoch seconds) |

`bots/mm/ondo_mm/_test_ws.py` was the diagnostic tool that surfaced the
auth-order issue; `ONDO_DEBUG_WS=1` in `.env` enables raw-frame logging
across all four WS consumers and was how the array-vs-object level
encoding was caught.

## Reference

- [API key authentication](https://docs.ondoperps.xyz/api-reference/api_key_authentication)
- [Documentation index](https://docs.ondoperps.xyz/llms.txt)
- [Create order](https://docs.ondoperps.xyz/api-reference/orders/create-order) · [Cancel](https://docs.ondoperps.xyz/api-reference/orders/cancel-order-by-id) · [Batch cancel](https://docs.ondoperps.xyz/api-reference/orders/batch-cancel-orders)
- [Markets](https://docs.ondoperps.xyz/api-reference/markets/get-markets) · [Portfolio](https://docs.ondoperps.xyz/api-reference/portfolio/get-portfolio-summary) · [Positions](https://docs.ondoperps.xyz/api-reference/positions/get-positions)
- [WS connect](https://docs.ondoperps.xyz/api-reference/connection/connect) · [WS login](https://docs.ondoperps.xyz/api-reference/connection/login)
- [Channels: orders](https://docs.ondoperps.xyz/api-reference/private-channels/subscribe:-perps-orders) · [positions](https://docs.ondoperps.xyz/api-reference/private-channels/subscribe:-perps-positions) · [fills](https://docs.ondoperps.xyz/api-reference/private-channels/subscribe:-perps-fills) · [depth book](https://docs.ondoperps.xyz/api-reference/public-channels/subscribe:-perps-depth-book)
