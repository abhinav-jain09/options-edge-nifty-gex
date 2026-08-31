# nifty-gex-service

Live NIFTY gamma-exposure (GEX) board backed by the DhanHQ v2 Option Chain API
(https://dhanhq.co/docs/v2/). No dependencies beyond Python 3 stdlib.

## Run

```bash
./run.sh
```

Then open http://127.0.0.1:8188/ (HTML board), or:

- `GET /gex` — full JSON snapshot (all strikes, totals, flip level, walls)
- `GET /health` — status + data staleness + last poll error

Credentials live in `.env` (git-ignored): `DHAN_ACCESS_TOKEN`, `DHAN_CLIENT_ID`.


## Deploying

Kubernetes manifests live in **options-edge-deploy** under `k8s/services/nifty-gex/`
(base + dev/production overlays; RBAC in the common-infra overlay). Deploys go through
the Jenkins job **nifty-gex-service-deploy** (Jenkinsfile.nifty-gex-service): build on
the target arch → push → digest-pin → apply → rollout → health gate. Dev deploys at
replicas 0 by design (single Dhan token — see above).

## Token auto-renewal

Dhan tokens live 24 h, but the service now **renews itself**: whenever less than
`DHAN_GEX_RENEW_BEFORE_HOURS` (default 12) remain — or a poller hits a 401 — it calls
`GET /v2/RenewToken` (the docs say POST; that 400s with DH-905 — **GET is what works**,
response `{createTime, expiryTime, token}`), swaps the token in memory, and PATCHes it
back into the `dhan-credentials` k8s Secret (ServiceAccount `nifty-gex`, Role scoped to
that one Secret — RBAC lives with common infra in `options-edge-deploy`) so pod restarts survive. `/health` reports
`tokenHoursLeft`, `tokenRenewedAt`, `renewError`, `secretWrittenAt`.

Renewal INVALIDATES the previous token, so every stale copy (local `.env`, the dev
cluster's secret) dies whenever prod renews. Before running locally or scaling dev up,
run `./sync-dev-token.sh` to pull the current token from the prod secret. Manual
recovery (token fully expired, renew impossible): generate in Dhan web → `.env` →
`./update-prod-token.sh`.

## What it computes

Two pollers:

- **Chain** — `POST /v2/optionchain` for the nearest NIFTY expiry every 3.5 s
  (Dhan's limit is one unique request per 3 s) → gamma, OI, IV per strike.
- **Live spot** — `POST /v2/marketfeed/ltp` every 1.1 s (limit 1/s) → the board
  rebuilds on every spot tick, so GEX rescales with spot between chain polls.
  Header shows `(live 1s quote)`; if the quote feed stalls >15 s it falls back
  to the chain's own `last_price` and shows `(from chain)`.

Per strike, using Dhan's own greeks:

```
callGEX =  gamma_ce * OI_ce * multiplier * spot² * 0.01   (₹ per 1% move, shown in crore)
putGEX  = -gamma_pe * OI_pe * multiplier * spot² * 0.01
```

Calls positive / puts negative is the standard dealer-positioning convention
(dealers assumed long calls, short puts). Derived levels:

- **Zero-γ flip** — strike where cumulative net GEX crosses zero (crossing nearest spot)
- **Call wall / put wall** — largest call GEX and largest (most negative) put GEX
  within ±5% of spot
- **Totals** — board-wide call/put/net GEX

Dhan's `oi` is in underlying units (contracts × lot of 75), so the OI multiplier
defaults to **1**. If your account returns OI in contracts, set
`DHAN_GEX_OI_MULTIPLIER=75`.

## Config (env vars, all optional)

| Var | Default | Meaning |
|---|---|---|
| `DHAN_GEX_PORT` | 8188 | HTTP port (binds 127.0.0.1) |
| `DHAN_GEX_EXPIRY` | nearest | pin a specific expiry `YYYY-MM-DD` |
| `DHAN_GEX_UNDERLYING_SCRIP` | 13 | 13 = NIFTY; 25 = BANKNIFTY etc. |
| `DHAN_GEX_UNDERLYING_SEG` | IDX_I | index segment |
| `DHAN_GEX_UNDERLYING_NAME` | NIFTY | display name |
| `DHAN_GEX_POLL_SECONDS` | 3.5 | chain poll interval (keep > 3) |
| `DHAN_GEX_SPOT_POLL_SECONDS` | 1.1 | live-spot poll interval (keep > 1) |
| `DHAN_GEX_OI_MULTIPLIER` | 1 | multiply OI (see above) |
| `DHAN_GEX_WALL_BAND_PCT` | 5 | wall search band around spot, % |

Failure behavior: on API errors the service keeps serving the last good
snapshot and reports the error + staleness on `/health` and in the board header;
429s back off 10 s. Read-only market data — this service never places orders.
