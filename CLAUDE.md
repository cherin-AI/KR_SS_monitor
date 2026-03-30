# Short Pressure Monitor — Claude Code Guide

## Project overview

Korean stock market short pressure monitoring dashboard using KIS (Korea Investment & Securities) Open API.
Scores each stock in the top-30 universe (KOSPI + KOSDAQ) on a composite short-pressure metric and renders
results as an interactive web dashboard.

## Repository structure

```
short-pressure-monitor/
├── CLAUDE.md               ← this file
├── PLAN.md                 ← full implementation plan
├── config.yaml             ← API keys & runtime settings (gitignored)
├── config.example.yaml     ← template — commit this, never config.yaml
├── requirements.txt
│
├── pipeline/
│   ├── auth.py             ← token management (issue / cache / refresh)
│   ├── fetcher.py          ← all KIS API call functions
│   ├── score.py            ← z-score normalisation + weighted sum
│   └── run.py              ← daily batch entry point
│
├── dashboard/
│   ├── app.py              ← Flask/FastAPI server (serves dashboard.html)
│   ├── dashboard.html      ← single-file frontend (Chart.js)
│   └── static/
│
├── data/
│   ├── cache/              ← token cache, stock metadata cache
│   └── output/             ← daily JSON snapshots  e.g. 20250328.json
│
└── tests/
    ├── test_fetcher.py
    └── test_score.py
```

## Key constraints to always remember

- **30-stock hard cap per market** — `short-sale ranking` API (FHPST04820000) returns max 30 records,
  no pagination. Do not attempt to work around this with repeated calls.
- **KOSPI market code = `J`, KOSDAQ = `NX`** when calling `FID_COND_MRKT_DIV_CODE`.
  Exception: foreign/inst aggregate API uses `V` as default.
- **Daily short-sale trend API requires a ticker** — fetch universe first, then call per ticker (60 calls total).
- **Investor data available after 15:40 KST** — schedule the batch after market close.
- **No pagination on ranking APIs** — `tr_cont` pagination is explicitly disabled for these endpoints.
- **Token TTL = 86 400 s (24 h)** — cache the token in `data/cache/token.json`; reissue only when expired.
- **Rate limit** — KIS enforces ~20 req/s. Add `asyncio.sleep(0.1)` between ticker-level calls.

## Score formula (v2 — do not change weights without updating PLAN.md)

```
score =
    0.35 × zscore(ssts_vol_rlim)          # short ratio today
  + 0.15 × zscore(ssts_vol_rlim_5d_avg)  # short ratio 5-day average
  + 0.25 × zscore(-frgn_ntby_tr_pbmn)    # foreign net sell value
  + 0.15 × zscore(-orgn_ntby_tr_pbmn)    # institution net sell value
  + 0.10 × zscore(-prdy_ctrt)            # negative return %
```

Z-scores are computed **within each market** (KOSPI 30 and KOSDAQ 30 independently).
All five components must be present for a stock to receive a score. Stocks with any missing
component are flagged `incomplete` and shown at the bottom of the table.

## API reference (quick lookup)

| Purpose | TR_ID | URL path |
|---|---|---|
| Token issue | — | `POST /oauth2/tokenP` |
| Short-sale ranking | `FHPST04820000` | `/uapi/domestic-stock/v1/ranking/short-sale` |
| Daily short-sale trend | `FHPST04830000` | `/uapi/domestic-stock/v1/quotations/daily-short-sale` |
| Foreign/inst aggregate | `FHPTJ04400000` | `/uapi/domestic-stock/v1/quotations/foreign-institution-total` |
| Investor daily by stock | `FHPTJ04160001` | `/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily` |
| Stock basic info | `CTPF1002R` | `/uapi/domestic-stock/v1/quotations/search-stock-info` |

Fallback strategy for missing foreign/institution rows:
- Keep `foreign-institution-total` as the fast primary source.
- Backfill only missing tickers with `종목별 투자자매매동향(일별)` (`FHPTJ04160001`).
- Use the per-ticker values from that endpoint to populate `frgn_ntby_tr_pbmn` and `orgn_ntby_tr_pbmn` before scoring.

Fallback strategy for missing foreign/institution rows:
- Keep `foreign-institution-total` as the fast primary source.
- Backfill only missing tickers with `종목별 투자자매매동향(일별)` (`FHPTJ04160001`).
- Use the per-ticker values from that endpoint to populate `frgn_ntby_tr_pbmn` and `orgn_ntby_tr_pbmn` before scoring.

Base URL (prod): `https://openapi.koreainvestment.com:9443`
Base URL (paper): `https://openapivts.koreainvestment.com:29443`

## Config file format

```yaml
# config.yaml  (never commit — listed in .gitignore)
kis:
  app_key: "PSg5dctL9d..."
  app_secret: "yo2t8zS68z..."
  env: "prod"           # prod | paper

schedule:
  run_after: "15:44"    # KST — after investor data is available
  lookback_days: 5      # for 5-day short ratio average

output:
  json_dir: "data/output"
  serve_port: 8080
```

## Coding conventions

- Python 3.11+, type hints everywhere
- Use `httpx` (async) for all HTTP calls — not `requests`
- Use `pandas` for DataFrame operations; avoid raw loops over rows
- All API responses: check `rt_cd == "0"` before parsing `output`
- Log at INFO level for each API call: `[FETCH] {tr_id} {ticker or market} → {n} rows`
- Raise `KISAPIError(msg_cd, msg1)` on non-zero `rt_cd`
- Store secrets only in `config.yaml` — never hardcode

## Running locally

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. copy and fill in credentials
cp config.example.yaml config.yaml

# 3. run the pipeline once (fetches data + computes scores)
python pipeline/run.py

# 4. serve the dashboard
python dashboard/app.py
# → open http://localhost:8080
```

The dashboard server opens a browser tab automatically when it starts.

## Testing

```bash
pytest tests/ -v
```

Mock fixtures live in `tests/fixtures/` — use them for all unit tests so no real API calls are made.
