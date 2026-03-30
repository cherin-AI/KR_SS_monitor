# Short Pressure Monitor — Implementation Plan

## Status legend
- `[ ]` not started
- `[~]` in progress
- `[x]` complete

---

## Phase 1 — Data pipeline

### 1-1. Auth (`pipeline/auth.py`)
- [ ] `issue_token(app_key, app_secret, env) -> str` — POST `/oauth2/tokenP`
- [ ] `get_token(config) -> str` — load from cache if valid, else reissue
  - Cache path: `data/cache/token.json`
  - Check `expires_in` (86 400 s) against `issued_at` timestamp
- [ ] `build_headers(token, tr_id, app_key, app_secret) -> dict`

```python
# Expected cache format
{
  "access_token": "eyJ0...",
  "issued_at": "2025-03-28T15:45:00",
  "expires_in": 86400
}
```

---

### 1-2. Fetcher (`pipeline/fetcher.py`)

All functions are `async def` using `httpx.AsyncClient`.

#### A. `fetch_short_sale_ranking(client, token, config, market) -> pd.DataFrame`
- TR_ID: `FHPST04820000`
- URL: `/uapi/domestic-stock/v1/ranking/short-sale`
- Parameters:
  ```
  FID_APLY_RANG_VOL      = ""
  FID_COND_MRKT_DIV_CODE = "J"      # KOSPI / "NX" for KOSDAQ
  FID_COND_SCR_DIV_CODE  = "20482"
  FID_INPUT_ISCD         = "0000"   # all
  FID_PERIOD_DIV_CODE    = "D"
  FID_INPUT_CNT_1        = "0"      # today only
  FID_TRGT_EXLS_CLS_CODE = ""
  FID_TRGT_CLS_CODE      = ""
  FID_APLY_RANG_PRC_1    = ""
  FID_APLY_RANG_PRC_2    = ""
  ```
- Returns DataFrame with columns:
  `mksc_shrn_iscd, hts_kor_isnm, stck_prpr, prdy_ctrt,
   acml_vol, ssts_cntg_qty, ssts_vol_rlim, ssts_tr_pbmn,
   ssts_tr_pbmn_rlim, avrg_prc`
- Max 30 rows per call. Called twice (KOSPI + KOSDAQ).

#### B. `fetch_daily_short_trend(client, token, config, ticker, lookback_days) -> pd.DataFrame`
- TR_ID: `FHPST04830000`
- URL: `/uapi/domestic-stock/v1/quotations/daily-short-sale`
- Parameters:
  ```
  FID_COND_MRKT_DIV_CODE = "J"
  FID_INPUT_ISCD         = ticker           # 6-digit
  FID_INPUT_DATE_1       = date_n_days_ago  # YYYYMMDD
  FID_INPUT_DATE_2       = yesterday        # YYYYMMDD
  ```
- Parses `output2[]` array → returns last `lookback_days` rows of `ssts_vol_rlim`
- Called 60 times (30 KOSPI + 30 KOSDAQ) with `asyncio.sleep(0.1)` between calls.

#### C. `fetch_foreign_inst_aggregate(client, token, config, market) -> pd.DataFrame`
- TR_ID: `FHPTJ04400000`
- URL: `/uapi/domestic-stock/v1/quotations/foreign-institution-total`
- Parameters:
  ```
  FID_COND_MRKT_DIV_CODE = "V"
  FID_COND_SCR_DIV_CODE  = "16449"
  FID_INPUT_ISCD         = "0001"   # KOSPI / "1001" for KOSDAQ
  FID_DIV_CLS_CODE       = "1"      # sort by value
  FID_RANK_SORT_CLS_CODE = "1"      # net sell top
  FID_ETC_CLS_CODE       = "0"      # all investors
  ```
- Returns columns: `mksc_shrn_iscd, frgn_ntby_tr_pbmn, orgn_ntby_tr_pbmn,
  frgn_ntby_qty, orgn_ntby_qty, stck_prpr, prdy_ctrt`
- Called twice (KOSPI + KOSDAQ).

#### D. `fetch_stock_meta(client, token, config, ticker) -> dict` *(cached)*
- TR_ID: `CTPF1002R`
- URL: `/uapi/domestic-stock/v1/quotations/search-stock-info`
- Parameters: `PRDT_TYPE_CD=300, PDNO=ticker`
- Returns: sector, market cap tier, listed shares
- Cache in `data/cache/stock_meta.json` — refresh weekly, not daily.

---

### 1-3. Score engine (`pipeline/score.py`)

```python
def zscore_series(s: pd.Series) -> pd.Series:
    """Standard z-score; returns NaN for constant series."""

def compute_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input columns required:
        ssts_vol_rlim        float  (short ratio today, %)
        ssts_vol_rlim_5d_avg float  (5-day mean of ssts_vol_rlim)
        frgn_ntby_tr_pbmn   float  (foreign net buy value, KRW M — negative = net sell)
        orgn_ntby_tr_pbmn   float  (institution net buy value, KRW M)
        prdy_ctrt            float  (return %, negative = decline)

    Weights:
        short_today  0.35
        short_5d     0.15
        foreign_sell 0.25
        inst_sell    0.15
        return_neg   0.10

    Returns df with added columns:
        z_short_today, z_short_5d, z_foreign, z_inst, z_return
        score, rank
    """
```

- Z-scores computed per-market (KOSPI and KOSDAQ separately).
- Stocks missing any component get `score = NaN`, `rank = None`, flagged `"incomplete"`.
- Output sorted descending by score.

---

### 1-4. Batch runner (`pipeline/run.py`)

```
Execution order:
1.  get_token()
2.  fetch_short_sale_ranking(KOSPI)   → universe_kospi (≤30 tickers)
3.  fetch_short_sale_ranking(KOSDAQ)  → universe_kosdaq (≤30 tickers)
4.  fetch_daily_short_trend() × 60   → ssts_vol_rlim_5d_avg per ticker
5.  fetch_foreign_inst_aggregate(KOSPI)
6.  fetch_foreign_inst_aggregate(KOSDAQ)
7.  merge all DataFrames on mksc_shrn_iscd (left join from universe)
8.  compute_score() for KOSPI, then KOSDAQ
9.  write output JSON to data/output/YYYYMMDD.json
10. write data/output/latest.json (symlink or copy)
```

Total API calls: **63** (2 + 60 + 1 + ... see note on aggregate)
Target runtime: < 90 seconds

---

## Phase 2 — Dashboard

### 2-1. Backend (`dashboard/app.py`)
- [ ] Serve `dashboard.html` at `/`
- [ ] `GET /api/data` → returns `latest.json` contents
- [ ] `GET /api/data/{date}` → returns specific day's snapshot
- [ ] Framework: FastAPI + uvicorn (lightweight, async-native)

### 2-2. Frontend (`dashboard/dashboard.html`)

Single HTML file with inline CSS + Chart.js (CDN). No build step.

#### Sections

| Section | Data source | Chart type |
|---|---|---|
| KPI cards | Aggregate of universe | 4 metric cards |
| Score formula strip | Static (hardcoded weights) | Visual weight pills |
| KOSPI ranking table | `kospi[]` array | Sortable table + mini bar |
| KOSDAQ ranking table | `kosdaq[]` array | Sortable table + mini bar |
| Short ratio distribution | All 60 stocks | Grouped bar (today vs 5d avg) |
| Score vs return scatter | All 60 stocks | Scatter plot |
| 5-day trend lines | Top 8 KOSPI + Top 8 KOSDAQ | Line chart |

#### KPI cards (confirmed)
1. **Avg short ratio** — mean of `ssts_vol_rlim` across all 60 stocks, vs 5d avg
2. **Foreign net sell** — sum of `frgn_ntby_tr_pbmn` (shown negative when net sell), KRW B
3. **High-pressure stocks** — count of stocks where `score >= 2.0`
4. **Top score** — ticker name + score of the #1 ranked stock across both markets

#### Table columns
`rank | ticker | name | short% (today bar + 5d bar) | 5d avg | foreign net | inst net | return% | score`

---

## Phase 3 — Output format

### `data/output/YYYYMMDD.json`

```json
{
  "date": "2025-03-28",
  "generated_at": "2025-03-28T15:52:00+09:00",
  "kospi": [
    {
      "rank": 1,
      "ticker": "005930",
      "name": "삼성전자",
      "short_ratio": 14.2,
      "short_ratio_5d": 11.8,
      "frgn_net_value": -42100,
      "orgn_net_value": -18300,
      "return_pct": -1.8,
      "z_short_today": 1.82,
      "z_short_5d": 1.44,
      "z_foreign": 1.91,
      "z_inst": 1.23,
      "z_return": 0.87,
      "score": 2.41,
      "incomplete": false
    }
  ],
  "kosdaq": [ ... ],
  "meta": {
    "kospi_count": 30,
    "kosdaq_count": 30,
    "high_pressure_count": 7,
    "avg_short_ratio": 8.4,
    "total_frgn_net_value": -312000,
    "top_score_ticker": "247540",
    "top_score_name": "에코프로비엠",
    "top_score_value": 2.68
  }
}
```

---

## Phase 4 — Productionisation (later)

- [ ] Cron job / GitHub Actions scheduled at 15:44 KST
- [ ] Slack/email alert when `high_pressure_count >= 10`
- [ ] Historical chart: 5-day rolling score per ticker
- [ ] Docker container (`Dockerfile` + `docker-compose.yml`)
- [ ] `.env`-based secrets instead of `config.yaml` for container deployments

---

## Known limitations

| Constraint | Impact | Mitigation |
|---|---|---|
| 30-stock cap on ranking API | Universe limited to top-30 by short volume | Accepted — design decision |
| Investor aggregate is intraday estimate | frgn/orgn values may differ slightly from T+1 official | Run after 15:40 KST for best accuracy |
| No pagination on ranking APIs | Cannot expand universe via repeated calls | Use conditional search API if >30 needed in future |
| Daily short trend API needs ticker | 60 sequential calls required | Async + 0.1s sleep keeps total under 90s |
| Paper trading env not supported | All short/investor APIs return 모의투자 미지원 | Use prod credentials only |
