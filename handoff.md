# Session Handoff — 2026-04-06

## Status: Pipeline fully operational — KPI review + dashboard refinement in progress

All data pipeline issues resolved. **300/300 stocks scored, 0 incomplete, ~65s runtime.**

---

## What was fixed this session (pipeline — prior session)

### 1. `prdy_ctrt` (price change %) — was 44/300, now 300/300
- **Old source:** KIS `FHPTJ04400000` fi_total ranking API — only returns top ~30 per market
- **Fix:** Scrape `등락률` column (index 4) from Naver Finance market cap page — same scrape already running for universe, zero extra API calls
- **File:** `pipeline/krx_short_selling.py` → `_scrape_naver_page()`

### 2. `ssts_vol_rlim` (short ratio) — was 225/300, now 300/300
Two bugs fixed:
- **Date bug:** `FID_INPUT_DATE_2 = _yesterday()` missed today's trading data when pipeline runs post-market. Fixed to `date.today()` in both `fetch_daily_short_snapshot()` and `fetch_daily_short_trend()`
- **Retry missing:** `fetch_daily_short_snapshot()` used bare `client.get()` — dropped connections under 300-ticker concurrent load with no retry. Fixed to use `_get_with_retry()`
- **Concurrency tuned:** `fetch_short_snapshots_bulk()` set to `concurrency=5, sleep=0.3s`
- **File:** `pipeline/fetcher.py`

### 3. `frgn_ntby_tr_pbmn` + `orgn_ntby_tr_pbmn` — was 282/300 & 278/300, now 300/300
- **Old source:** `FHPTJ04400000` (primary, only top 30/market) → `FHPTJ04160001` (fallback, broken params) → `HHPTJ04160200` (estimate, inaccurate)
- **Fix:** Replace entire chain with `FHKST01010900` (`inquire-investor`) as primary — per-ticker, concurrency=5, confirmed accurate end-of-day values. `FHPTJ04400000` kept as conditional fallback only if FHKST01010900 leaves nulls (never triggered in practice)
- **Removed:** `FHPTJ04160001`, `HHPTJ04160200`, estimate conversion logic
- **File:** `pipeline/fetcher.py` → `fetch_inquire_investor_bulk()`, `pipeline/run.py`

---

## What was done this session (score formula + dashboard)

### 4. Score formula — `z(5D avg)` replaced with `z(acceleration)`
- **Change:** `0.15 × z(ssts_vol_rlim_5d)` → `0.15 × z(ssts_vol_rlim − ssts_vol_rlim_5d)`
- **Rationale:** 5D avg and today's turnover are highly correlated (both measure level). Acceleration captures whether pressure is *building*, a genuinely orthogonal dimension.
- **Impact on 2026-04-06 data:** score ≥ 1.0 changed from 13 → 15 (+2), score ≥ 0.5 changed from 48 → 46 (−2). Names that gained were borderline stocks with accelerating shorts; names that dropped had high historical short ratios but flat/declining momentum.
- **File:** `pipeline/score.py` line 96

### 5. Dashboard formula strip — label + tooltip
- **Change:** Pill label updated to `× z(short turnover accel.)`, with `i` button and hover tooltip showing `= today's SS turnover − 5D AVG SS turnover`
- **File:** `dashboard/dashboard.html` line 583

---

## Final pipeline metrics

| Metric | Session start | End of session |
|---|---|---|
| `prdy_ctrt` | 44/300 | **300/300** |
| `ssts_vol_rlim` | 225/300 | **300/300** |
| `frgn_ntby_tr_pbmn` | 282/300 | **300/300** |
| `orgn_ntby_tr_pbmn` | 278/300 | **300/300** |
| `score` populated | 32/300 | **300/300** |
| `incomplete` | 268/300 | **0/300** |
| Runtime | ~2m 30s | **~65s** |

---

## Current API call structure (per run)

| Step | API | Calls | ~Time |
|---|---|---|---|
| Universe + prdy_ctrt | Naver Finance scrape | ~20 pages | 10s |
| Short snapshots | `FHPST04830000` | 300 (concurrency=5) | 25s |
| Investor data (frgn/orgn) | `FHKST01010900` | 300 (concurrency=5) | 29s |
| fi_total fallback | `FHPTJ04400000` | 0 (not triggered) | — |
| **Total** | | **~620 calls** | **~65s** |

---

## Commits this session

| Hash | Description |
|---|---|
| `11c8194` | feat: revamp universe to top-300 market cap via Naver Finance |
| `45cca40` | fix(pipeline): achieve 300/300 scored stocks with accurate investor data |

---

## Next session: KPI 2/3/4 review + dashboard bottom charts

### KPI review — open questions and suggestions

**KPI 2 — Foreign & Inst Alignment (60% / 30% thresholds)**
- Are 60% / 30% the right cutpoints? No empirical basis for these — consider whether ALIGNED / MIXED / DIVERGED categories are correctly spaced given the 300-stock universe.
- Suggestion: review the actual distribution of alignment % values across recent dates before deciding.

**KPI 3 — Score-Weighted Return**
- Q: Should the denominator be `score > 0` (current) or `score ≥ 1.0` (high-pressure only)?
  - `score > 0` uses ~half the universe; may dilute signal with low-conviction names.
  - `score ≥ 1.0` is more focused but highly sensitive to that day's count (could be 0 names).
  - Suggestion: use `score ≥ 1.0` if high-pressure count is ≥ 5; fall back to `score > 0` otherwise.
- Q: Should `prdy_ctrt` be adjusted for market/index return before computing the weighted average?
  - Raw 1-day return conflates market beta with stock-specific confirmation. A stock down 2% on a KOSPI-down-3% day is not confirming shorts.
  - Suggestion: subtract same-day KOSPI or KOSDAQ index return from `prdy_ctrt` before KPI 3 computation.

**KPI 4 — Unconfirmed Shorts**
- Q: Should threshold be `prdy_ctrt ≥ 0%` (current) or `prdy_ctrt ≥ +0.5%`?
  - A stock up +0.05% and one up +3% are treated the same. Noise around zero is real.
  - Suggestion: `≥ +0.5%` as the unconfirmed threshold to filter micro-noise; or apply same index-relative adjustment as KPI 3.

### Dashboard bottom charts — to review and update
- Review chart relevance and layout for 300-stock universe (charts were designed for 60-stock pool)
- Consider replacing or supplementing with: score distribution histogram, market heatmap, top-N movers
- Axis labels and legends may need rescaling for 300-stock data range

---

## Running locally

```bash
# Run pipeline (after 15:44 KST)
python pipeline/run.py

# Serve dashboard
python dashboard/app.py
# → http://localhost:8080
```

## All tests

```bash
pytest tests/ -v
```

# git commit done 

7351d44