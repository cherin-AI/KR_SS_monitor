# Korea Short Pressure Monitor

A daily-refresh dashboard that scores Korean equities on a composite short-pressure metric and visualises the results as an interactive web UI.

Data is sourced from the [KIS (Korea Investment & Securities) Open API](https://apiportal.koreainvestment.com) and covers the top-30 universe for each market (KOSPI + KOSDAQ, 60 stocks total). The pipeline runs after market close (≥ 15:44 KST), writes a JSON snapshot, and the dashboard reads it statically — no backend required for viewing.

---

## Dashboard features

### KPI strip

| Card | What it measures |
|---|---|
| **High Pressure Count** | Number of stocks with composite score ≥ 1.0 |
| **Rising Short Pressure** | Share of high-pressure stocks where today's short turnover exceeds the 5-day average |
| **Score-Weighted Return** | Score-weighted average price change of high-pressure names — confirms whether positioning is validated by price action |
| **Unconfirmed Shorts** | High-pressure stocks with price return ≥ 0% — forward-looking squeeze risk watch list |

Each card has an **i** tooltip that explains the exact formula and category thresholds.

### AI Analysis

A bullet-point summary derived either from the Claude-generated `ai_summary` field in the daily JSON (when the pipeline runs with the AI step enabled) or from a rules-based fallback that evaluates breadth, price confirmation, squeeze risk, and concentration — without ever requiring an active API call on the frontend.

### Charts

| Chart | Description |
|---|---|
| **Top 25 Short Sell Acceleration** | Bar chart of stocks with the largest today-minus-5D-average short turnover delta |
| **Score Driver Breakdown** | Stacked horizontal bar showing the weighted contribution of each factor for the top-10 high-pressure names |
| **Score vs Price Return %** | Scatter plot of score vs daily return, split by KOSPI / KOSDAQ, with quadrant lines at score = 1.0 and return = 0% |

### Ranking table

- All 60 stocks ranked by composite score
- Sortable by any column (rank, short turnover, 5D avg, foreign flow, inst flow, price change, score)
- Filter by market (All / KOSPI / KOSDAQ) or free-text search by name or ticker
- Top-N selector: Top 25 / 50 / 100 / All

### Score formula

The composite score is computed **within each market** independently using z-score normalisation:

```
score =
    0.35 × z(short turnover %)
  + 0.15 × z(short turnover acceleration)   # today − 5D average
  + 0.25 × z(−foreign net flow, KRW mn)
  + 0.15 × z(−institution net flow, KRW mn)
  + 0.10 × z(−price change %)
```

Stocks missing any of the five components are flagged `incomplete` and shown at the bottom of the table.

---

## Repository structure

```
short-pressure-monitor/
├── pipeline/
│   ├── auth.py        ← KIS token management (issue / cache / refresh)
│   ├── fetcher.py     ← async KIS API calls
│   ├── score.py       ← z-score normalisation + weighted sum
│   └── run.py         ← daily batch entry point
│
├── dashboard/
│   ├── app.py         ← Flask server (serves dashboard.html + /api/data)
│   └── dashboard.html ← single-file frontend (Chart.js, no build step)
│
├── data/
│   ├── cache/         ← token cache, stock metadata cache
│   └── output/        ← daily JSON snapshots  e.g. 20250328.json
│                         latest.json ← symlink / copy of latest snapshot
└── tests/
```

---

## Running locally

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Add credentials**

```bash
cp config.example.yaml config.yaml
# Edit config.yaml and fill in your KIS app_key and app_secret
```

**3. Run the pipeline** (fetches today's data and computes scores)

```bash
python pipeline/run.py
```

**4. Serve the dashboard**

```bash
python dashboard/app.py
# Opens http://localhost:8080 automatically
```

---

## Viewing without the pipeline

The dashboard also works as a static file — open `dashboard/dashboard.html` directly in a browser alongside `data/output/latest.json` (it falls back to a relative path fetch). No server required.

---

## Data sources

| Purpose | KIS endpoint |
|---|---|
| Short-sale ranking (top 30 per market) | `FHPST04820000` |
| Daily short-sale trend per ticker | `FHPST04830000` |
| Foreign / institution aggregate | `FHPTJ04400000` |
| Per-ticker investor trade (backfill) | `FHPTJ04160001` |

Base URL (production): `https://openapi.koreainvestment.com:9443`

---

## Requirements

- Python 3.11+
- KIS Open API credentials (free registration at [apiportal.koreainvestment.com](https://apiportal.koreainvestment.com))
- Anthropic API key (optional — enables AI-generated analysis bullets)
