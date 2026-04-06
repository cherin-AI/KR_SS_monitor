# KPI Peer Review — Korea Short Pressure Monitor

## Background

This dashboard monitors short selling pressure across the Korean stock market using daily data from the KIS (Korea Investment & Securities) Open API. It is intended as an analytical tool for identifying stocks under significant short pressure and evaluating the conviction and confirmation of that pressure on any given trading day.

---

## Universe & Data Scope

- **Universe:** Top 300 Korean stocks by market capitalization, spanning both KOSPI and KOSDAQ
- **Source:** KIS Open API (live, post-market data pulled after 15:44 KST)
- **Important caveat:** This universe covers the top 300 by market cap only — not a random or breadth-representative sample of the full market. KOSPI has ~800 listed stocks and KOSDAQ has ~1,600. The universe likely covers 70–80% of total market cap but excludes small and mid caps entirely. **No KPI in this dashboard claims to represent the full KOSPI or KOSDAQ market.**

---

## Score Formula

Each stock receives a composite short-pressure score computed as a weighted sum of z-scores:

```
score =
    0.35 × z(ssts_vol_rlim)           # short turnover % today
  + 0.15 × z(ssts_vol_rlim_5d_avg)   # short turnover % 5-day average
  + 0.25 × z(−frgn_ntby_tr_pbmn)     # foreign net sell value (KRW mn)
  + 0.15 × z(−orgn_ntby_tr_pbmn)     # institution net sell value (KRW mn)
  + 0.10 × z(−prdy_ctrt)             # negative price change %
```

Z-scores are computed **within each market separately** (KOSPI and KOSDAQ independently), so scores are not comparable across markets in absolute terms. All five components must be present for a stock to receive a score; stocks missing any component are flagged `incomplete`.

---

## KPI Definitions

### KPI 1 — High-Pressure Stocks

| Field | Detail |
|---|---|
| **Formula** | Count of scored stocks where `score ≥ 1.0` |
| **Input data** | `short_pressure_score` for all 300 stocks |
| **Categories** | `NONE` = 0 stocks · `FOCUSED` = 1–5 stocks · `BROAD` = 6+ stocks |
| **Intent** | Headline count — how many stocks in the top-300 universe are under significant short pressure today |
| **Limitation** | Threshold of 1.0 is fixed; it has not been backtested against outcomes |

---

### KPI 2 — Foreign & Inst Alignment

| Field | Detail |
|---|---|
| **Formula** | Among high-pressure stocks (score ≥ 1.0): `count(frgn_ntby_tr_pbmn < 0 AND orgn_ntby_tr_pbmn < 0) / count(high-pressure)` expressed as % |
| **Input data** | `frgn_ntby_tr_pbmn` (foreign net buy/sell, KRW mn) and `orgn_ntby_tr_pbmn` (institution net buy/sell, KRW mn) |
| **Categories** | `ALIGNED` ≥ 60% · `MIXED` 30–60% · `DIVERGED` < 30% |
| **Intent** | Measures conviction behind the short pressure signal. When foreign and institutional money are simultaneously selling the same high-pressure names, the signal is coordinated and harder to dismiss. Low alignment (diverged) means the two sides disagree, weakening the overall pressure narrative |
| **Limitation** | Uses end-of-day net flow values only — intraday timing of flows is not captured. A stock could show net sell for both even if the selling occurred at different times of day |

---

### KPI 3 — Score-Weighted Return

| Field | Detail |
|---|---|
| **Formula** | `Σ(score × prdy_ctrt) / Σ(score)` across all stocks with `score > 0` |
| **Input data** | `short_pressure_score` and `prdy_ctrt` (price change % vs previous close) |
| **Categories** | `CONFIRMED` < −1% · `WEAK` −1% to 0% · `UNCONFIRMED` ≥ 0% |
| **Intent** | Measures whether today's short pressure is being validated by price action. Weighted by score so higher-conviction names count proportionally more. A strongly negative value means the model's top-ranked stocks are falling — confirming the signal. A flat or positive value means shorts are positioned but price has not reacted |
| **Limitation** | Only uses stocks with positive scores (roughly the top half of the ranked list). Computed on same-day return only — no multi-day confirmation. A single large-cap outlier with a very high score can dominate the weighted average |

---

### KPI 4 — Unconfirmed Shorts

| Field | Detail |
|---|---|
| **Formula** | Among high-pressure stocks (score ≥ 1.0): count where `prdy_ctrt ≥ 0%` |
| **Input data** | `short_pressure_score` and `prdy_ctrt` |
| **Categories** | `HIGH` ≥ 4 names · `MODERATE` 2–3 names · `LOW` 0–1 names |
| **Intent** | Identifies stocks where heavy short positioning exists but price has not broken down on the day. These names are either: (a) shorts positioned early ahead of a coming move, or (b) shorts that are wrong and may face a squeeze. The most forward-looking of the four KPIs — the watch list for next-day monitoring |
| **Limitation** | Uses only single-day price change. A stock up +0.1% and a stock up +5% are treated identically. Does not distinguish between "shorts are early" and "shorts are wrong" — requires qualitative judgment |

---

## Questions for Peer Review

1. Are the category thresholds for KPI 2 (60% / 30%) analytically defensible, or should they be calibrated differently?
2. For KPI 3, is restricting to `score > 0` stocks the right approach, or would it be more rigorous to use only `score ≥ 1.0` (high-pressure only)?
3. For KPI 4, should the `prdy_ctrt ≥ 0%` threshold be adjusted (e.g. ≥ +0.5% to filter out noise around zero)?
4. Given the large-cap-only universe, are there any systemic biases in these KPIs we should account for (e.g. index rebalancing effects on foreign flows)?
5. Is the score formula weighting (0.35 / 0.15 / 0.25 / 0.15 / 0.10) appropriate, or should it be reviewed given the 300-stock universe vs the original 60-stock design?
