"""
pipeline/run.py
Daily batch entry point for the short pressure monitor.

Universe: top-N common stocks by market cap (KOSPI + KOSDAQ combined),
          fetched from Naver Finance and filtered for common stocks only.

Usage:
    python pipeline/run.py               # uses config.yaml
    python pipeline/run.py --config path/to/config.yaml
    python pipeline/run.py --dry-run     # fetch data but skip writing output
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # loads .env from project root for local runs

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.auth import get_token, load_config
from pipeline.fetcher import (
    fetch_foreign_inst_aggregate,
    fetch_inquire_investor_bulk,
    fetch_market_cap_universe,
    fetch_short_snapshots_bulk,
    fetch_stock_meta,
)
from pipeline.score import build_meta_summary, compute_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── AI summary ────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """\
You are a quantitative market monitor for Korean equities. You analyze short-selling \
pressure signals and translate them into concise, factual observations for professional investors.

Rules:
- Output exactly 3 to 5 bullet points, each on its own line starting with "•"
- No intro, no conclusion, no headers, no markdown formatting beyond the bullet character
- Each bullet must follow this exact structure: a short summary sentence (4–8 words, ends with a period), then one supporting detail sentence. Example: "Short pressure is broad. 14 names exceed the threshold with 100% showing rising short turnover, suggesting systematic bearish positioning."
- Do not use semicolons to join clauses — use two separate sentences
- Do not repeat the same KPI across multiple bullets
- Do not use vague sentiment language without data support
- Do not give investment advice or price targets
- Do not reference sectors, names, or data not explicitly given to you
- Do not mention retail investors, retail short interest, or retail participation — the score formula contains no retail data and Korea's retail short market is structurally negligible; any such contrast is unsupported
- Do not reference or explain the score formula, its weights, or how individual components are weighted — the formula is an implementation detail, not a market observation. Insights must come from the KPI values, top-stock data, and what the numbers reveal about market behaviour, not from describing how the score is constructed

Prioritise these themes in order:
1. Breadth vs concentration of short pressure (KPI 1 + KPI 2)
2. Price action confirmation vs divergence (KPI 3)
3. Squeeze risk assessment (KPI 4)
4. Large-cap / liquid name concentration (top names list)
5. Early reversal or short exhaustion signals\
"""


def _build_kpis(high: pd.DataFrame) -> dict:
    """Compute the four KPI values from the high-pressure subset."""
    count = len(high)

    eligible = high.dropna(subset=["ssts_vol_rlim", "ssts_vol_rlim_5d"])
    rising_n  = int((eligible["ssts_vol_rlim"] > eligible["ssts_vol_rlim_5d"]).sum())
    rising_pct = round(rising_n / len(eligible) * 100, 1) if len(eligible) else None

    score_sum = high["score"].sum()
    kpi3 = round(float((high["score"] * high["prdy_ctrt"]).sum() / score_sum), 2) \
        if score_sum and high["prdy_ctrt"].notna().any() else None

    kpi4 = int((high["prdy_ctrt"] >= 0).sum())

    return {
        "count":       count,
        "rising_n":    rising_n,
        "rising_pct":  rising_pct,
        "kpi3":        kpi3,
        "kpi4":        kpi4,
    }


def _rules_based_summary(high: pd.DataFrame, kpis: dict) -> list[str]:
    """
    Deterministic fallback bullets generated from dashboard metrics.
    Called when the AI API is unavailable or returns an invalid response.
    """
    bullets: list[str] = []
    count      = kpis["count"]
    rising_pct = kpis["rising_pct"]
    kpi3       = kpis["kpi3"]
    kpi4       = kpis["kpi4"]

    # 1. Breadth
    if count == 0:
        bullets.append(
            "No stocks currently exceed the high-pressure threshold. "
            "Short positioning appears minimal across the universe."
        )
    elif count <= 5:
        rising_str = f" with {rising_pct:.0f}% showing rising short turnover" if rising_pct is not None else ""
        bullets.append(
            f"Short pressure is narrowly concentrated in {count} names{rising_str}, "
            "suggesting targeted rather than broad bearish positioning."
        )
    else:
        rising_str = f"{rising_pct:.0f}% show rising short turnover versus their 5-day average" \
            if rising_pct is not None else "most show rising short turnover"
        bullets.append(
            f"Short pressure is broad: {count} stocks exceed the threshold and {rising_str}, "
            "indicating active widespread bearish positioning."
        )

    # 2. Confirmation
    if kpi3 is not None:
        if kpi3 < -1.0:
            bullets.append(
                f"Price action confirms the signal. Score-weighted return of {kpi3:.2f}% shows "
                "that the highest-conviction shorts are declining in line with positioning."
            )
        elif kpi3 < 0:
            bullets.append(
                f"Price action weakly confirms. Score-weighted return of {kpi3:.2f}% shows modest "
                "declines among high-pressure names — signal has traction but not yet strong follow-through."
            )
        else:
            bullets.append(
                f"Price action diverges from the signal. Score-weighted return of {kpi3:.2f}% — "
                "pressured names are not falling, which reduces conviction in the short thesis."
            )

    # 3. Squeeze risk
    if kpi4 == 0:
        bullets.append(
            "No squeeze risk detected. All high-pressure stocks are flat or declining, "
            "so shorts face no near-term forced-covering pressure."
        )
    elif kpi4 <= 3:
        bullets.append(
            f"Moderate squeeze risk. {kpi4} high-pressure stocks are rising against short flows — "
            "monitor these names for potential covering triggers."
        )
    else:
        bullets.append(
            f"Elevated squeeze risk. {kpi4} high-pressure names show positive returns despite strong "
            "short positioning, raising the risk of disorderly short covering."
        )

    # 4. Concentration
    if count > 0:
        top3 = high.head(3)["display_name"].tolist()
        names = ", ".join(top3)
        bullets.append(
            f"Pressure is concentrated in liquid names: {names} rank at the top, "
            "which may reflect institutional or macro-driven positioning."
        )

    return bullets[:5]


async def generate_ai_summary(
    scored: pd.DataFrame,
    meta: dict,
    threshold: float,
    today_str: str,
) -> list[str]:
    """
    Generate 3–5 market-monitor bullet points using Claude Haiku.

    Falls back to `_rules_based_summary` if:
    - ANTHROPIC_API_KEY env var is not set
    - The API call fails for any reason
    - The response cannot be parsed into at least 3 bullets
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    complete = scored[~scored["incomplete"]].copy()
    high = complete[complete["score"] >= threshold].sort_values("score", ascending=False)
    kpis = _build_kpis(high)

    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — using rules-based fallback summary")
        return _rules_based_summary(high, kpis)

    # ── Build structured prompt payload ──────────────────────────────────────
    top5_lines = "\n".join(
        f"  {i+1}. {row['display_name']} ({row['mksc_shrn_iscd']}): "
        f"score={row['score']:.2f}, return={row['prdy_ctrt']:+.2f}%"
        for i, (_, row) in enumerate(high.head(5).iterrows())
    ) or "  (none above threshold)"

    kpi1_badge = "NONE" if kpis["count"] == 0 else "FOCUSED" if kpis["count"] <= 5 else "BROAD"
    kpi2_str   = f"{kpis['rising_pct']:.1f}%" if kpis["rising_pct"] is not None else "N/A"
    kpi2_badge = ("FADING" if kpis["rising_pct"] is not None and kpis["rising_pct"] <= 33
                  else "RISING" if kpis["rising_pct"] is not None and kpis["rising_pct"] > 66
                  else "STEADY")
    kpi3_str   = f"{kpis['kpi3']:+.2f}%" if kpis["kpi3"] is not None else "N/A"
    kpi3_badge = ("CONFIRMED" if kpis["kpi3"] is not None and kpis["kpi3"] < -1.0
                  else "WEAK" if kpis["kpi3"] is not None and kpis["kpi3"] < 0
                  else "UNCONFIRMED")
    kpi4_badge = "LOW" if kpis["kpi4"] <= 1 else "MODERATE" if kpis["kpi4"] <= 3 else "HIGH"

    user_prompt = f"""\
Korea Short Pressure Monitor — {today_str}
Universe: {meta['total_count']} large-cap stocks (KOSPI {meta['kospi_count']} / KOSDAQ {meta['kosdaq_count']})

KPI Summary:
• High-pressure stocks (score ≥ 1.0): {kpis['count']} [{kpi1_badge}]
• Rising short pressure: {kpi2_str} of high-pressure stocks have short turnover above 5-day avg [{kpi2_badge}]
• Score-weighted return: {kpi3_str} [{kpi3_badge}]
• Unconfirmed shorts (score ≥ 1.0 & price ≥ 0%): {kpis['kpi4']} [{kpi4_badge}]

Score formula components (weights):
  z(short turnover %) ×0.35  +  z(short turnover accel.) ×0.15
  + z(−foreign net flow) ×0.25  +  z(−institution net flow) ×0.15  +  z(−price chg%) ×0.10

Top 5 names by short pressure score:
{top5_lines}

Write 3 to 5 bullet points interpreting what this data signals today.\
"""

    try:
        import anthropic  # deferred import — only needed if key is present

        client = anthropic.AsyncAnthropic(api_key=api_key)
        message = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )
        text = message.content[0].text.strip()
        bullets = [
            line.lstrip("•·-– ").strip()
            for line in text.splitlines()
            if line.strip().startswith(("•", "·", "-", "–"))
        ]
        if len(bullets) >= 3:
            logger.info("AI summary generated: %d bullets", len(bullets))
            return bullets[:5]

        logger.warning("AI response yielded <3 bullets — using rules-based fallback. Response: %s", text[:200])

    except Exception as exc:
        logger.warning("AI summary generation failed (%s) — using rules-based fallback", exc)

    return _rules_based_summary(high, kpis)


def _stock_meta_cache_path(config: dict) -> Path:
    cache_dir = Path(config["output"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "stock_meta.json"


def _contains_korean(s: str) -> bool:
    """Return True if s contains any Hangul syllable characters."""
    return any("\uAC00" <= c <= "\uD7A3" for c in (s or ""))


def _load_stock_meta_raw(path: Path) -> dict[str, dict] | None:
    """Load the stock meta cache regardless of age (used to salvage English names on refresh)."""
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        stocks = payload.get("stocks", {})
        return stocks if isinstance(stocks, dict) else None
    except Exception:
        return None


def _load_stock_meta_cache(path: Path, max_age_days: int) -> dict[str, dict] | None:
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            payload = json.load(f)
        updated_at = datetime.fromisoformat(payload["updated_at"])
        age_days = (datetime.now(timezone.utc) - updated_at).total_seconds() / 86400
        if age_days > max_age_days:
            return None
        stocks = payload.get("stocks", {})
        return stocks if isinstance(stocks, dict) else None
    except Exception:
        return None


def _write_stock_meta_cache(path: Path, stocks: dict[str, dict]) -> None:
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "stocks": stocks,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


async def _fetch_stock_meta_map(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    tickers: list[str],
    max_concurrency: int = 5,
) -> dict[str, dict]:
    sem = asyncio.Semaphore(max_concurrency)
    results: dict[str, dict] = {}

    async def _fetch_one(ticker: str) -> None:
        async with sem:
            try:
                results[ticker] = await fetch_stock_meta(client, token, config, ticker)
            except Exception as exc:
                logger.warning("Stock meta fetch failed for %s: %s", ticker, exc)

    await asyncio.gather(*(_fetch_one(ticker) for ticker in tickers))
    return results


async def run_pipeline(config: dict, dry_run: bool = False) -> dict:
    token = get_token(config)
    lookback = config["schedule"]["lookback_days"]
    top_n = config.get("universe", {}).get("size", 300)
    meta_cache_days = int(config["output"].get("meta_cache_days", 7))
    meta_cache_path = _stock_meta_cache_path(config)

    # ── 1. Universe ───────────────────────────────────────────────────────────
    logger.info("Fetching market cap universe (top %d common stocks)…", top_n)
    universe = fetch_market_cap_universe(top_n=top_n)
    logger.info("Universe ready: %d stocks (%d KOSPI / %d KOSDAQ)",
                len(universe),
                sum(1 for u in universe if u["market"] == "KOSPI"),
                sum(1 for u in universe if u["market"] == "KOSDAQ"))

    all_tickers  = [u["ticker"] for u in universe]
    kospi_tickers  = [u["ticker"] for u in universe if u["market"] == "KOSPI"]
    kosdaq_tickers = [u["ticker"] for u in universe if u["market"] == "KOSDAQ"]

    # Build ticker → universe entry map for easy lookup
    universe_map = {u["ticker"]: u for u in universe}

    async with httpx.AsyncClient(timeout=30) as client:

        # ── 1b. Stock metadata (English display names) ────────────────────────
        stock_meta = _load_stock_meta_cache(meta_cache_path, meta_cache_days)
        missing_meta = [t for t in all_tickers if t not in (stock_meta or {})]
        if stock_meta is None:
            # Cache expired — load stale entries to preserve curated English names that
            # the KIS API would otherwise overwrite with Korean abbreviated names.
            stale_meta = _load_stock_meta_raw(meta_cache_path) or {}
            logger.info("Fetching stock metadata for %d tickers…", len(all_tickers))
            stock_meta = await _fetch_stock_meta_map(client, token, config, all_tickers)
            # If the freshly fetched display_name is Korean but the stale cache had an
            # English name, keep the English name so it survives the refresh cycle.
            for ticker, entry in stock_meta.items():
                new_dn = entry.get("display_name", "")
                old_dn = stale_meta.get(ticker, {}).get("display_name", "")
                if _contains_korean(new_dn) and old_dn and not _contains_korean(old_dn):
                    entry["display_name"] = old_dn
                    entry["name"] = old_dn
            _write_stock_meta_cache(meta_cache_path, stock_meta)
        elif missing_meta:
            logger.info("Refreshing missing stock metadata for %d tickers…", len(missing_meta))
            fetched_meta = await _fetch_stock_meta_map(client, token, config, missing_meta)
            stock_meta.update(fetched_meta)
            _write_stock_meta_cache(meta_cache_path, stock_meta)
        logger.info("Stock metadata ready (%d cached / %d total)", len(stock_meta or {}), len(all_tickers))

        # ── 2. Short snapshots (today ratio + 5d avg) ─────────────────────────
        logger.info("Fetching short-sale snapshots for %d tickers…", len(all_tickers))
        snapshots = await fetch_short_snapshots_bulk(
            client, token, config, universe, lookback_days=lookback,
            concurrency=5, sleep_between=0.3,
        )
        latest_trade_date = next(
            (v.get("short_trade_date") for v in snapshots.values() if v.get("short_trade_date")),
            None,
        )
        logger.info("Short snapshots done. Latest trade date: %s", latest_trade_date)

        # ── 3. Foreign / inst investor data ───────────────────────────────────
        logger.info("Fetching inquire-investor (FHKST01010900) for %d tickers…", len(all_tickers))
        investor_data = await fetch_inquire_investor_bulk(
            client, token, config, all_tickers
        )

        # Fallback: fi_total (FHPTJ04400000) for tickers absent from results OR
        # where inquire_investor returned null values (truthy dict but null fields)
        still_missing = [
            t for t in all_tickers
            if not investor_data.get(t)
            or investor_data[t].get("frgn_ntby_tr_pbmn") is None
        ]
        fi_kospi: pd.DataFrame = pd.DataFrame()
        fi_kosdaq: pd.DataFrame = pd.DataFrame()
        if still_missing:
            logger.info("fi_total fallback (FHPTJ04400000) for %d tickers still missing…", len(still_missing))
            fi_kospi  = await fetch_foreign_inst_aggregate(client, token, config, "J")
            fi_kosdaq = await fetch_foreign_inst_aggregate(client, token, config, "NX")

    # ── 4. Merge ──────────────────────────────────────────────────────────────
    meta_map = stock_meta or {}

    def _display_name(ticker: str, name_ko: str) -> str:
        meta = meta_map.get(str(ticker), {})
        return str(meta.get("display_name") or meta.get("name") or name_ko or ticker)

    # Build unified DataFrame from universe
    rows = []
    for u in universe:
        ticker  = u["ticker"]
        snap    = snapshots.get(ticker, {})
        price   = snap.get("short_today_price")
        rows.append({
            "mksc_shrn_iscd":   ticker,
            "display_name":     _display_name(ticker, u["name_ko"]),
            "name_ko":          u["name_ko"],
            "market":           u["market"],
            "market_cap_100m":  u["market_cap_100m"],
            "ssts_vol_rlim":    snap.get("short_today_ratio"),
            "ssts_vol_rlim_5d": snap.get("short_5d_avg"),
            "stck_prpr":        price,
            "prdy_ctrt":        u.get("prdy_ctrt"),  # from Naver Finance scrape
            "frgn_ntby_tr_pbmn": None,
            "orgn_ntby_tr_pbmn": None,
        })

    df = pd.DataFrame(rows)

    # Apply primary investor data (FHKST01010900)
    if investor_data:
        df["frgn_ntby_tr_pbmn"] = df["mksc_shrn_iscd"].map(
            lambda t: investor_data.get(str(t), {}).get("frgn_ntby_tr_pbmn")
        )
        df["orgn_ntby_tr_pbmn"] = df["mksc_shrn_iscd"].map(
            lambda t: investor_data.get(str(t), {}).get("orgn_ntby_tr_pbmn")
        )

    # Apply fi_total fallback (FHPTJ04400000) for any remaining nulls
    _fi_frames = [f for f in [fi_kospi, fi_kosdaq] if not f.empty]
    fi_all = pd.concat(_fi_frames, ignore_index=True) if _fi_frames else pd.DataFrame()
    if not fi_all.empty:
        merge_cols = [c for c in ["mksc_shrn_iscd", "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"] if c in fi_all.columns]
        df = df.merge(fi_all[merge_cols], on="mksc_shrn_iscd", how="left", suffixes=("", "_fi"))
        for col in ["frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"]:
            fi_col = col + "_fi"
            if fi_col in df.columns:
                df[col] = df[col].combine_first(df[fi_col])
                df.drop(columns=[fi_col], inplace=True)

    # ── 5. Score ──────────────────────────────────────────────────────────────
    logger.info("Computing scores across %d stocks…", len(df))
    threshold = config.get("score", {}).get("high_pressure_threshold", 1.0)
    scored = compute_score(df)

    # ── 5b. AI summary ────────────────────────────────────────────────────────
    meta = build_meta_summary(scored, threshold)
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info("Generating AI summary…")
    ai_summary = await generate_ai_summary(scored, meta, threshold, today_str)

    # ── 6. Serialise ──────────────────────────────────────────────────────────
    def df_to_records(frame: pd.DataFrame) -> list[dict]:
        cols = [
            "rank", "mksc_shrn_iscd", "display_name", "name_ko",
            "market", "market_cap_100m",
            "ssts_vol_rlim", "ssts_vol_rlim_5d",
            "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn",
            "prdy_ctrt",
            "z_short_today", "z_short_5d", "z_foreign", "z_inst", "z_return",
            "score", "incomplete",
        ]
        out = []
        for _, row in frame.iterrows():
            rec: dict = {}
            for c in cols:
                val = row.get(c)
                if not isinstance(val, bool) and pd.isna(val) if not isinstance(val, bool) else False:
                    rec[c] = None
                elif isinstance(val, float):
                    rec[c] = round(val, 4)
                else:
                    rec[c] = val
            out.append(rec)
        return out

    generated_at = datetime.now(timezone.utc).isoformat()

    payload = {
        "date":         today_str,
        "generated_at": generated_at,
        "stocks":       df_to_records(scored),
        "meta":         meta,
        "ai_summary":   ai_summary,
    }

    # ── 7. Write ──────────────────────────────────────────────────────────────
    if not dry_run:
        out_dir = Path(config["output"]["json_dir"])
        out_dir.mkdir(parents=True, exist_ok=True)
        date_file   = out_dir / f"{today_str.replace('-', '')}.json"
        latest_file = out_dir / "latest.json"
        blob = json.dumps(payload, ensure_ascii=False, indent=2)
        date_file.write_text(blob, encoding="utf-8")
        latest_file.write_text(blob, encoding="utf-8")
        logger.info("Output written → %s", date_file)
    else:
        logger.info("[dry-run] skipping file write")

    logger.info(
        "Done. Total=%d KOSPI=%d KOSDAQ=%d high_pressure=%d top=%s (%.2f)",
        meta["total_count"], meta["kospi_count"], meta["kosdaq_count"],
        meta["high_pressure_count"],
        meta["top_score_ticker"], meta["top_score_value"] or 0,
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Short Pressure Monitor pipeline")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    asyncio.run(run_pipeline(config, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
