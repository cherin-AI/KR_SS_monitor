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
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.auth import get_token, load_config
from pipeline.fetcher import (
    fetch_foreign_inst_aggregate,
    fetch_inquire_investor_bulk,
    fetch_short_snapshots_bulk,
    fetch_stock_meta,
)
from pipeline.krx_short_selling import fetch_market_cap_universe
from pipeline.score import build_meta_summary, compute_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _stock_meta_cache_path(config: dict) -> Path:
    cache_dir = Path(config["output"]["cache_dir"])
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "stock_meta.json"


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
            logger.info("Fetching stock metadata for %d tickers…", len(all_tickers))
            stock_meta = await _fetch_stock_meta_map(client, token, config, all_tickers)
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

        # Fallback: fi_total (FHPTJ04400000) only if any tickers still missing
        still_missing = [t for t in all_tickers if not investor_data.get(t)]
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
    fi_all = pd.concat([f for f in [fi_kospi, fi_kosdaq] if not f.empty], ignore_index=True)
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

    today_str    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    generated_at = datetime.now(timezone.utc).isoformat()

    meta = build_meta_summary(scored, threshold)
    payload = {
        "date":         today_str,
        "generated_at": generated_at,
        "stocks":       df_to_records(scored),
        "meta":         meta,
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
