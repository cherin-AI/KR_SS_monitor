"""
pipeline/validate_universe.py

Validation runner for a curated short-pressure universe.

Use this when you already know the 30 KOSPI/KOSDAQ names or tickers you want
to audit. It skips the ranking endpoint entirely and performs per-ticker KIS
calls for:
  - daily short-sale history (today + 5d average)
  - foreign / institution net values
  - no fallback layers
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date
from pathlib import Path

import httpx
import pandas as pd

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.auth import get_token, load_config
from pipeline.fetcher import (
    fetch_daily_short_snapshot,
    fetch_investor_trade_by_stock_daily,
    fetch_stock_meta,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate a curated universe using per-stock KIS calls.")
    p.add_argument("--input", required=True, help="CSV with at least mksc_shrn_iscd; optional hts_kor_isnm/display_name.")
    p.add_argument("--output", default="data/validation/universe_validation.csv", help="Output CSV path.")
    p.add_argument("--market", default="J", choices=["J", "NX"], help="Market code for the universe.")
    p.add_argument("--lookback-days", type=int, default=5, help="Number of trading days for the short-ratio average.")
    p.add_argument("--trade-date", default=None, help="Explicit latest trading date (YYYYMMDD). If omitted, auto-detect from short-sale history.")
    p.add_argument("--max-concurrency", type=int, default=4, help="Parallel per-ticker requests.")
    return p.parse_args()


async def _run_one(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    ticker: str,
    lookback_days: int,
    market: str,
    trade_date: str,
) -> dict:
    short = await fetch_daily_short_snapshot(client, token, config, ticker, lookback_days, trade_date_end=trade_date)
    try:
        flow = await fetch_investor_trade_by_stock_daily(client, token, config, market, ticker, trade_date=trade_date)
        flow_error = None
    except Exception as exc:
        logger.warning("Investor flow unavailable for %s: %s", ticker, exc)
        flow = {}
        flow_error = str(exc)

    return {
        "mksc_shrn_iscd": ticker,
        "short_today_ratio": short.get("short_today_ratio"),
        "short_5d_avg": short.get("short_5d_avg"),
        "short_today_numerator": short.get("short_today_numerator"),
        "short_today_trade_value": short.get("short_today_trade_value"),
        "short_today_trade_ratio": short.get("short_today_trade_ratio"),
        "short_trade_date": short.get("short_trade_date"),
        "foreign_net_m": flow.get("frgn_ntby_tr_pbmn"),
        "foreign_source": "FHPTJ04160001 investor-trade-by-stock-daily",
        "inst_net_m": flow.get("orgn_ntby_tr_pbmn"),
        "inst_source": "FHPTJ04160001 investor-trade-by-stock-daily",
        "short_source": short.get("source"),
        "flow_error": flow_error,
    }


async def main() -> None:
    args = _parse_args()
    config = load_config("config.yaml")
    token = get_token(config)

    universe = pd.read_csv(args.input, dtype={"mksc_shrn_iscd": str})
    if "mksc_shrn_iscd" not in universe.columns:
        raise ValueError("Input CSV must include mksc_shrn_iscd")
    universe["mksc_shrn_iscd"] = universe["mksc_shrn_iscd"].astype(str).str.zfill(6)

    if "display_name" not in universe.columns:
        universe["display_name"] = None
    if "hts_kor_isnm" not in universe.columns:
        universe["hts_kor_isnm"] = None

    sem = asyncio.Semaphore(args.max_concurrency)
    results: list[dict] = []

    async with httpx.AsyncClient(timeout=30) as client:
        probe_ticker = universe["mksc_shrn_iscd"].iloc[0]
        if args.trade_date:
            trade_date = args.trade_date
        else:
            probe = await fetch_daily_short_snapshot(
                client,
                token,
                config,
                str(probe_ticker),
                args.lookback_days,
                trade_date_end=date.today().strftime("%Y%m%d"),
            )
            trade_date = str(probe.get("short_trade_date") or "")
            if not trade_date:
                raise RuntimeError("Could not determine latest trading date from short-sale history.")
        logger.info("Using latest trading date %s for all KIS calls", trade_date)

        async def _wrapped(ticker: str) -> None:
            async with sem:
                try:
                    results.append(await _run_one(client, token, config, ticker, args.lookback_days, args.market, trade_date))
                except Exception as exc:
                    logger.warning("Validation fetch failed for %s: %s", ticker, exc)
                    results.append({
                        "mksc_shrn_iscd": ticker,
                        "short_today_ratio": None,
                        "short_5d_avg": None,
                        "short_today_numerator": None,
                        "short_today_trade_value": None,
                        "short_today_trade_ratio": None,
                        "short_trade_date": None,
                        "foreign_net_m": None,
                        "foreign_source": None,
                        "inst_net_m": None,
                        "inst_source": None,
                        "short_source": None,
                        "flow_error": None,
                    })

        await asyncio.gather(*(_wrapped(str(t)) for t in universe["mksc_shrn_iscd"].astype(str).tolist()))

        meta_rows: dict[str, dict] = {}
        for ticker in universe["mksc_shrn_iscd"].astype(str).tolist():
            try:
                meta_rows[ticker] = await fetch_stock_meta(client, token, config, ticker)
            except Exception:
                meta_rows[ticker] = {}

    out = pd.DataFrame(results)
    out = universe.merge(out, on="mksc_shrn_iscd", how="left")
    if meta_rows:
        meta_df = pd.DataFrame.from_dict(meta_rows, orient="index").reset_index(names="mksc_shrn_iscd")
        out = out.merge(meta_df[["mksc_shrn_iscd", "display_name", "name_ko", "sector"]], on="mksc_shrn_iscd", how="left", suffixes=("", "_meta"))
        if "display_name_meta" in out.columns:
            out["display_name"] = out["display_name"].fillna(out["display_name_meta"])
            out = out.drop(columns=["display_name_meta"])
        if "name_ko_meta" in out.columns:
            out["name_ko"] = out["name_ko"].fillna(out["name_ko_meta"])
            out = out.drop(columns=["name_ko_meta"])

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    logger.info("Saved %s (%d rows)", out_path, len(out))
    print(out.to_string(index=False))


if __name__ == "__main__":
    asyncio.run(main())
