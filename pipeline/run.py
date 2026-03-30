"""
pipeline/run.py
Daily batch entry point for the short pressure monitor.

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
    fetch_5d_averages,
    fetch_foreign_inst_aggregate,
    fetch_investor_trade_fallbacks,
    fetch_investor_trend_estimate_fallbacks,
    fetch_short_sale_ranking,
    fetch_stock_meta,
)
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
            except Exception as exc:  # pragma: no cover - logged fallback path
                logger.warning("Stock meta fetch failed for %s: %s", ticker, exc)

    await asyncio.gather(*(_fetch_one(ticker) for ticker in tickers))
    return results


async def run_pipeline(config: dict, dry_run: bool = False) -> dict:
    token = get_token(config)
    lookback = config["schedule"]["lookback_days"]
    meta_cache_days = int(config["output"].get("meta_cache_days", 7))
    meta_cache_path = _stock_meta_cache_path(config)

    def _latest_trade_date(*frames: pd.DataFrame) -> str | None:
        dates: list[str] = []
        for frame in frames:
            if frame is None or frame.empty:
                continue
            for col in ("stnd_date2", "stnd_date1", "stck_bsop_date"):
                if col not in frame.columns:
                    continue
                values = frame[col].dropna().astype(str).tolist()
                dates.extend(v for v in values if v)
        return max(dates) if dates else None

    async with httpx.AsyncClient(timeout=30) as client:

        # ── 1. Universe ───────────────────────────────────────────────────────
        logger.info("Fetching short-sale ranking — KOSPI")
        kospi_raw = await fetch_short_sale_ranking(client, token, config, "J")

        logger.info("Fetching short-sale ranking — KOSDAQ")
        kosdaq_raw = await fetch_short_sale_ranking(client, token, config, "NX")

        kospi_tickers = kospi_raw["mksc_shrn_iscd"].tolist()
        kosdaq_tickers = kosdaq_raw["mksc_shrn_iscd"].tolist()
        all_tickers = kospi_tickers + kosdaq_tickers
        latest_trade_date = _latest_trade_date(kospi_raw, kosdaq_raw)
        if latest_trade_date is None:
            raise RuntimeError("Could not determine latest trading date from short-sale ranking payloads")
        logger.info("Using latest trading date %s for all KIS calls", latest_trade_date)

        # ── 1b. Stock metadata (English display names) ───────────────────────
        stock_meta = _load_stock_meta_cache(meta_cache_path, meta_cache_days)
        missing_meta = [t for t in all_tickers if t not in (stock_meta or {})]
        if stock_meta is None:
            logger.info("Fetching stock metadata for %d tickers...", len(all_tickers))
            stock_meta = await _fetch_stock_meta_map(client, token, config, all_tickers)
            _write_stock_meta_cache(meta_cache_path, stock_meta)
        elif missing_meta:
            logger.info("Refreshing missing stock metadata for %d tickers...", len(missing_meta))
            fetched_meta = await _fetch_stock_meta_map(client, token, config, missing_meta)
            stock_meta.update(fetched_meta)
            _write_stock_meta_cache(meta_cache_path, stock_meta)
        logger.info("Stock metadata ready (%d cached / %d total)", len(stock_meta or {}), len(all_tickers))

        # ── 2. 5-day short trend ─────────────────────────────────────────────
        logger.info("Fetching 5-day short trend for %d tickers...", len(all_tickers))
        avg_5d = await fetch_5d_averages(
            client,
            token,
            config,
            all_tickers,
            lookback,
            trade_date_end=latest_trade_date,
        )

        # ── 3. Foreign / inst aggregate ──────────────────────────────────────
        logger.info("Fetching foreign/inst aggregate — KOSPI")
        fi_kospi = await fetch_foreign_inst_aggregate(client, token, config, "J")

        logger.info("Fetching foreign/inst aggregate — KOSDAQ")
        fi_kosdaq = await fetch_foreign_inst_aggregate(client, token, config, "NX")

        fi_kospi_tickers = set(fi_kospi["mksc_shrn_iscd"].astype(str).tolist()) if not fi_kospi.empty else set()
        fi_kosdaq_tickers = set(fi_kosdaq["mksc_shrn_iscd"].astype(str).tolist()) if not fi_kosdaq.empty else set()
        kospi_missing = [t for t in kospi_tickers if t not in fi_kospi_tickers]
        kosdaq_missing = [t for t in kosdaq_tickers if t not in fi_kosdaq_tickers]

        fi_kospi_fallback = {}
        fi_kosdaq_fallback = {}
        if kospi_missing:
            logger.info("Fetching investor-trade fallback — KOSPI (%d missing)", len(kospi_missing))
            fi_kospi_fallback = await fetch_investor_trade_fallbacks(
                client, token, config, "J", kospi_missing, trade_date=latest_trade_date
            )
        if kosdaq_missing:
            logger.info("Fetching investor-trade fallback — KOSDAQ (%d missing)", len(kosdaq_missing))
            fi_kosdaq_fallback = await fetch_investor_trade_fallbacks(
                client, token, config, "NX", kosdaq_missing, trade_date=latest_trade_date
            )

        # For KOSDAQ rows that still come back as 0M, use the estimate endpoint
        # and convert estimated net quantities to KRW using the current price.
        kospi_price_map = dict(zip(kospi_raw["mksc_shrn_iscd"], kospi_raw["stck_prpr"]))
        kosdaq_price_map = dict(zip(kosdaq_raw["mksc_shrn_iscd"], kosdaq_raw["stck_prpr"]))

        def _needs_estimate(row: pd.Series) -> bool:
            frgn = pd.to_numeric(row.get("frgn_ntby_tr_pbmn"), errors="coerce")
            orgn = pd.to_numeric(row.get("orgn_ntby_tr_pbmn"), errors="coerce")
            return pd.isna(frgn) or pd.isna(orgn) or (float(frgn) == 0.0 and float(orgn) == 0.0)

        kospi_base = kospi_raw.copy()
        if not fi_kospi.empty:
            kospi_base = kospi_base.merge(
                fi_kospi[["mksc_shrn_iscd", "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"]],
                on="mksc_shrn_iscd",
                how="left",
            )
        kospi_estimate_candidates = kospi_base.loc[kospi_base.apply(_needs_estimate, axis=1), "mksc_shrn_iscd"].astype(str).tolist()
        kosdaq_base = kosdaq_raw.copy()
        if not fi_kosdaq.empty:
            kosdaq_base = kosdaq_base.merge(
                fi_kosdaq[["mksc_shrn_iscd", "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"]],
                on="mksc_shrn_iscd",
                how="left",
            )
        kosdaq_estimate_candidates = kosdaq_base.loc[kosdaq_base.apply(_needs_estimate, axis=1), "mksc_shrn_iscd"].astype(str).tolist()

        kospi_estimate_fallback = {}
        kosdaq_estimate_fallback = {}
        if kospi_estimate_candidates:
            logger.info("Fetching estimate fallback — KOSPI (%d zero rows)", len(kospi_estimate_candidates))
            kospi_estimate_fallback = await fetch_investor_trend_estimate_fallbacks(
                client, token, config, kospi_estimate_candidates
            )
        if kosdaq_estimate_candidates:
            logger.info("Fetching estimate fallback — KOSDAQ (%d zero rows)", len(kosdaq_estimate_candidates))
            kosdaq_estimate_fallback = await fetch_investor_trend_estimate_fallbacks(
                client, token, config, kosdaq_estimate_candidates
            )

    # ── 4. Merge ──────────────────────────────────────────────────────────────
    def merge_market(
        universe: pd.DataFrame,
        fi: pd.DataFrame,
        fallback_map: dict[str, dict[str, float | None]],
        estimate_map: dict[str, dict[str, float | None]],
        price_map: dict[str, float | int | None],
    ) -> pd.DataFrame:
        df = universe.copy()
        meta_map = stock_meta or {}

        def _display_name(ticker: str) -> str:
            meta = meta_map.get(str(ticker), {})
            if meta.get("display_name"):
                return str(meta["display_name"])
            if meta.get("name"):
                return str(meta["name"])
            match = df.loc[df["mksc_shrn_iscd"] == ticker, "hts_kor_isnm"]
            return str(match.iloc[0]) if not match.empty else str(ticker)

        def _name_ko(ticker: str) -> str:
            meta = meta_map.get(str(ticker), {})
            if meta.get("name_ko"):
                return str(meta["name_ko"])
            match = df.loc[df["mksc_shrn_iscd"] == ticker, "hts_kor_isnm"]
            return str(match.iloc[0]) if not match.empty else ""

        df["display_name"] = df["mksc_shrn_iscd"].map(_display_name)
        df["name_ko"] = df["mksc_shrn_iscd"].map(_name_ko)
        df["ssts_vol_rlim_5d"] = df["mksc_shrn_iscd"].map(avg_5d)

        if not fi.empty:
            fi_cols = [c for c in ["mksc_shrn_iscd", "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"] if c in fi.columns]
            df = df.merge(fi[fi_cols], on="mksc_shrn_iscd", how="left")
        else:
            df["frgn_ntby_tr_pbmn"] = float("nan")
            df["orgn_ntby_tr_pbmn"] = float("nan")

        if fallback_map:
            fallback_frgn = df["mksc_shrn_iscd"].map(lambda t: fallback_map.get(str(t), {}).get("frgn_ntby_tr_pbmn"))
            fallback_orgn = df["mksc_shrn_iscd"].map(lambda t: fallback_map.get(str(t), {}).get("orgn_ntby_tr_pbmn"))
            df["frgn_ntby_tr_pbmn"] = df["frgn_ntby_tr_pbmn"].combine_first(fallback_frgn)
            df["orgn_ntby_tr_pbmn"] = df["orgn_ntby_tr_pbmn"].combine_first(fallback_orgn)

        if estimate_map:
            def _estimate_money(ticker: str, key: str) -> float | None:
                estimate = estimate_map.get(str(ticker), {})
                qty_key = "frgn_fake_ntby_qty" if key == "frgn_ntby_tr_pbmn" else "orgn_fake_ntby_qty"
                qty = estimate.get(qty_key)
                price = price_map.get(str(ticker))
                if qty is None or price in (None, 0):
                    return None
                return round(float(qty) * float(price) / 1_000_000, 3)

            for key in ["frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"]:
                est_series = df["mksc_shrn_iscd"].map(lambda t, k=key: _estimate_money(t, k))
                current = pd.to_numeric(df[key], errors="coerce")
                zero_mask = current.isna() | (current == 0.0)
                df.loc[zero_mask, key] = est_series.loc[zero_mask]

        return df

    kospi_merged = merge_market(kospi_raw, fi_kospi, fi_kospi_fallback, kospi_estimate_fallback, kospi_price_map)
    kosdaq_merged = merge_market(kosdaq_raw, fi_kosdaq, fi_kosdaq_fallback, kosdaq_estimate_fallback, kosdaq_price_map)

    # ── 5. Score ──────────────────────────────────────────────────────────────
    logger.info("Computing scores...")
    threshold = config["score"].get("high_pressure_threshold", 2.0)
    kospi_scored  = compute_score(kospi_merged)
    kosdaq_scored = compute_score(kosdaq_merged)

    # ── 6. Serialise ─────────────────────────────────────────────────────────
    def df_to_records(df: pd.DataFrame) -> list[dict]:
        cols = [
            "rank", "mksc_shrn_iscd", "display_name", "hts_kor_isnm",
            "ssts_vol_rlim", "ssts_vol_rlim_5d",
            "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn",
            "prdy_ctrt",
            "acml_tr_pbmn",
            "z_short_today", "z_short_5d", "z_foreign", "z_inst", "z_return",
            "score", "incomplete",
        ]
        out = []
        for _, row in df.iterrows():
            rec: dict = {}
            for c in cols:
                val = row.get(c)
                if pd.isna(val) if not isinstance(val, bool) else False:
                    rec[c] = None
                elif isinstance(val, float):
                    rec[c] = round(val, 4)
                else:
                    rec[c] = val
            out.append(rec)
        return out

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    generated_at = datetime.now(timezone(timezone.utc.utcoffset(None))).isoformat()

    meta = build_meta_summary(kospi_scored, kosdaq_scored, threshold)
    payload = {
        "date": today_str,
        "generated_at": generated_at,
        "kospi": df_to_records(kospi_scored),
        "kosdaq": df_to_records(kosdaq_scored),
        "meta": meta,
    }

    # ── 7. Write ──────────────────────────────────────────────────────────────
    if not dry_run:
        out_dir = Path(config["output"]["json_dir"])
        out_dir.mkdir(parents=True, exist_ok=True)
        date_file = out_dir / f"{today_str.replace('-', '')}.json"
        latest_file = out_dir / "latest.json"
        blob = json.dumps(payload, ensure_ascii=False, indent=2)
        date_file.write_text(blob, encoding="utf-8")
        latest_file.write_text(blob, encoding="utf-8")
        logger.info("Output written → %s", date_file)
    else:
        logger.info("[dry-run] skipping file write")

    logger.info(
        "Done. KOSPI=%d KOSDAQ=%d high_pressure=%d top=%s (%.2f)",
        meta["kospi_count"], meta["kosdaq_count"],
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
