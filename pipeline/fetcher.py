"""
pipeline/fetcher.py
All KIS API fetch functions for the short pressure pipeline.
Each function logs its call and raises KISAPIError on non-zero rt_cd.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Any

import httpx
import pandas as pd

from pipeline.auth import KISAPIError, build_headers

logger = logging.getLogger(__name__)

_RETRYABLE = (httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadTimeout)


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict,
    params: dict,
    retries: int = 3,
    backoff: float = 2.0,
) -> httpx.Response:
    """GET with exponential backoff on transient KIS connection drops."""
    for attempt in range(retries):
        try:
            return await client.get(url, headers=headers, params=params)
        except _RETRYABLE as exc:
            if attempt == retries - 1:
                raise
            wait = backoff ** attempt
            logger.warning("Transient error (%s), retrying in %.1fs…", exc, wait)
            await asyncio.sleep(wait)
    raise RuntimeError("unreachable")


# ── helpers ───────────────────────────────────────────────────────────────────

def _base_url(config: dict) -> str:
    env = config["kis"]["env"]
    return config["kis"][f"base_url_{env}"]


def _check(data: dict, label: str) -> None:
    if data.get("rt_cd") != "0":
        raise KISAPIError(data.get("msg_cd", ""), data.get("msg1", label))


def _ndays_ago(n: int) -> str:
    return (date.today() - timedelta(days=n)).strftime("%Y%m%d")


def _yesterday() -> str:
    return (date.today() - timedelta(days=1)).strftime("%Y%m%d")


# ── A. Short-sale ranking ─────────────────────────────────────────────────────

async def fetch_short_sale_ranking(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    market: str,          # "J" = KOSPI, "NX" = KOSDAQ
) -> pd.DataFrame:
    """
    Fetch short-sale ranking (top 30) for a given market.
    Returns DataFrame with columns from the API output array.
    """
    market_label = "KOSPI" if market == "J" else "KOSDAQ"
    tr_id = "FHPST04820000"
    url = _base_url(config) + "/uapi/domestic-stock/v1/ranking/short-sale"
    params = {
        "FID_APLY_RANG_VOL": "",
        "FID_COND_MRKT_DIV_CODE": "J",           # always "J" for this endpoint
        "FID_COND_SCR_DIV_CODE": "20482",
        "FID_INPUT_ISCD": "0001" if market == "J" else "1001",
        "FID_PERIOD_DIV_CODE": "D",
        "FID_INPUT_CNT_1": "0",
        "FID_TRGT_EXLS_CLS_CODE": "",
        "FID_TRGT_CLS_CODE": "",
        "FID_APLY_RANG_PRC_1": "",
        "FID_APLY_RANG_PRC_2": "",
    }
    resp = await client.get(url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"short_sale_ranking {market_label}")

    rows = data.get("output", [])
    logger.info("[FETCH] %s %s → %d rows", tr_id, market_label, len(rows))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    numeric = [
        "stck_prpr", "prdy_ctrt", "acml_vol",
        "ssts_cntg_qty", "ssts_vol_rlim",
        "ssts_tr_pbmn", "ssts_tr_pbmn_rlim", "avrg_prc",
    ]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["market"] = market_label
    return df


# ── B. Daily short-sale trend (per ticker) ────────────────────────────────────

async def fetch_daily_short_trend(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    ticker: str,
    lookback_days: int = 5,
    trade_date_end: str | None = None,
) -> float | None:
    """
    Fetch the last `lookback_days` trading days of ssts_vol_rlim for a ticker.
    Returns the simple average, or None if data is unavailable.
    """
    tr_id = "FHPST04830000"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/daily-short-sale"
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": _ndays_ago(lookback_days * 2),  # buffer for non-trading days
        "FID_INPUT_DATE_2": trade_date_end or date.today().strftime("%Y%m%d"),
    }
    resp = await _get_with_retry(
        client, url, headers=build_headers(token, tr_id, config), params=params
    )
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"daily_short_trend {ticker}")

    rows = data.get("output2", [])
    logger.info("[FETCH] %s %s → %d days", tr_id, ticker, len(rows))

    if not rows:
        return None

    ratios = []
    for row in rows[:lookback_days]:
        try:
            ratios.append(float(row["ssts_vol_rlim"]))
        except (KeyError, ValueError, TypeError):
            continue

    return round(sum(ratios) / len(ratios), 4) if ratios else None


async def fetch_daily_short_snapshot(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    ticker: str,
    lookback_days: int = 5,
    trade_date_end: str | None = None,
    market: str = "KOSPI",
) -> dict[str, float | str | None]:
    """
    Fetch the latest daily short-sale row plus a 5-day average for one ticker.

    This is the preferred validation path when the universe is already known:
    we avoid the ranking endpoint entirely and pull the per-stock short-sale
    history directly.
    """
    tr_id = "FHPST04830000"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/daily-short-sale"
    mrkt_code = "Q" if market == "KOSDAQ" else "J"
    params = {
        "FID_COND_MRKT_DIV_CODE": mrkt_code,
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": _ndays_ago(lookback_days * 2),  # buffer for non-trading days
        "FID_INPUT_DATE_2": trade_date_end or date.today().strftime("%Y%m%d"),
    }
    resp = await _get_with_retry(client, url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"daily_short_snapshot {ticker}")

    rows = data.get("output2", [])
    logger.info("[FETCH] %s %s → %d days", tr_id, ticker, len(rows))
    if not rows:
        return {}

    latest = rows[0]
    ratios: list[float] = []
    for row in rows[:lookback_days]:
        try:
            ratios.append(float(row["ssts_vol_rlim"]))
        except (KeyError, ValueError, TypeError):
            continue

    out: dict[str, float | str | None] = {
        "short_today_ratio": None,
        "short_5d_avg": round(sum(ratios) / len(ratios), 4) if ratios else None,
        "short_today_numerator": None,
        "short_today_trade_value": None,
        "short_today_trade_ratio": None,
        "short_today_price": None,
        "short_trade_date": latest.get("stck_bsop_date"),
        "source": tr_id,
    }

    for key, target in [
        ("ssts_vol_rlim", "short_today_ratio"),
        ("ssts_cntg_qty", "short_today_numerator"),
        ("ssts_tr_pbmn", "short_today_trade_value"),
        ("ssts_tr_pbmn_rlim", "short_today_trade_ratio"),
        ("stck_prpr", "short_today_price"),
    ]:
        try:
            value = float(latest[key])
        except (KeyError, TypeError, ValueError):
            value = None
        out[target] = value

    return out


# ── C. Foreign / institution aggregate ───────────────────────────────────────

async def fetch_foreign_inst_aggregate(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    market: str,          # "J" = KOSPI, "NX" = KOSDAQ
) -> pd.DataFrame:
    """
    Fetch foreign + institution net buy/sell aggregate for all stocks in a market.
    Returns DataFrame keyed on mksc_shrn_iscd.
    """
    market_label = "KOSPI" if market == "J" else "KOSDAQ"
    iscd = "0001" if market == "J" else "1001"
    tr_id = "FHPTJ04400000"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/foreign-institution-total"
    params = {
        "FID_COND_MRKT_DIV_CODE": "V",
        "FID_COND_SCR_DIV_CODE": "16449",
        "FID_INPUT_ISCD": iscd,
        "FID_DIV_CLS_CODE": "1",          # sort by value
        "FID_RANK_SORT_CLS_CODE": "1",    # net sell top
        "FID_ETC_CLS_CODE": "0",          # all investor types
    }
    resp = await client.get(url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"foreign_inst_aggregate {market_label}")

    rows = data.get("Output", data.get("output", []))
    logger.info("[FETCH] %s %s → %d rows", tr_id, market_label, len(rows))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    numeric = [
        "frgn_ntby_qty", "orgn_ntby_qty",
        "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn",
        "stck_prpr", "prdy_ctrt",
    ]
    for col in numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["mksc_shrn_iscd"] + [c for c in numeric if c in df.columns]]


async def fetch_investor_trade_by_stock_daily(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    market: str,          # "J" = KOSPI, "NX" = KOSDAQ
    ticker: str,
    trade_date: str | None = None,
) -> dict[str, float | None]:
    """
    Fetch per-ticker investor net buy/sell values.

    This is the documented KIS fallback for rows missing from the market-wide
    foreign/institution aggregate response.
    """
    market_label = "KOSPI" if market == "J" else "KOSDAQ"
    tr_id = "FHPTJ04160001"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
    params = {
        "FID_COND_MRKT_DIV_CODE": market,
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": trade_date or date.today().strftime("%Y%m%d"),
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "1",
    }
    resp = await client.get(url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"investor_trade_by_stock_daily {market_label} {ticker}")

    rows = data.get("output2", [])
    logger.info("[FETCH] %s %s %s → %d rows", tr_id, market_label, ticker, len(rows))
    if not rows:
        return {}

    row = rows[0]
    if trade_date:
        for candidate in rows:
            if candidate.get("stck_bsop_date") == trade_date:
                row = candidate
                break

    out: dict[str, float | None] = {}
    for key in ("frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"):
        try:
            out[key] = float(row[key])
        except (KeyError, TypeError, ValueError):
            out[key] = None
    return out


async def fetch_investor_trade_fallbacks(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    market: str,
    tickers: list[str],
    trade_date: str | None = None,
    sleep_between: float = 0.1,
) -> dict[str, dict[str, float | None]]:
    """
    Fetch per-ticker fallback values for only the tickers that are missing from
    the aggregate response.
    """
    results: dict[str, dict[str, float | None]] = {}
    for ticker in tickers:
        try:
            values = await fetch_investor_trade_by_stock_daily(
                client,
                token,
                config,
                market,
                ticker,
                trade_date=trade_date,
            )
            if values:
                results[ticker] = values
        except Exception as exc:  # pragma: no cover - logged fallback path
            logger.warning("Investor trade fallback failed for %s %s: %s", market, ticker, exc)
        await asyncio.sleep(sleep_between)
    return results


async def fetch_investor_trend_estimate(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    ticker: str,
) -> dict[str, float | None]:
    """
    Fetch estimated foreign/institution net quantities for a ticker.

    The official KIS endpoint returns net quantities rather than KRW value, so
    callers should convert the quantities using a current price source.
    """
    tr_id = "HHPTJ04160200"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/investor-trend-estimate"
    params = {"MKSC_SHRN_ISCD": ticker}
    resp = await client.get(url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"investor_trend_estimate {ticker}")

    rows = data.get("output2", [])
    logger.info("[FETCH] %s %s → %d rows", tr_id, ticker, len(rows))
    if not rows:
        return {}

    row = rows[0]
    out: dict[str, float | None] = {}
    for key in ("frgn_fake_ntby_qty", "orgn_fake_ntby_qty", "sum_fake_ntby_qty"):
        try:
            out[key] = float(row[key])
        except (KeyError, TypeError, ValueError):
            out[key] = None
    return out


async def fetch_investor_trend_estimate_fallbacks(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    tickers: list[str],
    sleep_between: float = 0.1,
) -> dict[str, dict[str, float | None]]:
    """
    Fetch estimated quantity fallbacks for tickers whose daily money values are
    still zero after the main per-ticker investor trade call.
    """
    results: dict[str, dict[str, float | None]] = {}
    for ticker in tickers:
        try:
            values = await fetch_investor_trend_estimate(client, token, config, ticker)
            if values:
                results[ticker] = values
        except Exception as exc:  # pragma: no cover - logged fallback path
            logger.warning("Investor estimate fallback failed for %s: %s", ticker, exc)
        await asyncio.sleep(sleep_between)
    return results


async def fetch_inquire_investor_bulk(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    tickers: list[str],
    concurrency: int = 5,
    sleep_between: float = 0.3,
) -> dict[str, dict[str, float | None]]:
    """
    Fetch frgn_ntby_tr_pbmn + orgn_ntby_tr_pbmn for all tickers using
    FHKST01010900 (inquire-investor) as the primary source.
    Runs with a semaphore (concurrency=5) matching the short-snapshot pattern.
    """
    tr_id = "FHKST01010900"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/inquire-investor"
    results: dict[str, dict[str, float | None]] = {}
    sem = asyncio.Semaphore(concurrency)

    async def _one(ticker: str) -> None:
        async with sem:
            try:
                params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker}
                resp = await _get_with_retry(client, url, headers=build_headers(token, tr_id, config), params=params)
                resp.raise_for_status()
                data = resp.json()
                _check(data, f"inquire_investor {ticker}")
                output = data.get("output", [])
                row = output[0] if isinstance(output, list) and output else output
                out: dict[str, float | None] = {}
                for key in ("frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn"):
                    try:
                        out[key] = float(row[key])
                    except (KeyError, TypeError, ValueError):
                        out[key] = None
                logger.info("[FETCH] %s %s → frgn=%s orgn=%s", tr_id, ticker, out.get("frgn_ntby_tr_pbmn"), out.get("orgn_ntby_tr_pbmn"))
                results[ticker] = out
            except Exception as exc:
                logger.warning("inquire_investor failed for %s: %s", ticker, exc)
            await asyncio.sleep(sleep_between)

    await asyncio.gather(*(_one(t) for t in tickers))
    return results


# ── D. Stock metadata (cached) ────────────────────────────────────────────────

async def fetch_stock_meta(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    ticker: str,
) -> dict[str, Any]:
    """Fetch basic stock info (sector, name). Results should be cached externally."""
    tr_id = "CTPF1002R"
    url = _base_url(config) + "/uapi/domestic-stock/v1/quotations/search-stock-info"
    params = {"PRDT_TYPE_CD": "300", "PDNO": ticker}
    resp = await client.get(url, headers=build_headers(token, tr_id, config), params=params)
    resp.raise_for_status()
    data = resp.json()
    _check(data, f"stock_meta {ticker}")
    out = data.get("output", {})
    logger.info("[FETCH] %s %s → ok", tr_id, ticker)
    display_name = (
        out.get("prdt_eng_abrv_name")
        or out.get("prdt_eng_name")
        or out.get("prdt_abrv_name")
        or out.get("prdt_name")
        or ticker
    )
    return {
        "ticker": ticker,
        "name": display_name,
        "display_name": display_name,
        "name_ko": out.get("prdt_abrv_name", "") or out.get("prdt_name", ""),
        "sector": out.get("bstp_kor_isnm", ""),
    }


# ── E. Bulk short snapshot for entire universe ────────────────────────────────

async def fetch_short_snapshots_bulk(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    universe: list[dict],
    lookback_days: int = 5,
    concurrency: int = 10,
    sleep_between: float = 0.1,
    trade_date_end: str | None = None,
) -> dict[str, dict]:
    """
    Fetch short-sale snapshot for every ticker in universe.

    universe is a list of dicts with at least a "ticker" key (as returned by
    fetch_market_cap_universe).

    Returns {ticker: snapshot_dict} where snapshot_dict has keys:
        short_today_ratio, short_5d_avg, short_trade_date, short_today_price, ...
    """
    sem = asyncio.Semaphore(concurrency)
    results: dict[str, dict] = {}

    async def _one(entry: dict) -> None:
        ticker = entry["ticker"]
        market = entry.get("market", "KOSPI")
        async with sem:
            try:
                snap = await fetch_daily_short_snapshot(
                    client, token, config, ticker, lookback_days, trade_date_end, market
                )
                results[ticker] = snap
            except Exception as exc:
                logger.warning("Short snapshot failed %s: %s", ticker, exc)
            await asyncio.sleep(sleep_between)

    await asyncio.gather(*(_one(e) for e in universe))
    return results


# ── F. Orchestrated 5-day fetch for entire universe (legacy) ──────────────────

async def fetch_5d_averages(
    client: httpx.AsyncClient,
    token: str,
    config: dict,
    tickers: list[str],
    lookback_days: int = 5,
    sleep_between: float = 0.5,
    trade_date_end: str | None = None,
) -> dict[str, float | None]:
    """
    Fetch 5-day short ratio average for all tickers.
    Returns dict: {ticker: avg_ssts_vol_rlim or None}
    """
    results: dict[str, float | None] = {}
    for ticker in tickers:
        avg = await fetch_daily_short_trend(
            client,
            token,
            config,
            ticker,
            lookback_days,
            trade_date_end=trade_date_end,
        )
        results[ticker] = avg
        await asyncio.sleep(sleep_between)
    return results
