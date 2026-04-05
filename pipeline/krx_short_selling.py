"""
pipeline/krx_short_selling.py
Direct KRX short-selling fetch helpers.

This module talks to KRX's public JSON endpoint directly instead of going
through pykrx, so it is closer to the actual KRX web API surface.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pandas as pd


class KRXShortSellingError(RuntimeError):
    """Raised when a KRX short-selling request fails."""


_KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
_KRX_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
}


def _market_name(market: str) -> str:
    market = market.upper()
    if market not in {"KOSPI", "KOSDAQ", "KONEX"}:
        raise ValueError(f"Unsupported KRX market: {market}")
    return market


def _market_to_idx(market: str) -> int:
    return {"KOSPI": 1, "KOSDAQ": 2, "KONEX": 3}[_market_name(market)]


def _post_json(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        resp = httpx.post(_KRX_URL, headers=_KRX_HEADERS, data=payload, timeout=30)
        if resp.status_code >= 400:
            raise KRXShortSellingError(f"HTTP {resp.status_code} from KRX")
        return resp.json()
    except Exception as exc:  # pragma: no cover - network dependent
        raise KRXShortSellingError(str(exc)) from exc


def _extract_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("OutBlock_1", "outBlock_1", "output", "Output", "block1"):
        rows = data.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _normalize_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in columns:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _reset_and_normalize_index(df: pd.DataFrame, index_name: str = "date") -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if out.index.name or isinstance(out.index, pd.MultiIndex):
        out = out.reset_index()
    else:
        out = out.reset_index().rename(columns={"index": index_name})

    if "날짜" in out.columns:
        out = out.rename(columns={"날짜": index_name})
    if "티커" in out.columns:
        out = out.rename(columns={"티커": "ticker"})
    return out


def fetch_short_selling_volume_top50(date: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    Return the KRX top-50 short-selling transaction table for one market.
    """
    payload = {
        "bld": "dbms/MDC/STAT/srt/MDCSTAT30401",
        "trdDd": date,
        "mktTpCd": _market_to_idx(market),
    }
    data = _post_json(payload)
    rows = _extract_rows(data)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if {"RANK", "ISU_CD", "CVSRTSELL_TRDVAL", "ACC_TRDVAL", "TDD_SRTSELL_WT", "TDD_SRTSELL_TRDVAL_INCDEC_RT", "VALU_PD_AVG_SRTSELL_WT", "VALU_PD_CMP_TDD_SRTSELL_RTO", "PRC_YD"}.issubset(df.columns):
        df = df[
            [
                "RANK",
                "ISU_CD",
                "CVSRTSELL_TRDVAL",
                "ACC_TRDVAL",
                "TDD_SRTSELL_WT",
                "TDD_SRTSELL_TRDVAL_INCDEC_RT",
                "VALU_PD_AVG_SRTSELL_WT",
                "VALU_PD_CMP_TDD_SRTSELL_RTO",
                "PRC_YD",
            ]
        ]

    df = df.rename(
        columns={
            "RANK": "rank",
            "ISU_CD": "ticker",
            "ISU_ABBRV": "display_name",
            "CVSRTSELL_TRDVAL": "short_volume",
            "ACC_TRDVAL": "total_volume",
            "TDD_SRTSELL_WT": "short_ratio",
            "TDD_SRTSELL_TRDVAL_INCDEC_RT": "short_volume_change",
            "VALU_PD_AVG_SRTSELL_WT": "short_40d_avg_ratio",
            "VALU_PD_CMP_TDD_SRTSELL_RTO": "short_ratio_change",
            "PRC_YD": "return_pct",
        }
    )
    df = _normalize_numeric(
        df,
        [
            "rank",
            "short_volume",
            "total_volume",
            "short_ratio",
            "short_volume_change",
            "short_40d_avg_ratio",
            "short_ratio_change",
            "return_pct",
        ],
    )
    return df.set_index("ticker")


def fetch_short_selling_balance_top50(date: str, market: str = "KOSPI") -> pd.DataFrame:
    """
    Return the KRX top-50 short position table for one market.
    """
    payload = {
        "bld": "dbms/MDC/STAT/srt/MDCSTAT30801",
        "trdDd": date,
        "mktTpCd": _market_to_idx(market),
    }
    data = _post_json(payload)
    rows = _extract_rows(data)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if {"RANK", "ISU_CD", "BAL_QTY", "LIST_SHRS", "BAL_AMT", "MKTCAP", "BAL_RTO"}.issubset(df.columns):
        df = df[["RANK", "ISU_CD", "BAL_QTY", "LIST_SHRS", "BAL_AMT", "MKTCAP", "BAL_RTO"]]

    df = df.rename(
        columns={
            "RANK": "rank",
            "ISU_CD": "ticker",
            "ISU_ABBRV": "display_name",
            "BAL_QTY": "short_balance",
            "LIST_SHRS": "listed_shares",
            "BAL_AMT": "short_balance_amount",
            "MKTCAP": "market_cap",
            "BAL_RTO": "short_balance_ratio",
        }
    )
    df = _normalize_numeric(
        df,
        [
            "rank",
            "short_balance",
            "listed_shares",
            "short_balance_amount",
            "market_cap",
            "short_balance_ratio",
        ],
    )
    return df.set_index("ticker")


def fetch_short_selling_volume_history(
    fromdate: str,
    todate: str,
    ticker: str,
    market: str = "KOSPI",
) -> pd.DataFrame:
    """
    Return KRX short-selling transaction history for one ticker.

    KRX's public short-selling page is driven by the same JSON endpoint family,
    but the per-ticker history flow is not as stable as the market top-50 pages
    across installations. We keep this helper available but clearly scoped to
    the public page model.
    """
    payload = {
        "bld": "dbms/MDC/STAT/srt/MDCSTAT30102",
        "strtDd": fromdate,
        "endDd": todate,
        "isuCd": ticker,
    }
    data = _post_json(payload)
    rows = _extract_rows(data)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(
        columns={
            "TRD_DD": "date",
            "CVSRTSELL_TRDVOL": "short_volume",
            "ACC_TRDVOL": "total_volume",
            "TRDVOL_WT": "short_ratio",
            "CVSRTSELL_TRDVAL": "short_value",
            "ACC_TRDVAL": "total_value",
            "TRDVAL_WT": "short_value_ratio",
        }
    )
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return _normalize_numeric(
        df,
        [
            "short_volume",
            "total_volume",
            "short_ratio",
            "short_value",
            "total_value",
            "short_value_ratio",
        ],
    )


def fetch_short_selling_balance_history(
    fromdate: str,
    todate: str,
    ticker: str,
) -> pd.DataFrame:
    """
    Return KRX short-selling balance history for one ticker.
    """
    payload = {
        "bld": "dbms/MDC/STAT/srt/MDCSTAT30502",
        "strtDd": fromdate,
        "endDd": todate,
        "isuCd": ticker,
    }
    data = _post_json(payload)
    rows = _extract_rows(data)
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.rename(
        columns={
            "RPT_DUTY_OCCR_DD": "date",
            "BAL_QTY": "short_balance",
            "LIST_SHRS": "listed_shares",
            "BAL_AMT": "short_balance_amount",
            "MKTCAP": "market_cap",
            "BAL_RTO": "short_balance_ratio",
        }
    )
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return _normalize_numeric(
        df,
        [
            "short_balance",
            "listed_shares",
            "short_balance_amount",
            "market_cap",
            "short_balance_ratio",
        ],
    )


_NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9",
}
_NAVER_URL = "https://finance.naver.com/sise/sise_market_sum.naver"

_ETF_BRANDS = {
    "KODEX", "TIGER", "KBSTAR", "ARIRANG", "HANARO", "KOSEF",
    "SOL", "ACE", "RISE", "PLUS", "TIMEFOLIO", "SMART", "FOCUS", "TREX",
}


def _is_excluded(name: str) -> bool:
    """Return True for ETFs (brand name), ETNs, and TR total-return fund products."""
    if name.split()[0].upper() in _ETF_BRANDS:
        return True
    if "ETN" in name or "TR" in name:
        return True
    return False


def _is_preferred(ticker: str) -> bool:
    """Return True for preferred shares per official KRX ticker convention."""
    return ticker[-1] in {"5", "7", "9", "K"}


def _scrape_naver_page(sosok: int, page: int, market_label: str) -> list[dict]:
    """Scrape one page of Naver Finance market cap ranking."""
    import time as _time
    resp = httpx.get(
        _NAVER_URL,
        headers=_NAVER_HEADERS,
        params={"sosok": sosok, "page": page},
        timeout=20,
        follow_redirects=True,
    )
    resp.raise_for_status()
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="type_2")
    rows: list[dict] = []
    if not table:
        return rows
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        rank_txt = tds[0].get_text(strip=True)
        if not rank_txt.isdigit():
            continue
        a_tag = tds[1].find("a")
        if not a_tag:
            continue
        name = a_tag.get_text(strip=True)
        href = a_tag.get("href", "")
        ticker = href.split("code=")[-1] if "code=" in href else ""
        mktcap_raw = tds[6].get_text(strip=True).replace(",", "")
        rows.append({
            "ticker":          ticker,
            "name_ko":         name,
            "market":          market_label,
            "market_cap_100m": int(mktcap_raw) if mktcap_raw.isdigit() else 0,
        })
    return rows


def fetch_market_cap_universe(top_n: int = 300) -> list[dict]:
    """
    Return top_n common stocks by market cap (KOSPI + KOSDAQ combined).

    Scrapes Naver Finance live market cap rankings and filters out:
    - ETFs (brand-name first word), ETNs, TR total-return fund products
    - Preferred shares (KRX ticker convention: last char in {5,7,9,K})

    Returns list of dicts: [{ticker, name_ko, market, market_cap_100m}, ...]
    sorted by market cap descending.
    """
    import time as _time

    all_rows: list[dict] = []
    for sosok, label in [(0, "KOSPI"), (1, "KOSDAQ")]:
        for page in range(1, 11):
            rows = _scrape_naver_page(sosok, page, label)
            if not rows:
                break
            all_rows.extend(rows)
            _time.sleep(0.25)

    all_rows.sort(key=lambda r: r["market_cap_100m"], reverse=True)

    common: list[dict] = []
    for r in all_rows:
        if _is_excluded(r["name_ko"]):
            continue
        if _is_preferred(r["ticker"]):
            continue
        common.append(r)
        if len(common) == top_n:
            break

    return common


@dataclass(frozen=True)
class KRXShortSnapshot:
    ticker: str
    fromdate: str
    todate: str
    market: str = "KOSPI"

    def to_dict(self) -> dict[str, Any]:
        history = fetch_short_selling_volume_history(
            self.fromdate,
            self.todate,
            self.ticker,
            market=self.market,
        )
        balance = fetch_short_selling_balance_history(
            self.fromdate,
            self.todate,
            self.ticker,
        )
        return {
            "ticker": self.ticker,
            "market": self.market,
            "fromdate": self.fromdate,
            "todate": self.todate,
            "volume_history": history.to_dict(orient="records"),
            "balance_history": balance.to_dict(orient="records"),
        }
