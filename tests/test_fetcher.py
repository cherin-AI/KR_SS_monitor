"""
tests/test_fetcher.py
Unit tests for fetcher functions using httpx mock transport.
No real API calls are made.
"""
import json
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest
import pytest_asyncio

from pipeline.fetcher import (
    fetch_daily_short_trend,
    fetch_foreign_inst_aggregate,
    fetch_investor_trade_by_stock_daily,
    fetch_investor_trade_fallbacks,
    fetch_investor_trend_estimate,
    fetch_investor_trend_estimate_fallbacks,
    fetch_short_sale_ranking,
    fetch_stock_meta,
)

# ── fixtures ──────────────────────────────────────────────────────────────────

CONFIG = {
    "kis": {
        "app_key": "test_key",
        "app_secret": "test_secret",
        "env": "prod",
        "base_url_prod": "https://openapi.koreainvestment.com:9443",
        "base_url_paper": "https://openapivts.koreainvestment.com:29443",
    }
}
TOKEN = "test_token"

RANKING_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "output": [
        {
            "mksc_shrn_iscd": "005930",
            "hts_kor_isnm": "삼성전자",
            "stck_prpr": "82800",
            "prdy_ctrt": "-1.80",
            "acml_vol": "27000000",
            "ssts_cntg_qty": "39000",
            "ssts_vol_rlim": "14.20",
            "ssts_tr_pbmn": "3200000000",
            "ssts_tr_pbmn_rlim": "0.15",
            "avrg_prc": "82000",
        },
        {
            "mksc_shrn_iscd": "000660",
            "hts_kor_isnm": "SK하이닉스",
            "stck_prpr": "185900",
            "prdy_ctrt": "-2.30",
            "acml_vol": "3000000",
            "ssts_cntg_qty": "13000",
            "ssts_vol_rlim": "12.70",
            "ssts_tr_pbmn": "2400000000",
            "ssts_tr_pbmn_rlim": "0.45",
            "avrg_prc": "181000",
        },
    ],
}

TREND_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "output1": {"stck_prpr": "82800", "prdy_ctrt": "-1.80"},
    "output2": [
        {"stck_bsop_date": "20250328", "ssts_vol_rlim": "11.5"},
        {"stck_bsop_date": "20250327", "ssts_vol_rlim": "12.0"},
        {"stck_bsop_date": "20250326", "ssts_vol_rlim": "10.8"},
        {"stck_bsop_date": "20250325", "ssts_vol_rlim": "13.2"},
        {"stck_bsop_date": "20250324", "ssts_vol_rlim": "11.9"},
    ],
}

FI_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "Output": [
        {
            "mksc_shrn_iscd": "005930",
            "frgn_ntby_tr_pbmn": "-42100",
            "orgn_ntby_tr_pbmn": "-18300",
            "frgn_ntby_qty": "-5000",
            "orgn_ntby_qty": "-2000",
            "stck_prpr": "82800",
            "prdy_ctrt": "-1.80",
        },
    ],
}

INVESTOR_DAILY_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "output1": {
        "stck_prpr": "71000",
        "prdy_vrss": "-800",
        "prdy_vrss_sign": "5",
        "prdy_ctrt": "-1.11",
        "acml_vol": "11354253",
        "prdy_vol": "11354253",
        "rprs_mrkt_kor_name": "KOSPI200",
    },
    "output2": [
        {
            "stck_bsop_date": "20250328",
            "frgn_ntby_tr_pbmn": "-144363",
            "orgn_ntby_tr_pbmn": "-40903",
        },
        {
            "stck_bsop_date": "20250327",
            "frgn_ntby_tr_pbmn": "-120000",
            "orgn_ntby_tr_pbmn": "25000",
        },
    ],
}

ESTIMATE_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "output2": [
        {
            "bsop_hour_gb": "5",
            "frgn_fake_ntby_qty": "-00000000000030000",
            "orgn_fake_ntby_qty": "000000000000121000",
            "sum_fake_ntby_qty": "000000000000091000",
        }
    ],
}

STOCK_META_RESPONSE = {
    "rt_cd": "0",
    "msg_cd": "MCA00000",
    "msg1": "정상처리 되었습니다.",
    "output": {
        "prdt_name": "에이치엘비",
        "prdt_abrv_name": "에이치엘비",
        "prdt_eng_name": "HLB",
        "prdt_eng_abrv_name": "HLB",
        "bstp_kor_isnm": "제약",
    },
}


def _mock_client(response_body: dict) -> httpx.AsyncClient:
    """Return an AsyncClient whose GET always returns the given JSON body."""
    transport = httpx.MockTransport(
        lambda req: httpx.Response(200, json=response_body)
    )
    return httpx.AsyncClient(transport=transport)


# ── tests ─────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_short_sale_ranking_returns_dataframe():
    async with _mock_client(RANKING_RESPONSE) as client:
        df = await fetch_short_sale_ranking(client, TOKEN, CONFIG, "J")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert "mksc_shrn_iscd" in df.columns
    assert "ssts_vol_rlim" in df.columns


@pytest.mark.asyncio
async def test_fetch_short_sale_ranking_numeric_cast():
    async with _mock_client(RANKING_RESPONSE) as client:
        df = await fetch_short_sale_ranking(client, TOKEN, CONFIG, "J")
    assert df["ssts_vol_rlim"].dtype == float
    assert df["prdy_ctrt"].dtype == float


@pytest.mark.asyncio
async def test_fetch_short_sale_ranking_market_label():
    async with _mock_client(RANKING_RESPONSE) as client:
        df = await fetch_short_sale_ranking(client, TOKEN, CONFIG, "NX")
    assert (df["market"] == "KOSDAQ").all()


@pytest.mark.asyncio
async def test_fetch_daily_short_trend_average():
    async with _mock_client(TREND_RESPONSE) as client:
        avg = await fetch_daily_short_trend(client, TOKEN, CONFIG, "005930", lookback_days=5)
    expected = (11.5 + 12.0 + 10.8 + 13.2 + 11.9) / 5
    assert avg is not None
    assert abs(avg - round(expected, 4)) < 1e-3


@pytest.mark.asyncio
async def test_fetch_daily_short_trend_empty():
    empty_resp = {"rt_cd": "0", "msg_cd": "MCA00000", "msg1": "ok", "output2": []}
    async with _mock_client(empty_resp) as client:
        avg = await fetch_daily_short_trend(client, TOKEN, CONFIG, "000001", lookback_days=5)
    assert avg is None


@pytest.mark.asyncio
async def test_fetch_foreign_inst_aggregate_returns_dataframe():
    async with _mock_client(FI_RESPONSE) as client:
        df = await fetch_foreign_inst_aggregate(client, TOKEN, CONFIG, "J")
    assert isinstance(df, pd.DataFrame)
    assert "frgn_ntby_tr_pbmn" in df.columns
    assert df["frgn_ntby_tr_pbmn"].iloc[0] == -42100.0


@pytest.mark.asyncio
async def test_fetch_investor_trade_by_stock_daily_returns_fallback_values():
    async with _mock_client(INVESTOR_DAILY_RESPONSE) as client:
        values = await fetch_investor_trade_by_stock_daily(client, TOKEN, CONFIG, "J", "005930")
    assert values["frgn_ntby_tr_pbmn"] == -144363.0
    assert values["orgn_ntby_tr_pbmn"] == -40903.0


@pytest.mark.asyncio
async def test_fetch_investor_trade_fallbacks_returns_map():
    async with _mock_client(INVESTOR_DAILY_RESPONSE) as client:
        result = await fetch_investor_trade_fallbacks(client, TOKEN, CONFIG, "J", ["005930"])
    assert result["005930"]["frgn_ntby_tr_pbmn"] == -144363.0
    assert result["005930"]["orgn_ntby_tr_pbmn"] == -40903.0


@pytest.mark.asyncio
async def test_fetch_investor_trend_estimate_returns_quantities():
    async with _mock_client(ESTIMATE_RESPONSE) as client:
        values = await fetch_investor_trend_estimate(client, TOKEN, CONFIG, "005930")
    assert values["frgn_fake_ntby_qty"] == -30000.0
    assert values["orgn_fake_ntby_qty"] == 121000.0


@pytest.mark.asyncio
async def test_fetch_investor_trend_estimate_fallbacks_returns_map():
    async with _mock_client(ESTIMATE_RESPONSE) as client:
        result = await fetch_investor_trend_estimate_fallbacks(client, TOKEN, CONFIG, ["005930"])
    assert result["005930"]["sum_fake_ntby_qty"] == 91000.0


@pytest.mark.asyncio
async def test_fetch_stock_meta_prefers_english_name_fields():
    async with _mock_client(STOCK_META_RESPONSE) as client:
        meta = await fetch_stock_meta(client, TOKEN, CONFIG, "028300")
    assert meta["display_name"] == "HLB"
    assert meta["name_ko"] == "에이치엘비"


@pytest.mark.asyncio
async def test_api_error_raises():
    error_resp = {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "접근토큰 발급 오류"}
    from pipeline.auth import KISAPIError
    async with _mock_client(error_resp) as client:
        with pytest.raises(KISAPIError):
            await fetch_short_sale_ranking(client, TOKEN, CONFIG, "J")
