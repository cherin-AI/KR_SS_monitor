from __future__ import annotations

import pytest

import pipeline.krx_short_selling as krx


TOP50_VOLUME_RESPONSE = {
    "OutBlock_1": [
        {
            "RANK": "1",
            "ISU_CD": "005930",
            "CVSRTSELL_TRDVAL": "38510030",
            "ACC_TRDVAL": "915824030",
            "TDD_SRTSELL_WT": "4.21",
            "TDD_SRTSELL_TRDVAL_INCDEC_RT": "6.62",
            "VALU_PD_AVG_SRTSELL_WT": "0.51",
            "VALU_PD_CMP_TDD_SRTSELL_RTO": "8.33",
            "PRC_YD": "-1.25",
        }
    ]
}

TOP50_BALANCE_RESPONSE = {
    "OutBlock_1": [
        {
            "RANK": "1",
            "ISU_CD": "005930",
            "BAL_QTY": "4693027",
            "LIST_SHRS": "69275662",
            "BAL_AMT": "74853780650",
            "MKTCAP": "1104946808900",
            "BAL_RTO": "6.77",
        }
    ]
}

HISTORY_RESPONSE = {
    "OutBlock_1": [
        {
            "TRD_DD": "2021/01/26",
            "CVSRTSELL_TRDVOL": "0",
            "ACC_TRDVOL": "1665933",
            "TRDVOL_WT": "0.0",
            "CVSRTSELL_TRDVAL": "0",
            "ACC_TRDVAL": "3927562180",
            "TRDVAL_WT": "0.0",
        },
        {
            "TRD_DD": "2021/01/25",
            "CVSRTSELL_TRDVOL": "0",
            "ACC_TRDVOL": "328833",
            "TRDVOL_WT": "0.0",
            "CVSRTSELL_TRDVAL": "0",
            "ACC_TRDVAL": "748091530",
            "TRDVAL_WT": "0.0",
        },
    ]
}

BALANCE_HISTORY_RESPONSE = {
    "OutBlock_1": [
        {
            "RPT_DUTY_OCCR_DD": "2020/01/10",
            "BAL_QTY": "5489240",
            "LIST_SHRS": "5969782550",
            "BAL_AMT": "326609780000",
            "MKTCAP": "355202061725000",
            "BAL_RTO": "0.090027",
        }
    ]
}


def _fake_post(response_map):
    def fake_post(url, headers=None, data=None, timeout=None):
        payload = data or {}
        key = (
            payload.get("bld"),
            payload.get("trdDd") or payload.get("strtDd"),
            payload.get("mktTpCd") or payload.get("isuCd"),
        )
        body = response_map.get(key)
        if body is None:
            raise AssertionError(f"unexpected payload {payload}")
        return __import__("httpx").Response(200, json=body)

    return fake_post


def test_fetch_short_selling_volume_top50(monkeypatch):
    monkeypatch.setattr(
        krx.httpx,
        "post",
        _fake_post({("dbms/MDC/STAT/srt/MDCSTAT30401", "20250402", 1): TOP50_VOLUME_RESPONSE}),
    )
    df = krx.fetch_short_selling_volume_top50("20250402", market="KOSPI")
    assert list(df.index) == ["005930"]
    assert df.loc["005930", "rank"] == 1
    assert df.loc["005930", "short_volume"] == 38510030
    assert df.loc["005930", "short_ratio"] == pytest.approx(4.21)


def test_fetch_short_selling_balance_top50(monkeypatch):
    monkeypatch.setattr(
        krx.httpx,
        "post",
        _fake_post({("dbms/MDC/STAT/srt/MDCSTAT30801", "20250402", 1): TOP50_BALANCE_RESPONSE}),
    )
    df = krx.fetch_short_selling_balance_top50("20250402", market="KOSPI")
    assert list(df.index) == ["005930"]
    assert df.loc["005930", "rank"] == 1
    assert df.loc["005930", "short_balance"] == 4693027
    assert df.loc["005930", "short_balance_ratio"] == pytest.approx(6.77)


def test_fetch_short_selling_volume_history(monkeypatch):
    monkeypatch.setattr(
        krx.httpx,
        "post",
        _fake_post({("dbms/MDC/STAT/srt/MDCSTAT30102", "20201226", "005930"): HISTORY_RESPONSE}),
    )
    df = krx.fetch_short_selling_volume_history("20201226", "20210126", "005930")
    assert list(df["date"].dt.strftime("%Y-%m-%d")) == ["2021-01-26", "2021-01-25"]
    assert df.loc[0, "short_volume"] == 0
    assert df.loc[0, "total_volume"] == 1665933


def test_fetch_short_selling_balance_history(monkeypatch):
    monkeypatch.setattr(
        krx.httpx,
        "post",
        _fake_post({("dbms/MDC/STAT/srt/MDCSTAT30502", "20200106", "005930"): BALANCE_HISTORY_RESPONSE}),
    )
    df = krx.fetch_short_selling_balance_history("20200106", "20200110", "005930")
    assert df.loc[0, "short_balance"] == 5489240
    assert df.loc[0, "market_cap"] == 355202061725000


def test_bad_market_raises():
    with pytest.raises(ValueError):
        krx.fetch_short_selling_volume_top50("20250402", market="BAD")
