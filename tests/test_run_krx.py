from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import pipeline.run_krx as run_krx


def _volume_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "005930",
                "display_name": "삼성전자",
                "rank": 1,
                "short_volume": 38510030,
                "total_volume": 915824030,
                "short_ratio": 4.21,
                "short_volume_change": 6.62,
                "short_40d_avg_ratio": 0.51,
                "short_ratio_change": 8.33,
                "return_pct": -1.25,
            },
            {
                "ticker": "000660",
                "display_name": "SK하이닉스",
                "rank": 2,
                "short_volume": 13265200,
                "total_volume": 329805000,
                "short_ratio": 4.02,
                "short_volume_change": 4.82,
                "short_40d_avg_ratio": 0.66,
                "short_ratio_change": 6.14,
                "return_pct": -2.46,
            },
        ]
    ).set_index("ticker")


def _balance_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "005930",
                "display_name": "삼성전자",
                "rank": 1,
                "short_balance": 4693027,
                "listed_shares": 69275662,
                "short_balance_amount": 74853780650,
                "market_cap": 1104946808900,
                "short_balance_ratio": 6.77,
            }
        ]
    ).set_index("ticker")


def _history_df(short_ratio: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"date": "2026-04-01", "short_ratio": short_ratio},
            {"date": "2026-03-31", "short_ratio": short_ratio + 1},
        ]
    )


def test_run_krx_pipeline_dry_run(monkeypatch):
    monkeypatch.setattr(run_krx, "_snapshot_date", lambda: "2026-04-01")
    monkeypatch.setattr(run_krx, "fetch_short_selling_volume_top50", lambda date, market: _volume_df())
    monkeypatch.setattr(run_krx, "fetch_short_selling_balance_top50", lambda date, market: _balance_df())

    def fake_history(start, end, ticker):
        return _history_df(4.21 if ticker == "005930" else 4.02)

    monkeypatch.setattr(run_krx, "fetch_short_selling_volume_history", fake_history)

    payload = run_krx.run_pipeline(dry_run=True)

    assert payload["source"] == "krx"
    assert payload["date"] == "2026-04-01"
    assert len(payload["kospi"]) == 2
    assert payload["kospi"][0]["mksc_shrn_iscd"] == "005930"
    assert payload["kospi"][0]["score"] is not None
    assert payload["kospi"][0]["display_name"] == "삼성전자"
    assert payload["meta"]["source"] == "KRX"
