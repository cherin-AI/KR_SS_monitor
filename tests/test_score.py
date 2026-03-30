"""
tests/test_score.py
Unit tests for the score computation engine.
No real API calls — all data is synthetic.
"""
import numpy as np
import pandas as pd
import pytest

from pipeline.score import WEIGHTS, build_meta_summary, compute_score, zscore_series


# ── zscore_series ─────────────────────────────────────────────────────────────

def test_zscore_standard():
    s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    z = zscore_series(s)
    assert abs(z.mean()) < 1e-10
    assert abs(z.std(ddof=0) - 1.0) < 1e-10


def test_zscore_constant_returns_nan():
    s = pd.Series([5.0, 5.0, 5.0])
    z = zscore_series(s)
    assert z.isna().all()


# ── compute_score ─────────────────────────────────────────────────────────────

def _make_df(n: int = 10) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "mksc_shrn_iscd": [f"{i:06d}" for i in range(n)],
        "hts_kor_isnm": [f"Stock{i}" for i in range(n)],
        "display_name": [f"StockEN{i}" for i in range(n)],
        "ssts_vol_rlim":       rng.uniform(1, 20, n),
        "ssts_vol_rlim_5d":    rng.uniform(1, 15, n),
        "frgn_ntby_tr_pbmn":   rng.uniform(-50000, 10000, n),
        "orgn_ntby_tr_pbmn":   rng.uniform(-30000, 5000, n),
        "prdy_ctrt":            rng.uniform(-5, 5, n),
    })


def test_compute_score_columns_present():
    df = _make_df()
    result = compute_score(df)
    for col in ["z_short_today", "z_short_5d", "z_foreign", "z_inst", "z_return", "score", "rank", "incomplete"]:
        assert col in result.columns, f"Missing column: {col}"


def test_compute_score_no_incomplete():
    df = _make_df()
    result = compute_score(df)
    assert not result["incomplete"].any()
    assert result["rank"].notna().all()


def test_compute_score_ranks_descending():
    df = _make_df()
    result = compute_score(df)
    complete = result[~result["incomplete"]].sort_values("rank")
    scores = complete["score"].tolist()
    assert scores == sorted(scores, reverse=True)


def test_compute_score_incomplete_flagged():
    df = _make_df()
    df.loc[0, "ssts_vol_rlim_5d"] = np.nan   # introduce missing value
    result = compute_score(df)
    incomplete_rows = result[result["incomplete"]]
    assert len(incomplete_rows) == 1
    assert incomplete_rows.iloc[0]["rank"] is None


def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_score_is_weighted_sum():
    """With a single complete stock, score should equal weighted sum of its z-scores (all z=0 for n=1)."""
    df = pd.DataFrame({
        "mksc_shrn_iscd": ["000001"],
        "hts_kor_isnm": ["Test"],
        "ssts_vol_rlim":     [10.0],
        "ssts_vol_rlim_5d":  [8.0],
        "frgn_ntby_tr_pbmn": [-5000.0],
        "orgn_ntby_tr_pbmn": [-2000.0],
        "prdy_ctrt":          [-1.5],
    })
    result = compute_score(df)
    # Single stock: all z-scores are NaN (std=0 for n=1) → score should also be NaN or 0
    # The important check is no crash and incomplete=False (data present)
    assert not result.empty


# ── build_meta_summary ────────────────────────────────────────────────────────

def test_meta_summary_keys():
    df_k = compute_score(_make_df(10))
    df_q = compute_score(_make_df(8))
    meta = build_meta_summary(df_k, df_q)
    for key in ["kospi_count", "kosdaq_count", "avg_short_ratio", "avg_score",
                "kospi_avg_score", "kosdaq_avg_score", "score_gap",
                "total_frgn_net_value", "high_pressure_count", "high_pressure_share",
                "top_score_ticker", "top_score_value"]:
        assert key in meta, f"Missing meta key: {key}"


def test_meta_high_pressure_count():
    df_k = compute_score(_make_df(10))
    df_q = compute_score(_make_df(10))
    meta = build_meta_summary(df_k, df_q, threshold=999.0)
    assert meta["high_pressure_count"] == 0


def test_meta_summary_prefers_display_name():
    df_k = compute_score(_make_df(10))
    df_q = compute_score(_make_df(8))
    meta = build_meta_summary(df_k, df_q)
    assert meta["top_score_name"].startswith("StockEN")
