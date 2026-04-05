"""
pipeline/score.py
Z-score normalisation and weighted short pressure score computation.
All z-scores are computed within each market independently.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


WEIGHTS: dict[str, float] = {
    "short_today": 0.35,
    "short_5d":    0.15,
    "foreign_sell": 0.25,
    "inst_sell":   0.15,
    "return_neg":  0.10,
}

assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, "Weights must sum to 1.0"

KRX_WEIGHTS: dict[str, float] = {
    "short_today": 0.50,
    "short_5d": 0.30,
    "return_neg": 0.20,
}

assert abs(sum(KRX_WEIGHTS.values()) - 1.0) < 1e-9, "KRX weights must sum to 1.0"


def zscore_series(s: pd.Series) -> pd.Series:
    """
    Standard z-score normalisation.
    Returns NaN for constant series (std == 0) to avoid division by zero.
    """
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(np.nan, index=s.index)
    return (s - s.mean()) / std


def compute_score(
    df: pd.DataFrame,
    weights: dict[str, float] | None = None,
    mode: str = "kis",
) -> pd.DataFrame:
    """
    Compute short pressure score for all stocks in df.

    Required input columns:
        ssts_vol_rlim        float  short ratio today (%)
        ssts_vol_rlim_5d     float  5-day mean of ssts_vol_rlim
        frgn_ntby_tr_pbmn   float  foreign net buy value (KRW M; negative = net sell)
        orgn_ntby_tr_pbmn   float  institution net buy value (KRW M)
        prdy_ctrt            float  return % (negative = price decline)

    Added output columns:
        z_short_today, z_short_5d, z_foreign, z_inst, z_return
        score        float   weighted sum of z-scores
        rank         int     rank within this DataFrame (1 = highest pressure)
        incomplete   bool    True if any component was missing
    """
    if mode not in {"kis", "krx"}:
        raise ValueError(f"Unsupported scoring mode: {mode}")

    if mode == "krx":
        w = weights or KRX_WEIGHTS
        required = [
            "ssts_vol_rlim",
            "ssts_vol_rlim_5d",
            "prdy_ctrt",
        ]
    else:
        w = weights or WEIGHTS
        required = [
            "ssts_vol_rlim",
            "ssts_vol_rlim_5d",
            "frgn_ntby_tr_pbmn",
            "orgn_ntby_tr_pbmn",
            "prdy_ctrt",
        ]

    df = df.copy()
    for col in required:
        if col not in df.columns:
            df[col] = np.nan

    # Mark incomplete rows before z-scoring
    df["incomplete"] = df[required].isnull().any(axis=1)

    # Z-score each component (on complete rows only to avoid contamination)
    complete = df[~df["incomplete"]].copy()

    if not complete.empty:
        complete["z_short_today"] = zscore_series(complete["ssts_vol_rlim"])
        complete["z_short_5d"]    = zscore_series(complete["ssts_vol_rlim_5d"])
        complete["z_return"]      = zscore_series(-complete["prdy_ctrt"])

        if mode == "krx":
            complete["score"] = (
                w["short_today"] * complete["z_short_today"]
                + w["short_5d"] * complete["z_short_5d"]
                + w["return_neg"] * complete["z_return"]
            )
            complete["z_foreign"] = np.nan
            complete["z_inst"] = np.nan
        else:
            complete["z_foreign"] = zscore_series(-complete["frgn_ntby_tr_pbmn"])
            complete["z_inst"] = zscore_series(-complete["orgn_ntby_tr_pbmn"])
            complete["score"] = (
                w["short_today"]  * complete["z_short_today"]
                + w["short_5d"]    * complete["z_short_5d"]
                + w["foreign_sell"] * complete["z_foreign"]
                + w["inst_sell"]   * complete["z_inst"]
                + w["return_neg"]  * complete["z_return"]
            )
    else:
        for col in ["z_short_today", "z_short_5d", "z_foreign", "z_inst", "z_return", "score"]:
            complete[col] = np.nan

    # Merge z-score columns back
    z_cols = ["z_short_today", "z_short_5d", "z_foreign", "z_inst", "z_return", "score"]
    df = df.merge(complete[["mksc_shrn_iscd"] + z_cols], on="mksc_shrn_iscd", how="left")

    # Rank: complete stocks by score desc, incomplete at bottom
    df_complete = df[~df["incomplete"]].sort_values("score", ascending=False)
    df_incomplete = df[df["incomplete"]]

    df_complete = df_complete.reset_index(drop=True)
    df_complete["rank"] = df_complete.index + 1

    df_incomplete = df_incomplete.copy()
    df_incomplete["rank"] = None

    result = pd.concat([df_complete, df_incomplete], ignore_index=True)
    return result


def build_meta_summary(df_all: pd.DataFrame, threshold: float = 1.0) -> dict:
    """
    Compute aggregate KPI values across the combined universe.

    df_all must have a "market" column with values "KOSPI" or "KOSDAQ".
    """
    complete = df_all[~df_all["incomplete"]]

    def _safe_mean(series: pd.Series, digits: int = 2) -> float | None:
        value = series.mean()
        return round(float(value), digits) if pd.notna(value) else None

    avg_short = _safe_mean(complete["ssts_vol_rlim"], 2) if not complete.empty else None
    avg_score = _safe_mean(complete["score"], 2) if not complete.empty else None
    total_frgn = round(complete["frgn_ntby_tr_pbmn"].sum(), 0) if not complete.empty else None
    high_pressure = int((complete["score"] >= threshold).sum()) if not complete.empty else 0
    high_pressure_share = round((high_pressure / len(complete)) * 100, 1) if not complete.empty else None

    top_row = complete.loc[complete["score"].idxmax()] if not complete.empty else None

    def _first_text(*values: object) -> str:
        for value in values:
            if value is None:
                continue
            if pd.isna(value):
                continue
            text = str(value)
            if text:
                return text
        return ""

    kospi_count  = int((df_all["market"] == "KOSPI").sum())  if "market" in df_all.columns else 0
    kosdaq_count = int((df_all["market"] == "KOSDAQ").sum()) if "market" in df_all.columns else 0

    return {
        "total_count": len(df_all),
        "kospi_count": kospi_count,
        "kosdaq_count": kosdaq_count,
        "avg_short_ratio": avg_short,
        "avg_score": avg_score,
        "total_frgn_net_value": total_frgn,
        "high_pressure_count": high_pressure,
        "high_pressure_share": high_pressure_share,
        "top_score_ticker": top_row["mksc_shrn_iscd"] if top_row is not None else None,
        "top_score_name": _first_text(top_row.get("display_name"), top_row.get("name_ko"))
            if top_row is not None else None,
        "top_score_value": round(float(top_row["score"]), 2) if top_row is not None else None,
    }
