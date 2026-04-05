"""
pipeline/run_krx.py
Daily batch entry point for the KRX short pressure monitor.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.krx_short_selling import (
    fetch_short_selling_balance_top50,
    fetch_short_selling_volume_history,
    fetch_short_selling_volume_top50,
)
from pipeline.score import compute_score

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")


def _snapshot_date() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d")


def _market_label(market: str) -> str:
    return "KOSPI" if market == "KOSPI" else "KOSDAQ"


def _five_day_average(ticker: str, snapshot_date: str) -> float | None:
    start = (datetime.now(KST).date() - pd.Timedelta(days=14)).strftime("%Y%m%d")
    end = snapshot_date.replace("-", "")
    history = fetch_short_selling_volume_history(start, end, ticker)
    if history.empty or "short_ratio" not in history.columns:
        return None
    series = pd.to_numeric(history["short_ratio"], errors="coerce").dropna().head(5)
    if series.empty:
        return None
    return round(float(series.mean()), 4)


def _build_market_frame(market: str, snapshot_date: str) -> pd.DataFrame:
    label = _market_label(market)
    volume = fetch_short_selling_volume_top50(snapshot_date.replace("-", ""), market=market)
    balance = fetch_short_selling_balance_top50(snapshot_date.replace("-", ""), market=market)

    if volume.empty:
        return volume

    df = volume.reset_index().rename(columns={"index": "ticker"})
    if "ticker" not in df.columns:
        df = df.rename(columns={df.columns[0]: "ticker"})

    balance_df = balance.reset_index().rename(columns={"index": "ticker"})
    if not balance_df.empty:
        df = df.merge(
            balance_df[["ticker", "short_balance", "listed_shares", "short_balance_amount", "market_cap", "short_balance_ratio"]],
            on="ticker",
            how="left",
        )

    logger.info("Fetching 5-day short averages for %s (%d tickers)...", label, len(df))
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        avg_map = dict(zip(df["ticker"], ex.map(lambda t: _five_day_average(str(t), snapshot_date), df["ticker"])))

    df["mksc_shrn_iscd"] = df["ticker"].astype(str)
    df["hts_kor_isnm"] = df.get("display_name")
    df["display_name"] = df.get("display_name")
    df["ssts_vol_rlim"] = pd.to_numeric(df.get("short_ratio"), errors="coerce")
    df["ssts_vol_rlim_5d"] = df["ticker"].map(avg_map)
    df["prdy_ctrt"] = pd.to_numeric(df.get("return_pct"), errors="coerce")
    df["frgn_ntby_tr_pbmn"] = pd.NA
    df["orgn_ntby_tr_pbmn"] = pd.NA
    df["acml_tr_pbmn"] = pd.NA
    df["source"] = "KRX"

    scored = compute_score(df, mode="krx")
    return scored


def _safe_num(v: object, digits: int = 4):
    if pd.isna(v):
        return None
    if isinstance(v, float):
        return round(v, digits)
    return v


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    cols = [
        "rank",
        "mksc_shrn_iscd",
        "display_name",
        "hts_kor_isnm",
        "short_balance",
        "listed_shares",
        "short_balance_amount",
        "market_cap",
        "short_balance_ratio",
        "ssts_vol_rlim",
        "ssts_vol_rlim_5d",
        "prdy_ctrt",
        "z_short_today",
        "z_short_5d",
        "z_return",
        "score",
        "incomplete",
    ]
    out: list[dict] = []
    for _, row in df.iterrows():
        rec: dict[str, object] = {}
        for c in cols:
            rec[c] = _safe_num(row.get(c))
        out.append(rec)
    return out


def _print_preview(name: str, df: pd.DataFrame, rows: int = 5) -> None:
    if df.empty:
        print(f"\n{name}: no rows")
        return

    preview = df.head(rows).copy()
    print(f"\n{name} top {min(rows, len(preview))}")
    header = ["rank", "ticker", "name", "short%", "5d avg", "score"]
    print(" | ".join(header))
    print("-" * 72)
    for _, row in preview.iterrows():
        vals = []
        vals.append(str(int(row["rank"])) if pd.notna(row.get("rank")) else "—")
        vals.append(str(row.get("mksc_shrn_iscd", "—")))
        vals.append(str(row.get("display_name") or row.get("hts_kor_isnm") or "—"))
        vals.append(f'{float(row["ssts_vol_rlim"]):.2f}' if pd.notna(row.get("ssts_vol_rlim")) else "—")
        vals.append(f'{float(row["ssts_vol_rlim_5d"]):.2f}' if pd.notna(row.get("ssts_vol_rlim_5d")) else "—")
        vals.append(f'{float(row["score"]):.2f}' if pd.notna(row.get("score")) else "—")
        print(" | ".join(vals))


def _meta_summary(df_kospi: pd.DataFrame, df_kosdaq: pd.DataFrame, threshold: float = 1.0) -> dict:
    all_df = pd.concat([df_kospi, df_kosdaq], ignore_index=True)
    complete = all_df[~all_df["incomplete"]]
    if complete.empty:
        return {
            "kospi_count": len(df_kospi),
            "kosdaq_count": len(df_kosdaq),
            "avg_short_ratio": None,
            "avg_score": None,
            "high_pressure_count": 0,
            "high_pressure_share": None,
            "top_score_ticker": None,
            "top_score_name": None,
            "top_score_value": None,
            "source": "KRX",
        }

    top_row = complete.loc[complete["score"].idxmax()]
    high_pressure = int((complete["score"] >= threshold).sum())
    return {
        "kospi_count": len(df_kospi),
        "kosdaq_count": len(df_kosdaq),
        "avg_short_ratio": round(float(complete["ssts_vol_rlim"].mean()), 2),
        "avg_score": round(float(complete["score"].mean()), 2),
        "high_pressure_count": high_pressure,
        "high_pressure_share": round((high_pressure / len(complete)) * 100, 1),
        "top_score_ticker": top_row["mksc_shrn_iscd"],
        "top_score_name": top_row.get("display_name") or top_row.get("hts_kor_isnm"),
        "top_score_value": round(float(top_row["score"]), 2),
        "source": "KRX",
    }


def run_pipeline(dry_run: bool = False, preview_rows: int = 5) -> dict:
    snapshot_date = _snapshot_date()
    logger.info("Fetching KRX short-selling snapshot for %s", snapshot_date)
    kospi = _build_market_frame("KOSPI", snapshot_date)
    kosdaq = _build_market_frame("KOSDAQ", snapshot_date)

    meta = _meta_summary(kospi, kosdaq)

    if preview_rows > 0:
        _print_preview("KOSPI", kospi, preview_rows)
        _print_preview("KOSDAQ", kosdaq, preview_rows)

    payload = {
        "date": snapshot_date,
        "generated_at": datetime.now(KST).isoformat(),
        "source": "krx",
        "kospi": _df_to_records(kospi),
        "kosdaq": _df_to_records(kosdaq),
        "meta": meta,
    }

    if not dry_run:
        out_dir = Path("data/output")
        out_dir.mkdir(parents=True, exist_ok=True)
        date_file = out_dir / f"{snapshot_date.replace('-', '')}.json"
        latest_file = out_dir / "latest.json"
        blob = json.dumps(payload, ensure_ascii=False, indent=2)
        date_file.write_text(blob, encoding="utf-8")
        latest_file.write_text(blob, encoding="utf-8")
        logger.info("Output written → %s", date_file)
    else:
        logger.info("[dry-run] skipping file write")

    logger.info(
        "Done. KOSPI=%d KOSDAQ=%d high_pressure=%d top=%s (%.2f)",
        meta["kospi_count"],
        meta["kosdaq_count"],
        meta["high_pressure_count"],
        meta["top_score_ticker"],
        meta["top_score_value"] or 0,
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="KRX short pressure monitor pipeline")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--preview", type=int, default=5, help="Print a compact top-N preview per market.")
    args = parser.parse_args()
    run_pipeline(dry_run=args.dry_run, preview_rows=args.preview)


if __name__ == "__main__":
    main()
