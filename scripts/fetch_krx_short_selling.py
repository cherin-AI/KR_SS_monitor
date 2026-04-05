from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.krx_short_selling import (
    KRXShortSnapshot,
    fetch_short_selling_balance_top50,
    fetch_short_selling_volume_top50,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch KRX short-selling data directly from KRX.")
    parser.add_argument("--date", required=True, help="Trade date in YYYYMMDD format.")
    parser.add_argument("--fromdate", default=None, help="Start date for a ticker history query.")
    parser.add_argument("--todate", default=None, help="End date for a ticker history query.")
    parser.add_argument("--ticker", default=None, help="Ticker for history queries.")
    parser.add_argument("--market", default="KOSPI", help="KOSPI, KOSDAQ, or KONEX.")
    parser.add_argument("--mode", choices=["volume-top50", "balance-top50", "history"], default="volume-top50")
    parser.add_argument("--output", default=None, help="Optional output JSON file path.")
    args = parser.parse_args()

    if args.mode == "history":
        if not args.ticker:
            raise SystemExit("--ticker is required for --mode history")
        fromdate = args.fromdate or args.date
        todate = args.todate or args.date
        payload = KRXShortSnapshot(
            ticker=args.ticker,
            fromdate=fromdate,
            todate=todate,
            market=args.market,
        ).to_dict()
    elif args.mode == "balance-top50":
        payload = fetch_short_selling_balance_top50(args.date, market=args.market).to_dict(orient="records")
    else:
        payload = fetch_short_selling_volume_top50(args.date, market=args.market).to_dict(orient="records")

    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
