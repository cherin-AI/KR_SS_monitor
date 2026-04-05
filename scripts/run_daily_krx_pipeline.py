from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.run_krx import run_pipeline


KST = ZoneInfo("Asia/Seoul")


def _should_run() -> tuple[bool, str]:
    now = datetime.now(KST)
    cal = xc.get_calendar("XKRX")
    today = pd.Timestamp(now.date())
    if not cal.is_session(today):
        return False, f"Skipping: {today.date()} is not an XKRX trading session"
    return True, f"Running KRX daily pipeline for {today.date()} at {now:%H:%M} KST"


def main() -> None:
    should_run, message = _should_run()
    print(message)
    if not should_run:
        return
    run_pipeline(dry_run=False, preview_rows=5)


if __name__ == "__main__":
    main()
