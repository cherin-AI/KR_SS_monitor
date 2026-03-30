from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xc
import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.auth import load_config
from pipeline.run import run_pipeline


KST = ZoneInfo("Asia/Seoul")


def _parse_hhmm(value: str) -> tuple[int, int]:
    hour, minute = value.split(":", 1)
    return int(hour), int(minute)


def _should_run(config: dict) -> tuple[bool, str]:
    schedule = config.get("schedule", {})
    run_after = schedule.get("run_after", "15:44")
    cutoff_hour, cutoff_minute = _parse_hhmm(run_after)
    now = datetime.now(KST)
    if (now.hour, now.minute) < (cutoff_hour, cutoff_minute):
        return False, f"Skipping: current time {now:%H:%M} KST is before scheduled run_after {run_after}"

    cal = xc.get_calendar("XKRX")
    today = pd.Timestamp(now.date())
    if not cal.is_session(today):
        return False, f"Skipping: {today.date()} is not an XKRX trading session"

    return True, f"Running daily pipeline for {today.date()} at {now:%H:%M} KST"


def main() -> None:
    parser = argparse.ArgumentParser(description="Holiday-aware daily pipeline runner")
    parser.add_argument("--config", default="config.example.yaml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    should_run, message = _should_run(config)
    print(message)
    if not should_run:
        return

    asyncio.run(run_pipeline(config, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
