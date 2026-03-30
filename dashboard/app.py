"""
dashboard/app.py
FastAPI server for the Short Pressure Monitor dashboard.

Endpoints:
    GET /              → serves dashboard.html
    GET /api/data      → returns latest.json
    GET /api/data/{date} → returns YYYYMMDD.json snapshot
"""
from __future__ import annotations

import json
import threading
import webbrowser
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR.parent / "data" / "output"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Korea Short Pressure Monitor")

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(str(BASE_DIR / "dashboard.html"))


@app.get("/api/data")
async def get_latest() -> JSONResponse:
    """Return the most recent daily snapshot."""
    latest = DATA_DIR / "latest.json"
    if not latest.exists():
        raise HTTPException(status_code=404, detail="No data available yet. Run pipeline/run.py first.")
    return JSONResponse(json.loads(latest.read_text(encoding="utf-8")))


@app.get("/api/data/{date}")
async def get_by_date(date: str) -> JSONResponse:
    """
    Return a specific day's snapshot.
    date: YYYYMMDD or YYYY-MM-DD format.
    """
    clean = date.replace("-", "")
    if len(clean) != 8 or not clean.isdigit():
        raise HTTPException(status_code=400, detail="date must be YYYYMMDD or YYYY-MM-DD")
    snapshot = DATA_DIR / f"{clean}.json"
    if not snapshot.exists():
        raise HTTPException(status_code=404, detail=f"No snapshot for {clean}")
    return JSONResponse(json.loads(snapshot.read_text(encoding="utf-8")))


@app.get("/api/dates")
async def list_dates() -> JSONResponse:
    """Return available snapshot dates (newest first)."""
    dates = sorted(
        [p.stem for p in DATA_DIR.glob("????????.json")],
        reverse=True,
    )
    return JSONResponse({"dates": dates})


if __name__ == "__main__":
    import sys

    import uvicorn

    # Read port from config if available
    port = 8080
    try:
        import yaml

        cfg_path = BASE_DIR.parent / "config.yaml"
        if cfg_path.exists():
            cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
            port = cfg.get("output", {}).get("serve_port", 8080)
    except Exception:
        pass

    print(f"Starting dashboard on http://localhost:{port}")

    def _open_browser() -> None:
        webbrowser.open_new_tab(f"http://localhost:{port}")

    threading.Timer(1.0, _open_browser).start()
    uvicorn.run(app, host="0.0.0.0", port=port, reload="--reload" in sys.argv)
