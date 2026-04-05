"""
Temporary test script: probe KIS API for a market-cap ranking endpoint.
Tries two candidate TR_IDs and prints what comes back.
Delete this file after testing.
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
from pipeline.auth import build_headers, get_token, load_config


async def try_endpoint(client, token, config, tr_id, url, params, label):
    print(f"\n{'='*60}")
    print(f"[{label}]  TR_ID={tr_id}")
    print(f"URL: {url}")
    try:
        resp = await client.get(
            url,
            headers=build_headers(token, tr_id, config),
            params=params,
            timeout=15,
        )
        print(f"HTTP {resp.status_code}")
        data = resp.json()
        rt_cd = data.get("rt_cd", "?")
        msg1  = data.get("msg1", "")
        print(f"rt_cd={rt_cd}  msg1={msg1}")

        # Try common output keys
        rows = None
        for key in ("output", "output1", "output2", "Output"):
            if isinstance(data.get(key), list):
                rows = data[key]
                print(f"Output key='{key}'  rows={len(rows)}")
                break

        if rows:
            print("First row keys:", list(rows[0].keys()))
            print("First row sample:", {k: rows[0][k] for k in list(rows[0].keys())[:8]})
            # Try to find market cap field
            for field in ("stck_avls", "mktcap", "MKTCAP", "hts_avls", "data_rank"):
                if field in rows[0]:
                    print(f"  → market cap field '{field}' = {rows[0][field]}")
        else:
            print("No list output found. Top-level keys:", list(data.keys()))
    except Exception as e:
        print(f"ERROR: {e}")


async def main():
    config = load_config("config.yaml")
    token  = get_token(config)
    base   = config["kis"]["base_url_prod"]

    async with httpx.AsyncClient(timeout=20) as client:

        # ── Candidate 1: Market cap ranking via FHPST01710000 ─────────────────
        # KIS "국내주식 시세 순위" — sort code 20171 = market cap
        await try_endpoint(
            client, token, config,
            tr_id="FHPST01710000",
            url=f"{base}/uapi/domestic-stock/v1/ranking/market-cap",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",   # J=KOSPI
                "FID_COND_SCR_DIV_CODE":  "20171",
                "FID_INPUT_ISCD":         "0000",
                "FID_DIV_CLS_CODE":       "0",
                "FID_BLNG_CLS_CODE":      "0",
                "FID_TRGT_CLS_CODE":      "0",
                "FID_TRGT_EXLS_CLS_CODE": "0",
                "FID_INPUT_PRICE_1":      "",
                "FID_INPUT_PRICE_2":      "",
                "FID_VOL_CNT":            "",
                "FID_INPUT_DATE_1":       "",
            },
            label="KIS market-cap ranking (KOSPI) — FHPST01710000",
        )

        await asyncio.sleep(0.5)

        # ── Candidate 2: Same endpoint, KOSDAQ ────────────────────────────────
        await try_endpoint(
            client, token, config,
            tr_id="FHPST01710000",
            url=f"{base}/uapi/domestic-stock/v1/ranking/market-cap",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE":  "20171",
                "FID_INPUT_ISCD":         "1000",  # KOSDAQ
                "FID_DIV_CLS_CODE":       "0",
                "FID_BLNG_CLS_CODE":      "0",
                "FID_TRGT_CLS_CODE":      "0",
                "FID_TRGT_EXLS_CLS_CODE": "0",
                "FID_INPUT_PRICE_1":      "",
                "FID_INPUT_PRICE_2":      "",
                "FID_VOL_CNT":            "",
                "FID_INPUT_DATE_1":       "",
            },
            label="KIS market-cap ranking (KOSDAQ) — FHPST01710000",
        )

        await asyncio.sleep(0.5)

        # ── Candidate 3: Foreign-inst total (already in use) — check row count
        # This endpoint covers ALL listed stocks; we check if it returns enough
        # rows to build a market cap universe indirectly.
        await try_endpoint(
            client, token, config,
            tr_id="FHPTJ04400000",
            url=f"{base}/uapi/domestic-stock/v1/quotations/foreign-institution-total",
            params={
                "FID_COND_MRKT_DIV_CODE": "V",
                "FID_COND_SCR_DIV_CODE":  "16449",
                "FID_INPUT_ISCD":         "0001",  # KOSPI
                "FID_DIV_CLS_CODE":       "1",
                "FID_RANK_SORT_CLS_CODE": "1",
                "FID_ETC_CLS_CODE":       "0",
            },
            label="Foreign/inst aggregate KOSPI (existing) — row count check",
        )


asyncio.run(main())
