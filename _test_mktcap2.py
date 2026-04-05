"""
Temporary test: KRX public API — establish session then query market cap.
Delete after testing.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx
import pandas as pd

_KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
DATE = "20260403"

def _session_headers(cookies: dict) -> dict:
    h = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.cmd",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Origin": "https://data.krx.co.kr",
    }
    return h


def main():
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        # 1. Visit portal to get session cookie
        print("Step 1: establishing session...")
        r = client.get("https://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT01501.cmd")
        print(f"  GET portal → {r.status_code}, cookies: {dict(r.cookies)}")

        # 2. Query KOSPI market cap ranking
        print("\nStep 2: querying KOSPI market cap...")
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "locale": "ko_KR",
            "mktCls": "STK",
            "trdDd": DATE,
            "money": "1",
            "csvxls_isNo": "false",
        }
        r2 = client.post(
            _KRX_URL,
            headers=_session_headers(dict(client.cookies)),
            data=payload,
        )
        print(f"  POST → {r2.status_code}")
        if r2.status_code != 200:
            print(f"  Body: {r2.text[:300]}")
            # Try alternate params
            print("\nStep 2b: trying mktTpCd=1...")
            payload2 = {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
                "mktTpCd": "1",
                "trdDd": DATE,
                "money": "1",
            }
            r2b = client.post(_KRX_URL, headers=_session_headers({}), data=payload2)
            print(f"  POST → {r2b.status_code}")
            print(f"  Body: {r2b.text[:300]}")
            return

        data = r2.json()
        rows = None
        for key in ("OutBlock_1","outBlock_1","output","block1"):
            if isinstance(data.get(key), list):
                rows = data[key]
                break

        if not rows:
            print(f"  No rows. Keys={list(data.keys())}  sample={str(data)[:300]}")
            return

        df = pd.DataFrame(rows)
        print(f"  {len(df)} KOSPI rows | cols: {list(df.columns)[:12]}")
        print(f"  Row 0: { {k:v for k,v in list(df.iloc[0].items())[:8]} }")

        # 3. KOSDAQ
        print("\nStep 3: querying KOSDAQ market cap...")
        payload3 = {**payload, "mktCls": "KSQ"}
        r3 = client.post(_KRX_URL, headers=_session_headers({}), data=payload3)
        print(f"  POST → {r3.status_code}")
        if r3.status_code != 200:
            print(f"  Body: {r3.text[:200]}")
            return
        data3 = r3.json()
        rows3 = None
        for key in ("OutBlock_1","outBlock_1","output","block1"):
            if isinstance(data3.get(key), list):
                rows3 = data3[key]
                break
        df3 = pd.DataFrame(rows3) if rows3 else pd.DataFrame()
        print(f"  {len(df3)} KOSDAQ rows")

        if rows and rows3:
            # Build top 100
            mktcap_col = next((c for c in ["MKTCAP","mktcap","MKT_CAP"] if c in df.columns), None)
            ticker_col  = next((c for c in ["ISU_SRT_CD","ISU_CD"] if c in df.columns), None)
            name_col    = next((c for c in ["ISU_ABBRV","ISU_NM"] if c in df.columns), None)
            print(f"\n  mktcap_col={mktcap_col}, ticker_col={ticker_col}, name_col={name_col}")

            if mktcap_col and ticker_col:
                def prep(frame, mkt):
                    out = frame[[ticker_col, name_col or ticker_col, mktcap_col]].copy()
                    out.columns = ["ticker","name","mktcap_raw"]
                    out["market"] = mkt
                    out["mktcap"] = pd.to_numeric(out["mktcap_raw"].astype(str).str.replace(",",""), errors="coerce")
                    return out[["ticker","name","market","mktcap"]]

                combined = pd.concat([prep(df,"KOSPI"), prep(df3,"KOSDAQ")], ignore_index=True)
                combined = combined.dropna(subset=["mktcap"]).sort_values("mktcap", ascending=False).reset_index(drop=True)
                top100 = combined.head(100)

                print(f"\n{'='*60}")
                print(f"TOP 100 by market cap — {DATE}")
                print(f"{'='*60}")
                print(f"  KOSPI : {(top100['market']=='KOSPI').sum()}")
                print(f"  KOSDAQ: {(top100['market']=='KOSDAQ').sum()}")
                print(f"\nTop 15:")
                for i, row in top100.head(15).iterrows():
                    cap_t = row['mktcap'] / 100  # KRW 억 → 조
                    print(f"  {i+1:>3}. [{row['market']:6}] {row['ticker']}  {row['name']:<22}  {cap_t:>8.1f}조")


main()
