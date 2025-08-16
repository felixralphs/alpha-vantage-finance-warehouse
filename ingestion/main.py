"""
main.py — Alpha Vantage ingestion

- Reads config from .env in the repo root
- For each symbol:
    * Fetch TIME_SERIES_DAILY_ADJUSTED (prices)
    * Fetch fundamentals: OVERVIEW, INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW
    * Save JSON locally under _local_raw/...
    * If WRITE_TO_GCS=true, upload the same files to GCS under raw/...

This script is “ingest only”: store raw JSON. Parsing/flattening happens later in BigQuery.
"""

import os
import time
import json
import datetime
import pathlib
from typing import Dict, Any

import requests
from dotenv import load_dotenv

# ---- Load .env from repo root (works no matter where you run from) ----
load_dotenv(dotenv_path=pathlib.Path(__file__).parent.parent / ".env")

# ---- Config from environment ----
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL").split(",") if s.strip()]
LOCAL_RAW_DIR = os.getenv("LOCAL_RAW_DIR", "_local_raw").strip()
OUTPUTSIZE = os.getenv("OUTPUTSIZE", "compact").strip().lower()  # 'compact' (~100 days) or 'full'

# Optional GCS mirroring
WRITE_TO_GCS = os.getenv("WRITE_TO_GCS", "false").strip().lower() == "true"
GCS_BUCKET = os.getenv("GCS_BUCKET", "").replace("gs://", "").strip()

# ---- Alpha Vantage settings ----
BASE_URL = "https://www.alphavantage.co/query"
REQUESTS_PER_MINUTE_SAFE = 5          # Free tier ~5/min
SLEEP_SECONDS = int(60 / REQUESTS_PER_MINUTE_SAFE) + 1  # ~13s

# Fundamentals endpoints to fetch
FUND_FUNCS = ["OVERVIEW", "INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"]

# ---- Optional: Google Cloud Storage client (lazy) ----
_storage_client = None
def _get_storage_client():
    """Create a singleton GCS client when needed."""
    global _storage_client
    if _storage_client is None:
        # Uses Application Default Credentials (gcloud auth application-default login)
        from google.cloud import storage  # import here so script still runs without GCS installed
        _storage_client = storage.Client()
    return _storage_client


# ---------------------------
# HTTP helpers
# ---------------------------
def _get(params: Dict[str, Any]) -> Dict[str, Any]:
    """Perform a GET to Alpha Vantage with basic error handling."""
    params = {**params, "apikey": API_KEY}
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    # Handle AV's JSON-level signals
    if isinstance(data, dict):
        if "Note" in data and "Thank you" in str(data["Note"]):
            # Rate limited or throttled
            raise RuntimeError(f"Rate limited: {data['Note']}")
        if "Error Message" in data:
            raise RuntimeError(f"API error: {data['Error Message']}")
    return data


def _with_retry(fn, *args, retries: int = 1, wait: int = SLEEP_SECONDS):
    """Minimal retry wrapper for rate limit hiccups."""
    for attempt in range(retries + 1):
        try:
            return fn(*args)
        except RuntimeError as e:
            msg = str(e)
            is_rate = "Rate limited" in msg or "Please consider" in msg
            if is_rate and attempt < retries:
                print(f"   ⚠ {msg} — retrying in {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                continue
            raise


# ---------------------------
# Fetchers
# ---------------------------
def fetch_daily_adjusted(symbol: str) -> Dict[str, Any]:
    """Fetch TIME_SERIES_DAILY_ADJUSTED for a symbol."""
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": OUTPUTSIZE,  # 'compact' or 'full'
    }
    return _get(params)


def fetch_fundamental(fn: str, symbol: str) -> Dict[str, Any]:
    """Fetch a single fundamentals endpoint (e.g., OVERVIEW, INCOME_STATEMENT)."""
    params = {"function": fn, "symbol": symbol}
    return _get(params)


# ---------------------------
# IO helpers
# ---------------------------
def save_json(payload: Dict[str, Any], path: pathlib.Path) -> None:
    """Save JSON to disk, creating parent dirs as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def upload_to_gcs(local_path: pathlib.Path, gcs_key: pathlib.Path) -> None:
    """Upload a local file to GCS at the given key (if WRITE_TO_GCS enabled)."""
    if not WRITE_TO_GCS or not GCS_BUCKET:
        return
    client = _get_storage_client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(str(gcs_key).replace("\\", "/"))  # ensure POSIX-style path
    blob.upload_from_filename(str(local_path))
    print(f"   ↗ uploaded to gs://{GCS_BUCKET}/{blob.name}")


# ---------------------------
# Main
# ---------------------------
def main():
    if not API_KEY:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY (set it in .env).")

    # Use timezone-aware UTC; partition by YYYY/MM/DD
    today = datetime.datetime.now(datetime.timezone.utc)
    day_path = today.strftime("%Y/%m/%d")

    symbols_total = len(SYMBOLS)
    print(f"Starting ingestion for {symbols_total} symbol(s). Outputsize={OUTPUTSIZE}, GCS={'on' if WRITE_TO_GCS else 'off'}")

    for i, sym in enumerate(SYMBOLS, start=1):
        print(f"[{i}/{symbols_total}] Fetching {sym} prices …")
        data_prices = _with_retry(fetch_daily_adjusted, sym, retries=1)
        out_path_prices = pathlib.Path(LOCAL_RAW_DIR) / "prices" / sym / day_path / "daily_adjusted.json"
        save_json(data_prices, out_path_prices)
        print(f"   → saved to {out_path_prices}")

        # Mirror to GCS: raw/prices/<SYM>/YYYY/MM/DD/daily_adjusted.json
        rel_prices = out_path_prices.relative_to(pathlib.Path(LOCAL_RAW_DIR))
        upload_to_gcs(out_path_prices, pathlib.Path("raw") / rel_prices)

        # Fundamentals
        for fn in FUND_FUNCS:
            print(f"   • fundamentals: {fn} for {sym}")
            data_f = _with_retry(fetch_fundamental, fn, sym, retries=1)
            out_f = pathlib.Path(LOCAL_RAW_DIR) / "fundamentals" / sym / day_path / f"{fn.lower()}.json"
            save_json(data_f, out_f)
            print(f"     → saved to {out_f}")

            # Mirror to GCS: raw/fundamentals/<SYM>/YYYY/MM/DD/<fn>.json
            rel_f = out_f.relative_to(pathlib.Path(LOCAL_RAW_DIR))
            upload_to_gcs(out_f, pathlib.Path("raw") / rel_f)

            # Respect per-request pacing
            print(f"     sleeping {SLEEP_SECONDS}s (rate limit)…")
            time.sleep(SLEEP_SECONDS)

        # Extra sleep between symbols to stay under AV free-tier limits
        if i < symbols_total:
            print(f"   sleeping {SLEEP_SECONDS}s before next symbol …")
            time.sleep(SLEEP_SECONDS)

    print("Ingestion complete.")


if __name__ == "__main__":
    main()
