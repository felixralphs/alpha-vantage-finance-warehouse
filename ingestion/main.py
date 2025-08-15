import os, time, json, datetime, pathlib
import requests
from dotenv import load_dotenv

# Load .env from repo root
load_dotenv(dotenv_path=pathlib.Path(__file__).parent.parent / ".env")


API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "").strip()
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "AAPL").split(",") if s.strip()]
LOCAL_RAW_DIR = os.getenv("LOCAL_RAW_DIR", "_local_raw").strip()

BASE_URL = "https://www.alphavantage.co/query"
REQUESTS_PER_MINUTE_SAFE = 5  # AV free tier ~5/min
SLEEP_SECONDS = int(60 / REQUESTS_PER_MINUTE_SAFE) + 1  # ~13s

def fetch_daily_adjusted(symbol: str):
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "outputsize": "compact",
        "apikey": API_KEY,
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "Time Series (Daily)" not in data and "Note" in data:
        raise RuntimeError(f"Rate limited or note from API for {symbol}: {data.get('Note')}")
    if "Error Message" in data:
        raise RuntimeError(f"API error for {symbol}: {data['Error Message']}")
    return data

def save_json(payload: dict, path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

def main():
    if not API_KEY:
        raise RuntimeError("Missing ALPHAVANTAGE_API_KEY (set it in .env).")
    today = datetime.datetime.now(datetime.timezone.utc)
    day_path = today.strftime("%Y/%m/%d")

    for i, sym in enumerate(SYMBOLS, start=1):
        print(f"[{i}/{len(SYMBOLS)}] Fetching {sym} daily_adjusted …")
        data = fetch_daily_adjusted(sym)
        out_path = pathlib.Path(LOCAL_RAW_DIR) / "prices" / sym / day_path / "daily_adjusted.json"
        save_json(data, out_path)
        print(f"   → Saved to {out_path}")
        if i < len(SYMBOLS):
            print(f"   sleeping {SLEEP_SECONDS}s to respect rate limits…")
            time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()
