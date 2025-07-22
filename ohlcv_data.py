import requests
import json
from datetime import datetime
import time

SUPABASE_URL = "https://muwqydzmponlsoagasnw.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im11d3F5ZHptcG9ubHNvYWdhc253Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MzIwODM3MywiZXhwIjoyMDY4Nzg0MzczfQ.1l5Uiy760Z2CQAY_pO4dcwIGKY59u5R3OBQ2I-F12Ck"
BUCKET = "signals"

def upload_to_supabase_storage(local_file, remote_name):
    with open(local_file, "rb") as f:
        file_data = f.read()
    endpoint = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{remote_name}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.put(endpoint, headers=headers, data=file_data)
    print(f"[SUPABASE] {remote_name} upload status: {resp.status_code}")

INTERVALS = ["4h", "2h", "30m", "15m"]
LIMIT = 75

def fetch_ohlcv(symbol, interval, limit=LIMIT):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception:
        return []
    ohlcv = []
    seen = set()
    for kline in data:
        key = (kline[0], kline[6])
        if key in seen: continue
        seen.add(key)
        try:
            ohlcv.append({
                "open_time": int(kline[0]),
                "open": float(kline[1]),
                "high": float(kline[2]),
                "low": float(kline[3]),
                "close": float(kline[4]),
                "volume": float(kline[5]),
                "close_time": int(kline[6])
            })
        except: continue
    return ohlcv

def main():
    with open("coins.json", "r", encoding="utf-8") as f:
        coins = json.load(f)["coins"]
    all_data = {"last_update": datetime.now().isoformat(), "data": {}}
    for coin in coins:
        symbol = coin["symbol"]
        all_data["data"][symbol] = {}
        for interval in INTERVALS:
            ohlcv = fetch_ohlcv(symbol, interval)
            all_data["data"][symbol][interval] = ohlcv
            time.sleep(0.4)
    with open("ohlcv_data.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    upload_to_supabase_storage("ohlcv_data.json", "ohlcv_data.json")
    print("ohlcv_data.json kaydedildi ve Supabase'a yüklendi.")

if __name__ == "__main__":
    main()