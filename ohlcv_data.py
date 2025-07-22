import requests
import json
import time
import os
from datetime import datetime

SUPABASE_URL = "https://muwqydzmponlsoagasnw.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im11d3F5ZHptcG9ubHNvYWdhc253Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MzIwODM3MywiZXhwIjoyMDY4Nzg0MzczfQ.1l5Uiy760Z2CQAY_pO4dcwIGKY59u5R3OBQ2I-F12Ck"
BUCKET = "signals"

def upload_to_supabase_storage(local_file, remote_name):
    if not os.path.exists(local_file):
        print(f"[SUPABASE] {local_file} bulunamadı, yüklenmedi.")
        return
    with open(local_file, "rb") as f:
        file_data = f.read()
    endpoint = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{remote_name}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }
    try:
        resp = requests.put(endpoint, headers=headers, data=file_data)
        print(f"[SUPABASE] {remote_name} upload status: {resp.status_code}")
        if resp.status_code not in [200, 201]:
            print("Upload failed:", resp.text)
    except Exception as e:
        print(f"[SUPABASE] Upload error for {remote_name}: {e}")

INTERVALS = ["4h", "2h", "30m", "15m"]
LIMIT = 75

def fetch_ohlcv(symbol, interval, limit=LIMIT):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[HATA] {symbol} [{interval}] veri çekilemedi: {e}")
        return []
    ohlcv = []
    seen = set()
    for kline in data:
        key = (kline[0], kline[6])  # open_time, close_time
        if key in seen:
            continue
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
        except Exception as ex:
            print(f"[HATA] {symbol} [{interval}] satır hatası: {ex}")
    return ohlcv

def run_ohlcv_job():
    try:
        with open("coins.json", "r", encoding="utf-8") as f:
            coins = json.load(f)["coins"]
    except Exception as e:
        print(f"coins.json okunamadı: {e}")
        return

    all_data = {
        "last_update": datetime.now().isoformat(),
        "data": {}
    }

    for idx, coin in enumerate(coins, 1):
        symbol = coin["symbol"]
        all_data["data"][symbol] = {}
        for interval in INTERVALS:
            print(f"[{idx}/{len(coins)}] Çekiliyor: {symbol} [{interval}]")
            ohlcv = fetch_ohlcv(symbol, interval)
            if len(ohlcv) < LIMIT:
                print(f"UYARI: {symbol} [{interval}] için {len(ohlcv)} mum çekilebildi (beklenen: {LIMIT})")
            else:
                print(f"--> {symbol} [{interval}]: {len(ohlcv)} benzersiz mum çekildi.")
            all_data["data"][symbol][interval] = ohlcv
            time.sleep(0.4)  # API limit için küçük bekleme

    # JSON dosyasını hem local'e yaz hem Supabase'a upload et
    try:
        local_filename = "ohlcv_data.json"
        with open(local_filename, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"Veriler kaydedildi! {datetime.now()}")
        upload_to_supabase_storage(local_filename, local_filename)
    except Exception as e:
        print(f"Kaydetme veya upload hatası: {e}")

if __name__ == "__main__":
        print(f"\n[{datetime.now()}] OHLCV çekme işlemi başladı...")
        run_ohlcv_job()
        print("[OHLCV JOB] 1 dakika bekleniyor...\n")
        time.sleep(60)