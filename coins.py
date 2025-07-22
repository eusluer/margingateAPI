import requests
import pandas as pd
import json
import time
from datetime import datetime

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
    if resp.status_code not in [200, 201]:
        print("Upload failed:", resp.text)

INTERVAL = "4h"
LIMIT = 75

def has_sufficient_ohlcv(symbol):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={INTERVAL}&limit={LIMIT}"
    try:
        resp = requests.get(url, timeout=8)
        if resp.status_code != 200:
            return False
        data = resp.json()
        # Eğer coin delist olmuş, API bozuk dönüyor veya hiç veri yoksa
        if not isinstance(data, list) or len(data) != LIMIT:
            return False
        # Ekstra: Sıfır hacimli veya tüm mumlar sıfır mı?
        if all(float(x[5]) == 0 for x in data):
            return False
        return True
    except Exception as e:
        print(f"[WARN] {symbol} 4h mum verisi alınamadı veya coin delist edilmiş olabilir: {e}")
        return False

def get_top_volatile_perpetual_symbols(top_n=50):
    url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
    try:
        resp = requests.get(url, timeout=10).json()
    except Exception as e:
        print(f"[FATAL] Binance ticker verisi alınamadı: {e}")
        return []

    symbols = [
        x["symbol"] for x in resp
        if x["symbol"].endswith("USDT")
        and "DOWN" not in x["symbol"]
        and "UP" not in x["symbol"]
        and "_PERP" not in x["symbol"]
    ]
    df = pd.DataFrame([x for x in resp if x["symbol"] in symbols])
    df["priceChangePercent"] = df["priceChangePercent"].astype(float)
    df["volume"] = df["volume"].astype(float)
    df["lastPrice"] = df["lastPrice"].astype(float)

    # %30'dan fazla artmış/azalmış olanları çıkar
    df = df[df["priceChangePercent"].abs() <= 30]

    # Sıfırdan büyük hacmi olanları al
    df = df[df["volume"] > 0]

    # Büyükten küçüğe volatiliteye göre sırala
    df = df.reindex(df["priceChangePercent"].abs().sort_values(ascending=False).index)

    filtered_coins = []
    for _, row in df.iterrows():
        symbol = row["symbol"]
        # Coin delist mi/aktif mi/75 mum tam mı?
        if has_sufficient_ohlcv(symbol):
            filtered_coins.append({
                "symbol": symbol,
                "priceChangePercent": row["priceChangePercent"],
                "lastPrice": row["lastPrice"],
                "volume": row["volume"]
            })
            print(f"Coin uygun: {symbol} ({row['priceChangePercent']}%)")
        else:
            print(f"Yetersiz 4h veri veya delist: {symbol}")

        if len(filtered_coins) >= top_n:
            break

    return filtered_coins

def save_to_json(data, filename="coins.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump({
            "last_update": datetime.now().isoformat(),
            "coins": data
        }, f, ensure_ascii=False, indent=2)

def main_loop():
        print(f"\n[{datetime.now()}] --- Başladı ---")
        coins = get_top_volatile_perpetual_symbols(50)
        save_to_json(coins)
        print(f"{len(coins)} adet coin coins.json'a kaydedildi.")

        # Supabase Storage'a upload et
        try:
            upload_to_supabase_storage("coins.json", "coins.json")
        except Exception as e:
            print(f"Supabase upload hatası: {e}")

        print("1 dakika bekleniyor...\n")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()