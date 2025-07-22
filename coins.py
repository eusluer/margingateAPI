import requests
import pandas as pd
import json
import time
from datetime import datetime

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
    while True:
        print(f"\n[{datetime.now()}] --- Başladı ---")
        coins = get_top_volatile_perpetual_symbols(50)
        save_to_json(coins)
        print(f"{len(coins)} adet coin coins.json'a kaydedildi.")
        print("1 dakika bekleniyor...\n")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()