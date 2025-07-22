import requests
import json
from datetime import datetime
import time

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
            continue  # Tekrarlı veri varsa atla
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
    try:
        with open("ohlcv_data.json", "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"Veriler kaydedildi! {datetime.now()}")
    except Exception as e:
        print(f"Kaydetme hatası: {e}")

if __name__ == "__main__":
    while True:
        print(f"\n[{datetime.now()}] OHLCV çekme işlemi başladı...")
        run_ohlcv_job()
        print("[OHLCV JOB] 1 dakika bekleniyor...\n")
        time.sleep(60)