import json
import pandas as pd
from datetime import datetime

import requests

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

def detect_fvg(ohlcv):
    df = pd.DataFrame(ohlcv)
    results = []
    for i in range(2, len(df)):
        if df.loc[i-2, "high"] < df.loc[i, "low"]:
            results.append({"index": int(i), "type": "bullish", "gap": [float(df.loc[i-2, "high"]), float(df.loc[i, "low"])]})
        elif df.loc[i-2, "low"] > df.loc[i, "high"]:
            results.append({"index": int(i), "type": "bearish", "gap": [float(df.loc[i, "high"]), float(df.loc[i-2, "low"])]})
    return results

def detect_bos(ohlcv, lookback=20):
    df = pd.DataFrame(ohlcv)
    results = []
    for i in range(lookback, len(df)):
        local_high = max(df["high"][i-lookback:i])
        local_low = min(df["low"][i-lookback:i])
        if df.loc[i, "high"] > local_high:
            results.append({"index": int(i), "type": "BOS_up", "level": float(df.loc[i, "high"])})
        if df.loc[i, "low"] < local_low:
            results.append({"index": int(i), "type": "BOS_down", "level": float(df.loc[i, "low"])})
    return results

def detect_choch(bos_signals):
    results = []
    if len(bos_signals) < 2: return results
    last = bos_signals[-1]
    prev = bos_signals[-2]
    if last["type"] != prev["type"]:
        results.append({
            "index": last["index"],
            "type": "CHoCH",
            "from": prev["type"],
            "to": last["type"],
            "level": last["level"]
        })
    return results

def compute_rsi(ohlcv, period=14):
    closes = [x["close"] for x in ohlcv]
    df = pd.DataFrame({"close": closes})
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    df["rsi"] = rsi
    return df["rsi"].tolist()

def main():
    with open("ohlcv_data.json", "r") as f:
        raw = json.load(f)
    data = raw["data"]
    all_signals = {"last_update": datetime.now().isoformat(), "signals": {}}
    for symbol, intervals in data.items():
        all_signals["signals"][symbol] = {}
        for interval, ohlcv in intervals.items():
            if len(ohlcv) < 20: continue
            fvg_signals = detect_fvg(ohlcv)
            bos_signals = detect_bos(ohlcv)
            choch_signals = detect_choch(bos_signals)
            rsi_values = compute_rsi(ohlcv)
            last_rsi = rsi_values[-1] if len(rsi_values) > 0 else None
            all_signals["signals"][symbol][interval] = {
                "FVG": fvg_signals[-3:],
                "BOS": bos_signals[-3:],
                "CHoCH": choch_signals,
                "RSI": last_rsi
            }
    with open("signals.json", "w", encoding="utf-8") as f:
        json.dump(all_signals, f, ensure_ascii=False, indent=2)
    upload_to_supabase_storage("signals.json", "signals.json")
    print("signals.json kaydedildi ve Supabase'a yüklendi.")

if __name__ == "__main__":
    main()