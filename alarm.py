import json
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

def alarm_bot():
    with open("signals.json", "r", encoding="utf-8") as f:
        signals = json.load(f)["signals"]
    with open("ohlcv_data.json", "r", encoding="utf-8") as f:
        ohlcv_data = json.load(f)["data"]
    alarms = {"last_update": datetime.now().isoformat(), "alarms": []}

    # 1. KURAL: 4h BOS_up + 30m CHoCH → SHORT
    for symbol in signals:
        try:
            if "4h" not in signals[symbol] or "30m" not in signals[symbol]:
                continue
            bos_4h = signals[symbol]["4h"]["BOS"]
            choc_30m = signals[symbol]["30m"]["CHoCH"]
            bos_up_4h = None
            for bos in reversed(bos_4h):
                if bos["type"] == "BOS_up":
                    bos_up_4h = bos
                    break
            choc_last20 = [c for c in choc_30m if c["index"] >= 55]
            if bos_up_4h and len(choc_last20) > 0:
                alarms["alarms"].append({
                    "type": "SHORT",
                    "symbol": symbol,
                    "bos_4h": bos_up_4h,
                    "choc_30m": choc_last20,
                    "rule": "4h BOS_up + 30m last 20 CHoCH (Short Signal)"
                })
        except: continue

    # 2. KURAL: BOS equilibrium → LONG
    for symbol in signals:
        for interval in signals[symbol]:
            try:
                bos_list = signals[symbol][interval]["BOS"]
                ohlcv = ohlcv_data[symbol][interval]
                if len(bos_list) == 0 or len(ohlcv) == 0: continue
                for bos in reversed(bos_list):
                    if bos["type"] != "BOS_up": continue
                    bos_idx = bos["index"]
                    bos_level = bos["level"]
                    if bos_idx < 1: continue
                    lows = [ohlcv[i]["low"] for i in range(bos_idx)]
                    dip_idx = lows.index(min(lows))
                    dip_price = lows[dip_idx]
                    alarm_level = (dip_price + bos_level) / 2
                    last_close = ohlcv[-1]["close"]
                    if last_close < alarm_level:
                        alarms["alarms"].append({
                            "type": "LONG",
                            "symbol": symbol,
                            "interval": interval,
                            "bos_idx": bos_idx,
                            "bos_level": bos_level,
                            "dip_idx": dip_idx,
                            "dip_price": dip_price,
                            "alarm_level": alarm_level,
                            "current_price": last_close,
                            "rule": "BOS_up equilibrium long alarm"
                        })
                    break
            except: continue

    with open("alarm.json", "w", encoding="utf-8") as f:
        json.dump(alarms, f, ensure_ascii=False, indent=2)
    upload_to_supabase_storage("alarm.json", "alarm.json")
    print("alarm.json kaydedildi ve Supabase'a yüklendi.")

if __name__ == "__main__":
    alarm_bot()