import subprocess
import time
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
    if resp.status_code not in [200, 201]:
        print("Upload failed:", resp.text)

while True:
    print("\n[START] coins.py çalışıyor...")
    subprocess.call(["python", "coins.py"])
    upload_to_supabase_storage("coins.json", "coins.json")

    print("[NEXT] ohlcv_data.py çalışıyor...")
    subprocess.call(["python", "ohlcv_data.py"])
    upload_to_supabase_storage("ohlcv_data.json", "ohlcv_data.json")

    print("[NEXT] sinyal.py çalışıyor...")
    subprocess.call(["python", "sinyal.py"])
    upload_to_supabase_storage("signals.json", "signals.json")

    print("[NEXT] alarm.py çalışıyor...")
    subprocess.call(["python", "alarm.py"])
    upload_to_supabase_storage("alarm.json", "alarm.json")

    print("[LOOP] Tüm adımlar tamamlandı. 1 dakika bekleniyor...\n")
    time.sleep(60)