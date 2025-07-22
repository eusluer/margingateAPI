import subprocess
import time
import requests
import os

SUPABASE_URL = "https://muwqydzmponlsoagasnw.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
BUCKET = "signals"

def upload_to_supabase_storage(local_file, remote_name):
    if not os.path.exists(local_file):
        print(f"[SKIP] {local_file} bulunamadı, yüklenmedi.")
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

def run_script(script):
    try:
        print(f"[RUN] {script} başlatıldı...")
        subprocess.check_call(["python", script])
        print(f"[OK] {script} tamamlandı.")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {script} hata: {e}")

def upload_all_files():
    for fname in ["coins.json", "ohlcv_data.json", "signals.json", "alarm.json"]:
        upload_to_supabase_storage(fname, fname)

while True:
    print("\n[START] Döngü başladı...\n")
    run_script("coins.py")
    run_script("ohlcv_data.py")
    run_script("sinyal.py")
    run_script("alarm.py")

    print("[UPLOAD] Tüm json dosyaları Supabase'a yükleniyor...")
    upload_all_files()

    print("[LOOP] Tüm adımlar tamamlandı. 1 dakika bekleniyor...\n")
    time.sleep(60)