import subprocess
import time

while True:
    print("\n[START] coins.py çalışıyor...")
    subprocess.call(["python", "coins.py"])

    print("[NEXT] ohlcv_data.py çalışıyor...")
    subprocess.call(["python", "ohlcv_data.py"])

    print("[NEXT] sinyal.py çalışıyor...")
    subprocess.call(["python", "sinyal.py"])

    print("[NEXT] alarm.py çalışıyor...")
    subprocess.call(["python", "alarm.py"])

    print("[LOOP] Tüm adımlar tamamlandı. 1 dakika bekleniyor...\n")
    time.sleep(60)  # Her döngü arasında 1 dakika bekle