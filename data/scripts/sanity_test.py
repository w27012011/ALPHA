import sqlite3
import time
import subprocess
import os
import sys
import json

DB_PATH = r'd:\ALPHA\data\db\alpha_db.sqlite'
ACTIVE_FEED = r'd:\ALPHA\data\active_feed\M-01_payload.json'

def check_db(topic):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM message_bus WHERE topic = ?", (topic,))
    count = cur.fetchone()[0]
    conn.close()
    return count

def run_test():
    print("--- PROJECT ALPHA SYSTEM SANITY TEST ---")
    
    # 1. Verify Feed
    if not os.path.exists(ACTIVE_FEED):
        print("[FAIL] M-01 Feed not found. Run data_systemd.py first.")
        return
    print("[OK] M-01 Live Feed detected.")

    # 2. Start Modules in order
    # Mapping paths to module names for 'python -m'
    modules = [
        "modules.m_01_conn_ffwc",
        "modules.m_06_hydro_preproc",
        "modules.m_07_hydro_forecast"
    ]
    
    procs = []
    try:
        for mod in modules:
            print(f"[SHIPPING] Starting python -m {mod}...")
            # Use -m to resolve packages correctly
            p = subprocess.Popen([sys.executable, "-m", mod], 
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               cwd="d:/ALPHA") # Must run from root
            procs.append(p)
            time.sleep(5) 

        print("[WAITING] Allowing pipeline to propagate (30s)...")
        time.sleep(30)
        
        # 3. Assertions
        topics = ["raw.water_levels", "hydro.water_normalized", "hydro.flood_raw"]
        all_ok = True
        for t in topics:
            count = check_db(t)
            if count > 0:
                print(f"[PASS] Topic '{t}': {count} messages found.")
            else:
                print(f"[FAIL] Topic '{t}': NO MESSAGES FOUND.")
                all_ok = False
        
        if all_ok:
            print("\n[RESULT] SYSTEM IS FULLY OPERATIONAL!")
        else:
            print("\n[RESULT] SYSTEM HAS PIPELINE BLOCKAGES.")

    finally:
        for p in procs:
            p.terminate()

if __name__ == "__main__":
    run_test()
