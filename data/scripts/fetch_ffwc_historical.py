import json
import os
import requests
import time

DATA_DIR = r'd:\ALPHA\data'
OUT_FILE = os.path.join(DATA_DIR, 'hps_historical_baseline.json')

def fetch_historical_baselines():
    # 1. Fetch Station List
    print("Fetching FFWC Station List...")
    try:
        r = requests.get('https://api.ffwc.gov.bd/data_load/stations-2025/', timeout=15)
        stations = r.json()
    except Exception as e:
        print(f"Failed to fetch stations: {e}")
        return

    # 2. Iterate and Fetch 2024 Baselines for a representative sample
    # Using the numeric 'id' field as required by the API.
    baseline_map = {}
    sample_size = 15
    sample_stations = stations[:sample_size]
    
    print(f"Fetching 2024 Baselines for {len(sample_stations)} sample stations...")
    
    for st in sample_stations:
        sid = st.get('id')  # Numeric ID required for observed-waterlevel-sum endpoint
        st_label = st.get('st_id')
        if not sid: continue
        
        try:
            # Fetching the monthly observed sums for 2024
            url = f"https://api.ffwc.gov.bd/data_load/observed-waterlevel-sum-by-station-and-year/{sid}/2024/"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                baseline_map[str(st_label)] = {
                    "monthly_profile": data,
                    "danger_level": st.get("dl"),
                    "highest_historical": st.get("rhwl"),
                    "numeric_id": sid
                }
            print(f"Synced station {st_label}")
            time.sleep(0.5) 
        except Exception as e:
            print(f"Skipping {st_label}: {e}")
            continue
            
    with open(OUT_FILE, 'w') as f:
        json.dump(baseline_map, f, indent=2)
    
    print(f"Successfully populated {len(baseline_map)} station baselines to {OUT_FILE}")

if __name__ == "__main__":
    fetch_historical_baselines()
