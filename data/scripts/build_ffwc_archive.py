import os
import csv
import json
from datetime import datetime, timezone

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ARCHIVE_DIR = os.path.join(ROOT_DIR, "data", "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
IN_FILE = os.path.join(ROOT_DIR, "data", "scripts", "historical_water_level_template.csv")
OUT_FILE = os.path.join(ARCHIVE_DIR, "M-01_demo.json")

def build_ffwc_archive():
    print("Initiating FFWC Historical Archive Builder...")
    if not os.path.exists(IN_FILE):
        print(f"ERROR: Could not find {IN_FILE}. Please fill in the CSV template.")
        return
        
    data = []
    try:
        with open(IN_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Basic parsing protecting against empty strings
                wl = float(row['water_level_m']) if row['water_level_m'] else None
                dl = float(row['danger_level_m']) if row['danger_level_m'] else None
                data.append({
                    "station_id": row['station_id'],
                    "station_name": row['station_name'],
                    "river": row['river'],
                    "lat": float(row['lat']) if row['lat'] else None,
                    "lon": float(row['lon']) if row['lon'] else None,
                    "district": row['district'],
                    "water_level_m": wl,
                    "danger_level_m": dl,
                    "trend": row['trend'],
                    "data_source": "ARCHIVE_BUILDER",
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
                })
                
        with open(OUT_FILE, "w") as out_f:
            json.dump(data, out_f, indent=4)
            
        print(f"SUCCESS: Converted CSV template into FFWC offline archive format.")
        print(f"Saved to {OUT_FILE}")
    except Exception as e:
        print(f"FAILED TO CONVERT: {e}")

if __name__ == "__main__":
    build_ffwc_archive()
