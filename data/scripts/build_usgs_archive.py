import os
import json
import urllib.request
import urllib.parse
from datetime import datetime, timezone

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ARCHIVE_DIR = os.path.join(ROOT_DIR, "data", "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
OUT_FILE = os.path.join(ARCHIVE_DIR, "M-03_demo.json")

def build_usgs_archive():
    print("Initiating USGS Historical Archive Builder...")
    # Target: The deeply destructive 2016 Imphal Earthquakes or general historical data
    # Box matching Bangladesh broadly: lat [20.0, 27.0], lon [88.0, 93.0]
    
    start_time = "2016-01-01T00:00:00"
    end_time = "2016-12-31T23:59:59"
    
    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": 5.0, # Grab serious historical quakes
        "minlatitude": 20.0,
        "maxlatitude": 27.0,
        "minlongitude": 88.0,
        "maxlongitude": 93.0
    }
    
    query_string = urllib.parse.urlencode(params)
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?{query_string}"
    
    print(f"Requesting data from: {url}")
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                with open(OUT_FILE, "w") as f:
                    json.dump(data, f, indent=4)
                print(f"SUCCESS: Downloaded {len(data.get('features', []))} historical earthquakes.")
                print(f"Saved to: {OUT_FILE}")
            else:
                print(f"HTTP ERROR: {response.status}")
    except Exception as e:
        print(f"FETCH FAILED: {e}")

if __name__ == "__main__":
    build_usgs_archive()
