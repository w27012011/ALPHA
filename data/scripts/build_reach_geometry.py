import json
import os
import requests

DATA_DIR = r'd:\ALPHA\data'
OUT_FILE = os.path.join(DATA_DIR, 'reach_geometry.json')

def build():
    # Fetch current stations to get the IDs
    try:
        r = requests.get('https://api.ffwc.gov.bd/data_load/observed/', timeout=10)
        stations = r.json()
    except Exception as e:
        print(f"Error fetching stations: {e}")
        return

    reach_map = {}
    for st in stations:
        sid = str(st.get('st_id'))
        river = st.get('river', '').lower()
        
        # Roughness (n) per MDD 5.4.1
        n = 0.035 # Default secondary
        if any(r in river for r in ['brahmaputra', 'jamuna', 'ganges', 'padma', 'meghna']):
            n = 0.030
        elif 'floodplain' in river:
            n = 0.060
            
        reach_map[sid] = {
            "n": n,
            "A": 500.0,      # m^2 (Default per MDD)
            "P_w": 50.0,      # m (Default per MDD)
            "S": 0.0001,      # m/m (Delta slope)
            "L": 50000.0      # m (50km reach length)
        }
        
    with open(OUT_FILE, 'w') as f:
        json.dump(reach_map, f, indent=2)
        
    print(f"Successfully populated {len(reach_map)} reach geometries to {OUT_FILE}")

if __name__ == "__main__":
    build()
