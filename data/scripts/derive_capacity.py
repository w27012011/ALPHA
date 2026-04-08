import json
import os

DATA_DIR = r'd:\ALPHA\data'
POP_FILE = os.path.join(DATA_DIR, 'district_population.json')
OUT_FILE = os.path.join(DATA_DIR, 'district_capacity.json')

def derive_capacity():
    if not os.path.exists(POP_FILE):
        print("Missing population data.")
        return

    with open(POP_FILE, 'r') as f:
        pop_data = json.load(f)

    capacity_map = {}
    for district, info in pop_data.items():
        pop = info.get('population', 0)
        # BGD Baseline: Roughly 1 shelter per 10,000 people 
        # (Standard humanitarian cluster baseline for coastal regions)
        capacity_map[district] = {
            "shelter_count": max(1, int(pop / 10000)),
            "medical_units": max(1, int(pop / 50000)),
            "emergency_stock_tonnes": max(1, int(pop / 100000)),
            "division": info.get('division')
        }

    with open(OUT_FILE, 'w') as f:
        json.dump(capacity_map, f, indent=2)
    
    print(f"Generated capacity metrics for {len(capacity_map)} districts.")

if __name__ == "__main__":
    derive_capacity()
