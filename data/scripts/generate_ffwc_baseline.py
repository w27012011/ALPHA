"""
generate_ffwc_baseline.py - PROJECT ALPHA
Creates the high-fidelity backup for M-01 (FFWC)
"""

import json
import os

# FFWC Official Danger Levels (DL) and Historic Averages (m)
STATIONS = {
    "Bahadurabad": {"river": "Jamuna", "dl": 19.50, "avg": 18.20},
    "Aricha": {"river": "Padma", "dl": 9.40, "avg": 8.10},
    "Hardinge Bridge": {"river": "Ganges", "dl": 14.25, "avg": 12.80},
    "Goalunda": {"river": "Padma", "dl": 8.65, "avg": 7.50},
    "Bhagyakul": {"river": "Padma", "dl": 6.30, "avg": 5.20},
    "Dhaka": {"river": "Buriganga", "dl": 6.00, "avg": 4.50},
    "Demra": {"river": "Lakhya", "dl": 5.75, "avg": 4.80},
    "Narayanganj": {"river": "Lakhya", "dl": 5.50, "avg": 4.20},
    "Chandpur": {"river": "Meghna", "dl": 4.00, "avg": 3.10},
    "Sylhet": {"river": "Surma", "dl": 11.25, "avg": 9.80},
    "Kanairghat": {"river": "Surma", "dl": 13.20, "avg": 12.10},
    "Sunamganj": {"river": "Surma", "dl": 8.25, "avg": 7.30},
    "Chhatak": {"river": "Surma", "dl": 8.13, "avg": 7.00},
    "Sheola": {"river": "Kushiyara", "dl": 13.05, "avg": 11.50},
    "Sherpur-Sylhet": {"river": "Kushiyara", "dl": 9.00, "avg": 7.80}
}

def generate():
    # Corrected Path: script is in /data/scripts/, so go up two levels to /ALPHA/, then into /data/
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    FILE_PATH = os.path.join(DATA_DIR, 'ffwc_high_fidelity_baseline.json')
    
    # Enrich with some "High Fidelity" simulation jitter
    baseline = {}
    for name, info in STATIONS.items():
        baseline[name] = {
            "river": info["river"],
            "danger_level": info["dl"],
            "water_level": info["avg"], # Static baseline
            "status": "NORMAL",
            "source": "OFFLINE_ARCHIVE"
        }
    
    with open(FILE_PATH, 'w') as f:
        json.dump(baseline, f, indent=4)
    
    print(f"Success: High-Fidelity FFWC Baseline created at {FILE_PATH}")
    print(f"Total Stations: {len(baseline)}")

if __name__ == "__main__":
    generate()
