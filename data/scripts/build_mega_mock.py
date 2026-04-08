import os
import json
from datetime import datetime, timezone, timedelta

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CACHE_DIR = os.path.join(ROOT_DIR, "modules", "cache")
ARCHIVE_DIR = os.path.join(ROOT_DIR, "data", "archive")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)

def build_mega_mock():
    print("Initiating Omega-Class Disaster Mock Generator...")
    now_iso = datetime.now(timezone.utc).isoformat() + "Z"
    
    # 1. M-01 FFWC Water Levels (Massive Flooding)
    # Generate 15 stations all 3 meters above danger level
    ffwc_data = []
    stations = ["SW99", "SW90", "SW20", "BR11", "DH01", "SY44", "CH99", "RJ22", "KH33"]
    for st in stations:
        ffwc_data.append({
            "station_id": st,
            "district": "MockDist",
            "water_level_m": 25.5,
            "danger_level_m": 19.5,
            "data_source": "FFWC_API_MOCK",
            "record_time": now_iso,
            "timestamp": now_iso
        })
    with open(os.path.join(CACHE_DIR, "M-01_ffwc_last.json"), "w") as f:
        json.dump(ffwc_data, f)
        
    # 2. M-02 ERA5 Weather (Cyclone conditions: extreme rain, low press, high wind)
    era5_data = []
    # Grid covering Bangladesh roughly 20-26 N, 88-92 E
    import numpy as np
    for lat in np.arange(21.0, 26.0, 0.5):
        for lon in np.arange(88.0, 92.5, 0.5):
            era5_data.append({
                "grid_id": f"{lat:.1f}_{lon:.1f}",
                "lat": float(lat),
                "lon": float(lon),
                "temperature_2m_K": 298.15,
                "total_precipitation_m": 0.85, # 850 mm of rain VERY EXTREME
                "surface_pressure_Pa": 92000, # 920 hPa Cyclone eye
                "10m_u_component_of_wind_m_s": 35.0, # ~120 km/h
                "10m_v_component_of_wind_m_s": 35.0,
                "valid_time": now_iso,
                "data_source": "ERA5_MOCK",
                "timestamp": now_iso
            })
    with open(os.path.join(CACHE_DIR, "M-02_era5_last.json"), "w") as f:
        json.dump(era5_data, f)
        
    # 3. M-03 USGS Earthquake (Magnitude 8.2 in Sylhet)
    usgs_event = {
        "event_id": "usgs_mega_mock_8_2",
        "magnitude": 8.2,
        "depth_km": 15.0,
        "lat": 24.89,  # Sylhet
        "lon": 91.87,
        "place_description": "2km E of Sylhet, Bangladesh",
        "event_time": now_iso,
        "distance_to_bangladesh_km": 0.0,
        "felt_in_bangladesh": True,
        "pga_estimate_g": 0.85, # Critical structural damage
        "data_source": "USGS_MOCK",
        "timestamp": now_iso
    }
    # For M-03 fallback we write to both possible locations
    with open(os.path.join(CACHE_DIR, "M-03_last_events.json"), "w") as f:
        json.dump([usgs_event], f)
    with open(os.path.join(ARCHIVE_DIR, "M-03_demo.json"), "w") as f:
        json.dump(usgs_event, f)
        
    # 4. M-04 MODIS/Sentinel (Huge Crop Damage & Displacement)
    ndvi_data = {
        "product": "MOD13Q1 MOCK",
        "composite_start_date": now_iso,
        "composite_end_date": now_iso,
        "total_cells": len(era5_data),
        "cloud_cover_pct": 95.0,
        "data_source": "MODIS_MOCK",
        "timestamp": now_iso,
        "grid_cells": [
            {"grid_id": f"{lat:.1f}_{lon:.1f}", "lat": lat, "lon": lon, "district": "BD-Mock", "ndvi": 0.15, "pixel_reliability": 0, "evi": 0.1}
            for lat, lon in [(c["lat"], c["lon"]) for c in era5_data]
        ]
    }
    with open(os.path.join(CACHE_DIR, "M-04_ndvi_last.json"), "w") as f:
        json.dump(ndvi_data, f)
        
    sar_data = {
        "product": "S1A MOCK",
        "acquisition_date": now_iso,
        "reference_date": now_iso,
        "total_segments": 5,
        "data_source": "SENTINEL_MOCK",
        "timestamp": now_iso,
        "segments": [
            {"segment_id": f"SEG-00{i}", "lat_start": 24.0, "lon_start": 90.0, "lat_end": 24.5, "lon_end": 90.5, "district": "Sylhet", "displacement_mm": -150.5, "coherence": 0.45, "look_angle_deg": 35.0} # 15 cm drop!
            for i in range(1, 6)
        ]
    }
    with open(os.path.join(CACHE_DIR, "M-04_sar_last.json"), "w") as f:
        json.dump(sar_data, f)

    # 5. M-05 EIA Reserve (Empty fuel tanks)
    eia_data = {
        "series_id": "PET.RWTC.D_MOCK",
        "inventory_name": "Bangladesh Strategic Reserve",
        "total_bbl": 15000, # Almost empty
        "daily_consumption_bbl": 90000, 
        "supply_days_remaining": 0.16, # Critical
        "unit": "Thousand Barrels",
        "date_effective": now_iso,
        "data_source": "EIA_MOCK",
        "timestamp": now_iso
    }
    with open(os.path.join(CACHE_DIR, "M-05_eia_last.json"), "w") as f:
        json.dump(eia_data, f)

    print("Success: Generated Omega-Class Catastrophe across all 5 Input Sensor caches.")
    print("When the orchestrator boots offline, it will ingest this data and trigger a massive system-wide alert cascade.")

if __name__ == "__main__":
    build_mega_mock()
