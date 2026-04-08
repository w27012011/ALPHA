import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LIB_DIR = os.path.join(ROOT_DIR, "libs")
sys.path.insert(0, LIB_DIR)

ARCHIVE_DIR = os.path.join(ROOT_DIR, "data", "archive")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
OUT_FILE = os.path.join(ARCHIVE_DIR, "era5_amphan.grib")

def load_env():
    env_path = os.path.join(ROOT_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    k, v = line.strip().split("=", 1)
                    os.environ[k] = v

def build_era5_archive():
    print("Initiating Copernicus Historical Archive Builder (Cyclone Amphan, May 2020)...")
    load_env()
    
    api_key = os.environ.get("COPERNICUS_API_KEY")
    if not api_key:
        print("ERROR: COPERNICUS_API_KEY not found in d:\\ALPHA\\.env")
        return
        
    try:
        import cdsapi
        client = cdsapi.Client(url=os.environ.get("COPERNICUS_API_URL", "https://cds.climate.copernicus.eu/api"), key=api_key)
        
        print(f"Requesting Amphan datasets... saving to {OUT_FILE}")
        client.retrieve(
            "reanalysis-era5-land",
            {
                "variable": ["2m_temperature", "total_precipitation", "surface_pressure"],
                "year": "2020",
                "month": "05",
                "day": ["19", "20", "21"],
                "time": ["00:00", "06:00", "12:00", "18:00"],
                "area": [26.63, 88.01, 20.74, 92.67], # BD Bounding Box
                "format": "grib"
            },
            OUT_FILE
        )
        print("SUCCESS: File successfully archived.")
    except Exception as e:
        print(f"FETCH FAILED: {e}")

if __name__ == "__main__":
    build_era5_archive()
