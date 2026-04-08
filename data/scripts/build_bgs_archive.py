import os
import csv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ARCHIVE_DIR = os.path.join(ROOT_DIR, "data")
os.makedirs(ARCHIVE_DIR, exist_ok=True)
IN_FILE = os.path.join(ROOT_DIR, "NationalSurveyData.csv")
OUT_FILE = os.path.join(ARCHIVE_DIR, "bgs_well_dataset.csv")

def build_bgs_archive():
    print("Initiating BGS Arsenic Archive Builder...")
    if not os.path.exists(IN_FILE):
        print(f"ERROR: Could not find {IN_FILE}. Please place the BGS National Survey CSV at the root dir.")
        return
        
    print(f"Reading RAW BGS Dataset from {IN_FILE}...")
    
    rows_processed = 0
    valid_records = []
    
    try:
        with open(IN_FILE, "r") as f:
            # The BGS file has 4 lines of metadata before headers at line 5 (0-indexed line 4)
            for _ in range(4):
                next(f)
                
            reader = csv.DictReader(f)
            
            # Skip the unit line right after headers (line 6)
            next(reader) 
            
            for row in reader:
                sample_id = row.get('SAMPLE_ID')
                lat = row.get('LAT_DEG')
                lon = row.get('LONG_DEG')
                arsenic = row.get('As')
                
                # Basic validation
                if sample_id and lat and lon and arsenic:
                    valid_records.append({
                        "well_id": sample_id.strip(),
                        "lat": lat.strip(),
                        "lon": lon.strip(),
                        "arsenic_ug_l": arsenic.strip()
                    })
                    rows_processed += 1

        print(f"Extraction complete. Found {rows_processed} valid arsenic readings.")
        
        # Write to the specific system file format expected by M-11 Aqua-Kriging
        print(f"Writing parsed matrix to {OUT_FILE}...")
        with open(OUT_FILE, "w", newline="") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=["well_id", "lat", "lon", "arsenic_ug_l"])
            writer.writeheader()
            writer.writerows(valid_records)
            
        print("SUCCESS: Master BGS dataset is now armed for offline Simulation Mode.")
        
    except Exception as e:
        print(f"FAILED TO CONVERT: {e}")

if __name__ == "__main__":
    build_bgs_archive()
