import pandas as pd
import json
import os

def verify_bgs_data():
    csv_path = r'd:\ALPHA\data\bgs_well_dataset.csv'
    output_path = r'd:\ALPHA\data\arsenic_points.json'
    
    print(f"[*] Starting BGS Verification: {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"[!] Error: CSV file not found at {csv_path}")
        return

    try:
        # Load the dataset
        df = pd.read_csv(csv_path)
        print(f"[+] Loaded {len(df)} raw entries.")

        valid_points = []
        for index, row in df.iterrows():
            try:
                # M-87: Strict matching with BGS CSV Schema (Lat_dec, Long_dec)
                lat = float(row['Lat_dec'])
                lon = float(row['Long_dec'])
                
                # BGS Data cleaning for arsenic value (As_ug_l)
                raw_val = str(row['As_ug_l']).replace('<', '').replace('>', '').strip()
                val = float(raw_val)
                
                # Geographic validation (Bangladesh bounds approx)
                if 20.0 <= lat <= 27.0 and 88.0 <= lon <= 93.0:
                    valid_points.append([lat, lon, val])
            except (ValueError, KeyError):
                continue
        
        print(f"[+] Validation complete. Found {len(valid_points)} valid geo-referenced points.")
        
        # Save as optimized JSON for Leaflet
        with open(output_path, 'w') as f:
            json.dump(valid_points, f)
            
        print(f"[+] Successfully saved {output_path} ({os.path.getsize(output_path)} bytes)")

    except Exception as e:
        print(f"[!] Critical Error: {str(e)}")

if __name__ == "__main__":
    verify_bgs_data()
