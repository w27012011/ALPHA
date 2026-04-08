import csv
import json
import os

CSV_PATH = r'd:\ALPHA\NationalSurveyData.csv'
JSON_OUT = r'd:\ALPHA\data\arsenic_risk.json'

def process():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found.")
        return

    district_data = {}
    
    with open(CSV_PATH, mode='r', encoding='utf-8') as f:
        # Skip the first 4 lines of metadata
        for _ in range(4):
            next(f)
        
        reader = csv.DictReader(f)
        for row in reader:
            district = row.get('DISTRICT')
            arsenic_str = row.get('As')
            
            if district and arsenic_str:
                # Handle "< 6" or other non-numeric strings
                try:
                    if arsenic_str.startswith('<'):
                        arsenic_val = float(arsenic_str.replace('<', '').strip()) / 2.0 # Half the detection limit
                    else:
                        arsenic_val = float(arsenic_str)
                    
                    if district not in district_data:
                        district_data[district] = []
                    district_data[district].append(arsenic_val)
                except ValueError:
                    continue

    # Summarize by district
    risk_map = {}
    for district, values in district_data.items():
        avg_as = sum(values) / len(values)
        max_as = max(values)
        risk_level = "LOW"
        if avg_as > 50:
            risk_level = "CRITICAL"
        elif avg_as > 10:
            risk_level = "MODERATE"
            
        risk_map[district] = {
            "avg_arsenic_ugl": round(avg_as, 2),
            "max_arsenic_ugl": round(max_as, 2),
            "risk_level": risk_level,
            "sample_count": len(values)
        }

    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(risk_map, f, indent=2)
    
    print(f"Successfully processed {len(risk_map)} districts to {JSON_OUT}")

if __name__ == "__main__":
    process()
