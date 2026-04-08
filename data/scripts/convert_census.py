import csv
import json
import os

CSV_PATH = r'd:\ALPHA\bangladesh_bbs_population-and-housing-census-dataset_2022_admin-02.csv'
JSON_OUT = r'd:\ALPHA\data\district_population.json'

def process():
    if not os.path.exists(CSV_PATH):
        print(f"Error: {CSV_PATH} not found.")
        return

    population_map = {}
    
    with open(CSV_PATH, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            district = row.get('District')
            population = row.get('Population_Total')
            
            if district and population:
                try:
                    # Clean the population string (sometimes has formatting)
                    pop_val = int(population.replace(',', ''))
                    population_map[district] = {
                        "population": pop_val,
                        "division": row.get('Division'),
                        "geocode": row.get('District_Geocode')
                    }
                except ValueError:
                    continue

    with open(JSON_OUT, 'w', encoding='utf-8') as f:
        json.dump(population_map, f, indent=2)
    
    print(f"Successfully processed {len(population_map)} districts to {JSON_OUT}")

if __name__ == "__main__":
    process()
