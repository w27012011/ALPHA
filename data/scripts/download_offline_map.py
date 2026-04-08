import os
import urllib.request
import json

PUBLIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "public")
LIB_DIR = os.path.join(PUBLIC_DIR, "lib")
os.makedirs(LIB_DIR, exist_ok=True)

def download_offline_map():
    print("Initiating 100% Offline Map Asset Download Route...")
    
    # 1. Download Leaflet.js
    leaflet_js_url = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    leaflet_js_path = os.path.join(LIB_DIR, "leaflet.js")
    print(f"Downloading Leaflet.js... -> {leaflet_js_path}")
    urllib.request.urlretrieve(leaflet_js_url, leaflet_js_path)
    
    # 2. Download Leaflet CSS
    leaflet_css_url = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    leaflet_css_path = os.path.join(LIB_DIR, "leaflet.css")
    print(f"Downloading Leaflet.css... -> {leaflet_css_path}")
    urllib.request.urlretrieve(leaflet_css_url, leaflet_css_path)

    # 3. Download Bangladesh GeoJSON (High Quality Polygon)
    # Using a reliable public geojson repository for global boundaries
    bd_geojson_url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
    bd_geojson_path = os.path.join(PUBLIC_DIR, "bangladesh.geojson")
    print(f"Downloading Global GeoJSON to extract Bangladesh... -> {bd_geojson_url}")
    
    req = urllib.request.Request(bd_geojson_url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
        
        # Filter for just Bangladesh to save client processing power
        bangladesh_feature = None
        for feature in data["features"]:
            if feature["properties"]["ADMIN"] == "Bangladesh" or feature["properties"]["ISO_A3"] == "BGD":
                bangladesh_feature = feature
                break
                
        if bangladesh_feature:
            with open(bd_geojson_path, "w") as f:
                json.dump({"type": "FeatureCollection", "features": [bangladesh_feature]}, f)
            print(f"Successfully extracted and saved Bangladesh boundaries directly to {bd_geojson_path}")
        else:
            print("ERROR: Could not find BGD in global map set.")

    print("Success: All map assets are now physically locked on the pendrive for offline usage.")

if __name__ == "__main__":
    download_offline_map()
