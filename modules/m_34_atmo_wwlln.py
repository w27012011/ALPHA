"""
M-34 ATMO-WWLLN
WWLLN Lightning Data Processor
"""

import os
import json
import statistics
from collections import deque
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

def point_in_polygon(x, y, polygon):
    # Ray-casting algorithm for testing whether a point overlaps a polygon
    # polygon is a list of [lon, lat] pairs
    n = len(polygon)
    inside = False
    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside

class AtmoWwlln(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-34",
            input_topics=["raw.weather_fields", "atmo.weather_processed", "atmo.cape_index"],
            output_topics=["atmo.lightning_processed"]
        )
        self.weather = {}
        self.cape = {}
        self.strike_history = {} # district -> deque
        
        self.areas = self._load("district_area.json")
        self.d_polys = {}
        
        try:
            with open(os.path.join(DATA_DIR, "district_boundaries.geojson"), "r") as f:
                geo = json.load(f)
                for feat in geo.get("features", []):
                    dc = feat.get("properties", {}).get("district_code")
                    coords = feat.get("geometry", {}).get("coordinates", [])
                    if dc and coords:
                        # GeoJSON polygon is usually lists of lists of coords [ [[[lon, lat]...]] ]
                        self.d_polys[dc] = coords[0]
        except Exception as e:
            self.logger.error(f"BOUNDARY_DATA_MISSING: {e}")

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        dc = message.get("district_code")
        if topic == "atmo.weather_processed":
            if dc: self.weather[dc] = message
            return
            
        if topic == "atmo.cape_index":
            if dc: self.cape[dc] = message
            return
            
        if topic == "raw.weather_fields":
            self._process_wwlln(message)

    def _process_wwlln(self, wwlln_msg):
        strikes = wwlln_msg.get("wwlln_strikes_last_6h")
        has_feed = True
        if strikes is None:
            self.logger.warning("WWLLN_UNAVAILABLE")
            has_feed = False
            strikes = []
            
        now_ts = datetime.now(timezone.utc)
        dist_strikes = {}
        
        if has_feed:
            for s in strikes:
                res = s.get("residual_us", 0.0)
                if res > 30.0: continue
                
                lat = s.get("strike_lat")
                lon = s.get("strike_lon")
                if lat is None or lon is None: continue
                
                # Assign to district
                assigned_d = None
                for dcode, poly in self.d_polys.items():
                    if point_in_polygon(lon, lat, poly):
                        assigned_d = dcode
                        break
                        
                if assigned_d:
                    if assigned_d not in dist_strikes:
                        dist_strikes[assigned_d] = []
                    dist_strikes[assigned_d].append((lat, lon))

        # We must iterate over known districts even if no strikes hit them, or use the baseline areas keys
        for d_code in self.areas.keys():
            s_arr = dist_strikes.get(d_code, [])
            
            scount = None
            dens = None
            c_lat = None
            c_lon = None
            
            if has_feed:
                scount = len(s_arr)
                a = max(1.0, float(self.areas.get(d_code, 1000.0)))
                dens = float(scount) / a
                
                if scount > 0:
                    c_lat = statistics.mean([s[0] for s in s_arr])
                    c_lon = statistics.mean([s[1] for s in s_arr])
                    
                if d_code not in self.strike_history:
                    self.strike_history[d_code] = deque(maxlen=5)
                self.strike_history[d_code].append(dens)

            # Trend logic
            tr = "NO_DATA"
            if has_feed and d_code in self.strike_history and len(self.strike_history[d_code]) >= 2:
                hx = list(self.strike_history[d_code])
                rec_3h = statistics.mean(hx[-2:]) if len(hx) >= 2 else hx[-1]
                prev_3h = hx[-3] if len(hx) >= 3 else hx[-2] # rough approx since actual window is just a queue check
                
                if rec_3h > prev_3h * 1.3:
                    tr = "INCREASING"
                elif rec_3h < prev_3h * 0.7:
                    tr = "DECREASING"
                else:
                    tr = "STABLE"

            kalb = False
            w_msg = self.weather.get(d_code, {})
            c_msg = self.cape.get(d_code, {})
            
            cape_v = c_msg.get("CAPE_J_per_kg")
            wsh = w_msg.get("wind_shear_ms_per_km")
            
            if cape_v is not None and wsh is not None and has_feed:
                if float(cape_v) > 2000.0 and float(wsh) > 10.0 and tr == "INCREASING":
                    kalb = True
                    
            out = {
                "district_code": d_code,
                "district_name": d_code, # we map name downstream or assume d_code usually
                "strike_count_last_6h": scount,
                "strike_density_per_km2": float(dens) if dens is not None else None,
                "cluster_centroid_lat": float(c_lat) if c_lat is not None else None,
                "cluster_centroid_lon": float(c_lon) if c_lon is not None else None,
                "trend": tr,
                "kalbaishakhi_signature_detected": kalb,
                "wind_shear_ms_per_km": float(wsh) if wsh is not None else None,
                "CAPE_J_per_kg": float(cape_v) if cape_v is not None else None,
                "timestamp": now_ts.isoformat().replace("+00:00", "Z")
            }
            self.publish("atmo.lightning_processed", out)

if __name__ == "__main__":
    mod = AtmoWwlln()
    mod.start()
