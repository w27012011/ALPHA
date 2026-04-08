"""
M-26 GEO-SEISMIC
Seismic Risk Monitor
"""

import os
import json
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2-lon1)/2)**2
    return 2 * 6371 * math.asin(math.sqrt(a)) # returns km

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class GeoSeismic(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-26",
            input_topics=["raw.earthquake_events", "raw.weather_fields"],
            output_topics=["seismic_events"]
        )
        self.weather = {} # by district
        
        self.soil = self._load("district_soil_class.json")
        self.liq = self._load("liquefaction_susceptibility.json")
        self.gw = self._load("groundwater_table.json")
        self.sm = self._load("soil_moisture_baseline.json")
        
        self.districts = {
            "BD-15": {"lat": 22.33, "lon": 91.83}, # Chittagong
            "BD-26": {"lat": 23.81, "lon": 90.41}, # Dhaka
            "BD-06": {"lat": 22.70, "lon": 90.36}, # Barisal
            "BD-55": {"lat": 24.89, "lon": 91.87}, # Sylhet
            "BD-20": {"lat": 25.06, "lon": 91.40}, # Sunamganj
            "BD-21": {"lat": 24.88, "lon": 90.72}, # Netrokona
            "BD-36": {"lat": 21.42, "lon": 92.00}, # Cox's Bazar
            "BD-38": {"lat": 22.15, "lon": 90.11}, # Barguna
            "BD-40": {"lat": 22.35, "lon": 90.31}  # Patuakhali
        }

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        if topic == "raw.weather_fields":
            d_code = message.get("district_code", "UNKNOWN")
            self.weather[d_code] = message
            return
            
        if topic == "raw.earthquake_events":
            self._process_eq(message)

    def _process_eq(self, eq_msg):
        mw = eq_msg.get("magnitude_mw")
        if mw is None or mw < 3.0: return
        
        lat = eq_msg.get("epicentre_lat")
        lon = eq_msg.get("epicentre_lon")
        
        if not (20.0 <= lat <= 27.0 and 88.0 <= lon <= 96.0): return
        
        depth = max(1.0, min(700.0, float(eq_msg.get("depth_km", 10.0))))
        
        affected = []
        pga_arr = []
        dhaka_dist = 0.0
        
        c1, c2, c3, c4 = -2.991, 1.414, -1.693, -0.00607
        
        for dval, coords in self.districts.items():
            r_epi = haversine(lat, lon, coords["lat"], coords["lon"])
            if dval == "BD-26": dhaka_dist = r_epi
            
            r_hypo = max(10.0, math.sqrt(r_epi**2 + depth**2))
            if r_hypo > 200.0: continue
            
            ln_pga = c1 + c2*mw + c3*math.log(r_hypo) + c4*r_hypo
            pga_r = math.exp(ln_pga)
            
            sc = self.soil.get(dval, "STIFF_SOIL")
            f_soil = 1.5
            if sc == "ROCK": f_soil = 1.0
            elif sc == "SOFT_SOIL": f_soil = 2.5
            
            pga_s = pga_r * f_soil
            mmi = 2.20 + 1.00 * math.log(max(1e-6, pga_s)) / math.log(10) * 10
            
            if pga_s >= 0.05:
                affected.append(dval)
                pga_arr.append({
                    "district_code": dval, "distance_km": float(r_hypo),
                    "pga_site_g": float(pga_s), "soil_class": sc, "mmi_intensity": float(mmi)
                })
                
        if not affected:
            self._publish_empty(eq_msg, dhaka_dist)
            return
            
        # liq
        max_b_liq = 0.0
        max_d_liq = affected[0]
        for dval in affected:
            sq = self.liq.get(dval, "LOW")
            si = 0.2
            if sq == "HIGH": si = 0.9
            elif sq == "MODERATE": si = 0.5
            elif sq == "NONE": si = 0.0
            
            dtw = self.gw.get(dval, 3.0)
            df = max(0.0, min(1.0, 1.0 - dtw / 10.0))
            bl = si * df
            if bl > max_b_liq:
                max_b_liq = bl
                max_d_liq = dval
                
        sm_curr = self.weather.get(max_d_liq, {}).get("soil_moisture_m3m3")
        sm_base = self.sm.get(max_d_liq)
        
        sm_mod = 1.0
        if sm_curr is not None and sm_base is not None:
            sm_mod = 1.0 + (sm_curr - sm_base)
            
        p_liq_f = min(1.0, max_b_liq * sm_mod)
        pga_rock_g = max([a["pga_site_g"]/2.5 for a in pga_arr] + [0.0]) # approx
        
        out = {
            "event_id": eq_msg.get("event_id"),
            "magnitude_mw": mw,
            "depth_km": depth,
            "epicentre_lat": float(lat), "epicentre_lon": float(lon),
            "distance_to_dhaka_km": float(dhaka_dist),
            "affected_districts": affected,
            "pga_rock_g": float(pga_rock_g),
            "pga_districts": pga_arr,
            "base_liquefaction_probability": float(max_b_liq),
            "soil_moisture_modifier": float(sm_mod),
            "p_liq_final": float(p_liq_f),
            "embankment_assessment_triggered": (mw >= 4.5),
            "liquefaction_enriched": False,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("seismic_events", out)

    def _publish_empty(self, eq_msg, dist):
        out = {
            "event_id": eq_msg.get("event_id"),
            "magnitude_mw": eq_msg.get("magnitude_mw"),
            "depth_km": eq_msg.get("depth_km"),
            "epicentre_lat": eq_msg.get("epicentre_lat"), "epicentre_lon": eq_msg.get("epicentre_lon"),
            "distance_to_dhaka_km": float(dist),
            "affected_districts": [],
            "pga_rock_g": 0.0,
            "pga_districts": [],
            "base_liquefaction_probability": 0.0,
            "soil_moisture_modifier": 1.0,
            "p_liq_final": 0.0,
            "embankment_assessment_triggered": False,
            "liquefaction_enriched": False,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("seismic_events", out)

if __name__ == "__main__":
    mod = GeoSeismic()
    mod.start()
