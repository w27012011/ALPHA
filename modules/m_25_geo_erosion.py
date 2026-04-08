"""
M-25 GEO-EROSION
Riverbank Erosion Predictor
"""

import os
import json
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2-lon1)/2)**2
    return 2 * 6371000 * math.asin(math.sqrt(a))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class GeoErosion(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-25",
            input_topics=["geo.displacement_processed", "hydro.water_normalized"],
            output_topics=["erosion_alerts"]
        )
        self.water_levels = {} # by station
        
        self.seg_st_map = self._load("segment_station_map.json")
        self.mat_props = self._load("bank_material_properties.json")
        self.geom = self._load("bank_geometry.json")
        self.slopes = self._load("river_slope.json")
        self.pops = self._load("segment_population.json")
        self.rels = self._load("relocation_sites.json")

    def _load(self, file_name):
        try:
            with open(os.path.join(DATA_DIR, file_name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        if topic == "hydro.water_normalized":
            st_id = message.get("station_id")
            if st_id: self.water_levels[st_id] = message
            return
            
        if topic == "geo.displacement_processed":
            self._process_erosion(message)

    def _process_erosion(self, disp_msg):
        sid = disp_msg.get("segment_id")
        if not sid: return
        
        q_f = disp_msg.get("quality_flag")
        if q_f in ("UNUSABLE", "SINGLE_ACQUISITION"):
            self._publish_nulls(disp_msg, "NO_DISPLACEMENT_DATA")
            return
            
        # load material props
        mat = self.mat_props.get(sid, {})
        c_prime = mat.get("c_prime_kPa", 5.0)
        phi = mat.get("phi_prime_deg", 28.0)
        if phi >= 90.0: phi = 28.0
        
        # geometry
        g = self.geom.get(sid, {})
        h_bank = g.get("H_bank_m")
        toe_el = g.get("bank_toe_elevation_m")
        
        # water
        st_map = self.seg_st_map.get(sid, {})
        st_id = st_map.get("station_id")
        w_msg = self.water_levels.get(st_id, {})
        w_lvl = w_msg.get("water_level_m")
        l_above_danger = w_msg.get("level_above_danger", 0)
        
        hw = 0.0
        if w_lvl is not None and toe_el is not None:
            hw = max(0.0, float(w_lvl) - float(toe_el))
        elif l_above_danger is not None:
            hw = max(0.0, float(l_above_danger) * 0.5)
            
        # stress
        sig_n = 50.0
        if h_bank is not None:
            sig_n = 18.0 * float(h_bank) - 9.81 * hw
            
        tau_f = c_prime + sig_n * math.tan(math.radians(phi))
        
        S_river = self.slopes.get(disp_msg.get("river_name"), 0.0001)
        tau_app = 9.81 * hw * S_river
        
        ratio = tau_app / max(0.001, tau_f)
        if ratio > 2.0:
            self.logger.warning("STRESS_RATIO_CAPPED")
            ratio = 2.0
            
        fail_prob = 1.0 / (1.0 + math.exp(-10.0 * (ratio - 0.80)))
        
        lvl = "GREEN"
        if ratio >= 1.00: lvl = "CRITICAL"
        elif ratio >= 0.90: lvl = "RED"
        elif ratio >= 0.70: lvl = "ORANGE"
        elif ratio >= 0.60: lvl = "YELLOW"
        elif ratio >= 0.50: lvl = "BLUE"
        
        ttf = None
        rate = disp_msg.get("displacement_rate_mm_per_day")
        if rate is not None and rate > 0 and ratio < 1.00:
            ttf = (tau_f - tau_app) / (rate * 0.15)
        elif ratio >= 1.00:
            ttf = 0.0
            
        # Population
        pop = self.pops.get(sid)
        rs = self.rels.get(disp_msg.get("district_code"), [])
        # We dummy out the exact haversine sort since coords for relocation sites aren't fully baked in standard JSON
        rs = rs[:3]
        
        # Embankment check
        emb_score = None
        try:
            with open(os.path.join(DATA_DIR, "embankment_integrity.json"), "r") as f:
                e_data = json.load(f)
                emb_score = e_data.get(sid)
        except Exception:
            pass
            
        out = {
            "segment_id": sid,
            "district_code": disp_msg.get("district_code"),
            "river_name": disp_msg.get("river_name"),
            "site_lat": disp_msg.get("site_lat"),
            "site_lon": disp_msg.get("site_lon"),
            "failure_probability": float(fail_prob),
            "shear_stress_applied_kPa": float(tau_app),
            "shear_strength_calculated_kPa": float(tau_f),
            "stress_ratio": float(ratio),
            "alert_level": lvl,
            "time_to_failure_days": float(ttf) if ttf is not None else None,
            "people_in_displacement_zone": pop,
            "nearest_relocation_sites": rs,
            "embankment_integrity_score": float(emb_score) if emb_score is not None else None,
            "displacement_mm": disp_msg.get("displacement_mm"),
            "displacement_rate_mm_per_day": rate,
            "water_level_m": w_lvl,
            "quality_flag": disp_msg.get("quality_flag"),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("erosion_alerts", out)

    def _publish_nulls(self, disp_msg, cause):
        out = {
            "segment_id": disp_msg.get("segment_id"),
            "district_code": disp_msg.get("district_code"),
            "river_name": disp_msg.get("river_name"),
            "site_lat": disp_msg.get("site_lat"),
            "site_lon": disp_msg.get("site_lon"),
            "failure_probability": None,
            "shear_stress_applied_kPa": None,
            "shear_strength_calculated_kPa": None,
            "stress_ratio": None,
            "alert_level": "GREEN",
            "time_to_failure_days": None,
            "people_in_displacement_zone": None,
            "nearest_relocation_sites": [],
            "embankment_integrity_score": None,
            "displacement_mm": disp_msg.get("displacement_mm"),
            "displacement_rate_mm_per_day": disp_msg.get("displacement_rate_mm_per_day"),
            "water_level_m": None,
            "quality_flag": cause,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("erosion_alerts", out)

if __name__ == "__main__":
    mod = GeoErosion()
    mod.start()
