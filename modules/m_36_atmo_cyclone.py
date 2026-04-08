"""
M-36 ATMO-CYCLONE
Cyclone Monitor & Port Operability Assessor
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

class AtmoCyclone(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-36",
            input_topics=["atmo.storm_detected", "atmo.weather_processed"],
            output_topics=["storm_forecast"]
        )
        self.weather = {}
        
        self.chittagong = {"lat": 22.33, "lon": 91.81}
        self.dist_centroids = {
            "BD-26": {"lat": 23.81, "lon": 90.41},
            "BD-15": {"lat": 22.33, "lon": 91.83}, # Approx Chittagong District
            "BD-40": {"lat": 22.35, "lon": 90.31},
            "BD-38": {"lat": 22.15, "lon": 90.11}
        }

    def process(self, topic, message):
        if topic == "atmo.weather_processed":
            dc = message.get("district_code")
            if dc: self.weather[dc] = message
            return
            
        if topic == "atmo.storm_detected":
            self._process_cyclone(message)

    def _process_cyclone(self, st_msg):
        # We process this per storm_detected cycle. Usually one emission is enough but we might get many
        # It's better to just process it and check the state of the active cyclone.
        now_ts = datetime.now(timezone.utc)
        
        adv = {}
        try:
            with open(os.path.join(DATA_DIR, "cyclone_advisory.json"), "r") as f:
                adv = json.load(f)
        except Exception:
            pass
            
        is_active = False
        track_lat = None
        track_lon = None
        sus_w = None
        c_id = None
        c_cat = None
        c_name = None
        l_loc = None
        l_time = None
        
        # Priority 1: User Advisory
        if adv:
            adv_str = adv.get("timestamp_utc")
            if adv_str:
                try:
                    # Clean ISO format to handle different variants
                    import dateutil.parser
                    adv_ts = dateutil.parser.isoparse(adv_str)
                    age_h = (now_ts - adv_ts).total_seconds() / 3600.0
                    if age_h <= 6.0:
                        is_active = True
                        track_lat = adv.get("track_lat")
                        track_lon = adv.get("track_lon")
                        sus_w = float(adv.get("sustained_wind_kmh", 0))
                        c_cat = adv.get("category")
                        c_id = adv.get("cyclone_id")
                        c_name = adv.get("name")
                        l_loc = "Advisory Path"
                        l_time = "Unknown"
                except Exception as e:
                    pass
        
        # Priority 2: Storm object over Bay 
        if not is_active:
            slat = st_msg.get("location_lat")
            slon = st_msg.get("location_lon")
            if slat is not None and slon is not None:
                if slat < 22.0 and slon > 85.0 and st_msg.get("intensity") == "SEVERE":
                    self.logger.info("NO_ADVISORY_USING_DETECTION (Genesis)")
                    # Found potential genesis... we flag risk later
        
        # Setup fallback from generic weather
        w_msg = self.weather.get("BD-15", {}) # Using Chittagong area as proxy if needed
        sst_bay = w_msg.get("bay_of_bengal_sst_celsius")
        
        max_w = 0.0
        s_surge = 0.0
        
        if is_active and track_lat is not None and track_lon is not None:
            dist = haversine(track_lat, track_lon, self.chittagong["lat"], self.chittagong["lon"])
            df = math.exp(-0.005 * dist)
            max_w = sus_w * df
            
            s_surge = 0.20 * ((sus_w / 100.0) ** 2)
        else:
            w_spd = w_msg.get("wind_speed_10m_ms", 0.0)
            max_w = w_spd * 3.6
            s_surge = 0.0
            
        # Operability
        port_op = "NORMAL"
        cap = 100
        
        if max_w > 100.0 or s_surge > 1.5:
            port_op = "CLOSED"
            cap = 0
        elif max_w > 60.0 or s_surge > 0.5:
            port_op = "REDUCED"
            cap = 50
            
        # Districts
        at_risk = []
        if is_active and track_lat is not None and track_lon is not None:
            for d, c in self.dist_centroids.items():
                odist = haversine(track_lat, track_lon, c["lat"], c["lon"])
                if odist <= 150.0:
                    at_risk.append(d)
                    
        # Genesis Risk
        r_dev = "NONE"
        if not is_active:
            wsh = w_msg.get("wind_shear_ms_per_km")
            if sst_bay is not None:
                if wsh is not None:
                    if sst_bay >= 28.0 and wsh < 5.0: r_dev = "HIGH"
                    elif sst_bay >= 26.5 and wsh < 8.0: r_dev = "MODERATE"
                    elif sst_bay >= 25.0: r_dev = "LOW"
            else:
                self.logger.warning("SST_UNAVAILABLE")
                r_dev = "LOW"
                
        out = {
            "cyclone_active": is_active,
            "cyclone_id": c_id,
            "cyclone_name": c_name,
            "cyclone_category": c_cat,
            "track_lat": float(track_lat) if track_lat is not None else None,
            "track_lon": float(track_lon) if track_lon is not None else None,
            "sustained_wind_kmh": float(sus_w) if sus_w is not None else None,
            "projected_landfall_location": l_loc,
            "projected_landfall_time_utc": l_time,
            "chittagong_port_operability": port_op,
            "import_capacity_pct": int(cap),
            "storm_surge_m": float(s_surge),
            "districts_at_risk": at_risk,
            "max_wind_kmh_at_port": float(max_w),
            "cyclone_development_risk": r_dev,
            "bay_sst_celsius": float(sst_bay) if sst_bay is not None else None,
            "timestamp": now_ts.isoformat().replace("+00:00", "Z")
        }
        self.publish("storm_forecast", out)

if __name__ == "__main__":
    mod = AtmoCyclone()
    mod.start()
