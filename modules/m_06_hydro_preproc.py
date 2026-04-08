"""
M-06 HYDRO-PREPROC
Water Level Pre-Processor
"""

import os
import json
import statistics
from collections import deque
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class HydroPreproc(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-06", 
            input_topics=["raw.water_levels"], 
            output_topics=["hydro.water_normalized"]
        )
        
        # Load Station-District map
        self.station_map = {}
        map_file = os.path.join(DATA_DIR, "station_district_map.json")
        try:
            with open(map_file, "r") as f:
                self.station_map = json.load(f)
        except Exception as e:
            self.logger.warning(f"Could not load station_district_map.json: {e}")
            
        # Per-station history deque (240 ticks = 30 days @ 3h)
        self.history = {}

    def process(self, topic, message):
        """Processes raw.water_levels and emits hydro.water_normalized."""
        
        # E1 Proxy: If upstream sent _error, propagate it.
        if "_error" in message:
            out_msg = {
                "station_id": message.get("station_id"),
                "district_code": "BD-00",
                "water_level_normalised": None,
                "water_level_raw_m": None,
                "rolling_mean_30d": None,
                "rolling_std_30d": None,
                "trend": None,
                "anomaly_flag": None,
                "quality_flag": "MISSING",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "_error": message["_error"]
            }
            self.publish("hydro.water_normalized", out_msg)
            return

        station_id = message.get("station_id")
        if not station_id:
            self.logger.warning("Message missing station_id. Dropping.")
            return
            
        # District Mapping lookup
        district_data = self.station_map.get(station_id, {})
        district_code = district_data.get("district_code", "BD-00")
        
        wl_raw = message.get("water_level_m")
        quality_flag = "GOOD"
        
        if wl_raw is None:
            quality_flag = "MISSING"
        else:
            try:
                wl_raw = float(wl_raw)
                if wl_raw < -5.0 or wl_raw > 30.0:
                    quality_flag = "SUSPECT"
                    wl_raw = None
            except ValueError:
                quality_flag = "SUSPECT"
                wl_raw = None
                
        # Manage DEQUE
        if station_id not in self.history:
            self.history[station_id] = deque(maxlen=240)
            
        if quality_flag == "GOOD" and wl_raw is not None:
            self.history[station_id].append(wl_raw)
            
        hx = self.history[station_id]
        wl_norm, r_mean, r_std, anomaly = None, None, None, None
        
        if len(hx) >= 2:
            r_mean = statistics.mean(hx)
            r_std = statistics.stdev(hx)
            
            r_std_safe = max(r_std, 1e-6)  # Vulnerability Defense Rule 3: floor guard
            if r_std < 1e-6:
                quality_flag = "SUSPECT"
                self.logger.warning(f"NEAR_CONSTANT_WATER_LEVEL at {station_id} (std={r_std:.2e})")
            if wl_raw is not None:  # We have a current value to normalize
                wl_norm = (wl_raw - r_mean) / r_std_safe
                anomaly = abs(wl_norm) > 3.0
            
        out_msg = {
            "station_id": station_id,
            "district_code": district_code,
            "water_level_normalised": wl_norm,
            "water_level_raw_m": wl_raw,
            "rolling_mean_30d": r_mean,
            "rolling_std_30d": r_std,
            "trend": message.get("trend"),
            "anomaly_flag": anomaly,
            "quality_flag": quality_flag,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        
        self.publish("hydro.water_normalized", out_msg)

if __name__ == "__main__":
    mod = HydroPreproc()
    mod.start()
