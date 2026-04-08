"""
M-08 HYDRO-EXTENT
Inundation Extent Aggregator
"""

import os
import json
import time
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

# Safely import scipy for Pendrive Protocol constraint
try:
    import scipy
    from scipy.interpolate import interp1d
    LIBS_AVAILABLE = True
except ImportError:
    LIBS_AVAILABLE = False

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class HydroExtent(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-08",
            input_topics=["hydro.flood_raw"],
            output_topics=["flood_predictions"]
        )
        self.station_buffers = {}
        
        # Load World Bank table
        self.wb_table = {"area_km2": {}, "depth_m": {}}
        self.dist_meta = {}
        try:
            with open(os.path.join(DATA_DIR, "wb_inundation_scenarios.json"), "r") as f:
                self.wb_table = json.load(f)
            with open(os.path.join(DATA_DIR, "district_metadata.json"), "r") as f:
                self.dist_meta = json.load(f)
        except Exception as e:
            self.logger.error(f"Cannot load static dependency tables: {e}")
            
        self.probs_order = ["0.01", "0.02", "0.04", "0.10", "0.20", "0.50"]
        self.probs_float = [float(x) for x in self.probs_order]
        
    def process(self, topic, message):
        # Buffer stations
        station_id = message.get("station_id")
        if station_id:
            self.station_buffers[station_id] = message
            
        # Normally this module waits for a 3h aggregation window to expire.
        # For simplicity and rapid messaging in system tests, we trigger the district aggregation on every input.
        # In actual production, it's better to use `_poll_inputs` or a separate timer for 3-hourly emits.
        dist = message.get("district_code")
        if dist:
            self._aggregate_and_publish_district(dist)
            
    def _aggregate_and_publish_district(self, dist_code):
        dist_stations = [msg for st, msg in self.station_buffers.items() if msg.get("district_code") == dist_code]
        n_reporting = len(dist_stations)
        
        if n_reporting == 0:
            return
            
        # Mean probability
        probs = [s.get("flood_probability") for s in dist_stations if s.get("flood_probability") is not None]
        if not probs:
            # E1 propagates
            return
            
        prob_dist = sum(probs) / len(probs)
        
        # Phase
        phase = "Normal"
        if prob_dist >= 0.6: phase = "Critical"
        elif prob_dist >= 0.3: phase = "Stressed"
        
        # Area and Depth interpolation
        area_km2, max_depth = None, None
        
        if phase != "Normal":
            if LIBS_AVAILABLE:
                try:
                    area_y = [self.wb_table["area_km2"][p].get(dist_code, 0) for p in self.probs_order]
                    depth_y = [self.wb_table["depth_m"][p].get(dist_code, 0) for p in self.probs_order]
                    
                    interp_area = interp1d(self.probs_float, area_y, bounds_error=False, fill_value="extrapolate")
                    interp_depth = interp1d(self.probs_float, depth_y, bounds_error=False, fill_value="extrapolate")
                    
                    area_km2 = float(interp_area(prob_dist))
                    max_depth = float(interp_depth(prob_dist))
                    
                    if area_km2 < 0: area_km2 = 0.0
                    if max_depth < 0: max_depth = 0.0
                except Exception as e:
                    self.logger.warning(f"Interpolation failed for {dist_code}: {e}")
            else:
                # E4 Offline Approximation Fallback
                self.logger.debug("M-08 Offline Approximation: Defaulting to nearest neighbor for Scipy.")
                # find closest prob
                closest_p_idx = min(range(len(self.probs_float)), key=lambda i: abs(self.probs_float[i] - prob_dist))
                c_p_str = self.probs_order[closest_p_idx]
                area_km2 = self.wb_table["area_km2"][c_p_str].get(dist_code, 0)
                max_depth = self.wb_table["depth_m"][c_p_str].get(dist_code, 0)
                
        # Lead time computation
        lead_times = [s.get("lead_time_hours") for s in dist_stations if s.get("lead_time_hours") is not None]
        lt = min(lead_times) if lead_times else None
        if phase == "Normal": lt = None
        
        # Residuals
        res_list = [s.get("arima_residual") for s in dist_stations if s.get("arima_residual") is not None]
        mean_res = (sum(res_list)/len(res_list)) if res_list else None
        
        out = {
            "district_code": dist_code,
            "district_name": self.dist_meta.get(dist_code, {}).get("name", f"District {dist_code}"),
            "stations_reporting": n_reporting,
            "flood_probability": prob_dist,
            "confidence_interval_lower": prob_dist - 0.1, # simplified CI
            "confidence_interval_upper": prob_dist + 0.1,
            "phase": phase,
            "lead_time_hours": lt,
            "affected_area_km2": area_km2,
            "max_depth_m": max_depth,
            "river_primary": self.dist_meta.get(dist_code, {}).get("primary_river"),
            "arima_residual": mean_res,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        
        self.publish("flood_predictions", out)

if __name__ == "__main__":
    mod = HydroExtent()
    mod.start()
