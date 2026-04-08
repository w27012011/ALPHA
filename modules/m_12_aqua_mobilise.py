"""
M-12 AQUA-MOBILISE
Arsenic Mobilisation Risk Detector
"""

import os
from collections import deque
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2-lon1)/2)**2
    return 2 * 6371000 * math.asin(math.sqrt(a))

class AquaMobilise(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-12",
            input_topics=["aqua.kriging_field", "flood_predictions", "raw.earthquake_events"],
            output_topics=["aqua.mobilisation_risk"]
        )
        self.flood_context = {}
        self.eq_context = None
        self.history = {} # keys: grid_id -> deque of past log_arsenic_predicted values
        self.m_cum = {} # keys: grid_id -> PH cumulative stat

    def process(self, topic, message):
        if topic == "flood_predictions":
            dist = message.get("district_code")
            if dist: self.flood_context[dist] = message
            return
            
        if topic == "raw.earthquake_events":
            self.eq_context = message
            return
            
        if topic == "aqua.kriging_field":
            self._execute_ph_detector(message)

    def _execute_ph_detector(self, kriging_msg):
        if kriging_msg.get("_error"):
            # propagate
            out = {
                "trigger_type": "UNKNOWN", "trigger_flood_phase": None, "trigger_event_id": None,
                "grid_points": [], "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "_error": kriging_msg["_error"]
            }
            self.publish("aqua.mobilisation_risk", out)
            return

        points_out = []
        delta = 0.1
        lambda_ph = 2.0
        seismic_amp = 1.5
        min_history = 10
        baseline_window = 30
        
        # Seismic check
        active_eq = False
        eq_lat, eq_lon = None, None
        if self.eq_context and self.eq_context.get("felt_in_bangladesh"):
            if self.eq_context.get("magnitude", 0) >= 4.5:
                active_eq = True
                eq_lat = self.eq_context.get("lat")
                eq_lon = self.eq_context.get("lon")
                
        for pt in kriging_msg.get("grid_points", []):
            gid = pt.get("grid_id")
            val = pt.get("log_arsenic_predicted")
            if not gid or val is None:
                continue
                
            dist = pt.get("district_code")
            
            if gid not in self.history:
                self.history[gid] = deque(maxlen=baseline_window)
                self.m_cum[gid] = 0.0
                
            hx = self.history[gid]
            
            mob_prob = None
            alert = False
            amp_applied = False
            trig_local = None
            
            f_phase = "Normal"
            if dist in self.flood_context:
                f_phase = self.flood_context[dist].get("phase", "Normal")
                f_prob = self.flood_context[dist].get("flood_probability", 0)
                if f_prob > 0.3 and f_phase in ("Stressed", "Critical"):
                    trig_local = "FLOOD"
                    
            if active_eq and eq_lat is not None and eq_lon is not None:
                dist_m = haversine(pt["lat"], pt["lon"], eq_lat, eq_lon)
                if dist_m <= 200000:
                    if trig_local == "FLOOD": trig_local = "BOTH"
                    else: trig_local = "SEISMIC"
                    amp_applied = True
            
            # PH computation
            if len(hx) >= min_history:
                mu_0 = sum(hx) / len(hx)
                residual = val - mu_0
                # m_n = max(0, m_n-1 + residual - delta)
                prev_m = self.m_cum[gid]
                self.m_cum[gid] = max(0.0, prev_m + residual - delta)
                
                if amp_applied:
                    self.m_cum[gid] = self.m_cum[gid] * seismic_amp
                    
                self.m_cum[gid] = min(100.0, self.m_cum[gid])
                
                mob_prob = min(1.0, self.m_cum[gid] / lambda_ph)
                alert = self.m_cum[gid] > lambda_ph
                
            # append val AFTER computing current residual, per standard CUSUM
            self.history[gid].append(val)
            
            points_out.append({
                "grid_id": gid,
                "district_code": dist,
                "log_arsenic_predicted": val,
                "kriging_variance": pt.get("kriging_variance"),
                "ph_statistic": self.m_cum[gid],
                "mobilisation_probability": mob_prob,
                "alert_triggered": alert,
                "trigger_type_local": trig_local,
                "seismic_amplifier_applied": amp_applied,
                "below_detection": pt.get("below_detection", False),
                "low_data_density": pt.get("low_data_density", False)
            })

        out = {
            "trigger_type": kriging_msg.get("trigger_type"),
            "trigger_flood_phase": None, # Could derive overall
            "trigger_event_id": kriging_msg.get("trigger_event_id"),
            "grid_points": points_out,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("aqua.mobilisation_risk", out)

if __name__ == "__main__":
    mod = AquaMobilise()
    mod.start()
