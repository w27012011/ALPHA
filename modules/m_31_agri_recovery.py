"""
M-31 AGRI-RECOVERY
Agricultural Recovery & Food Security Monitor
"""

import os
import json
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class AgriRecovery(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-31",
            input_topics=["agri.loss_estimate", "flood_predictions"],
            output_topics=["crop_stress"]
        )
        self.flood = {}
        self.prod = self._load("district_crop_production.json")
        self.rdl = self._load("river_danger_levels.json")

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        dc = message.get("district_code")
        if topic == "flood_predictions":
            if dc: self.flood[dc] = message
            return
            
        if topic == "agri.loss_estimate":
            self._process_rec(message)

    def _process_rec(self, loss_msg):
        d_code = loss_msg.get("district_code")
        if not d_code: return
        
        c_type = loss_msg.get("crop_type")
        loss_p = loss_msg.get("loss_probability", 0.0)
        scen = loss_msg.get("loss_scenario", "NONE")
        tr = loss_msg.get("tonnage_at_risk_metric_tons")

        f_msg = self.flood.get(d_code, {})
        f_pwl = f_msg.get("peak_water_level_m")
        f_dur = f_msg.get("duration_days")
        f_prob = f_msg.get("flood_probability")

        has_flood = False
        if scen != "NONE" and f_prob is not None and f_prob >= 0.20:
            has_flood = True
            
        # Rep delay
        r_delay = 0
        cat = "NA"
        i_depth = None
        
        if has_flood:
            rd_lvl = self.rdl.get(d_code)
            if f_pwl is not None and rd_lvl is not None:
                i_depth = max(0.0, float(f_pwl) - float(rd_lvl))
            elif f_dur is not None:
                i_depth = max(0.0, float(f_dur) * 0.5)
                
            if i_depth is not None:
                r_delay = int(max(7, i_depth * 14))
            else:
                r_delay = 7 # fallback min
                
            if r_delay <= 7: cat = "IMMEDIATE"
            elif r_delay <= 30: cat = "WEEKS"
            else: cat = "MONTHS"

        # FSSI
        fssi = None
        pd = self.prod.get(f"{d_code}_{c_type}")
        
        if pd is not None and float(pd) > 0:
            if tr is not None:
                fssi = (loss_p * tr) / float(pd)
            else:
                fssi = loss_p * 0.5 # assumed 50% exposed
                
        if fssi is not None:
            fssi = max(0.0, min(1.0, float(fssi)))

        out = {
            "district_code": d_code,
            "district_name": loss_msg.get("district_name", d_code),
            "crop_type": c_type,
            "loss_probability": float(loss_p),
            "loss_scenario": scen,
            "tonnage_at_risk_metric_tons": float(tr) if tr is not None else None,
            "replanting_delay_days": r_delay if has_flood else 0,
            "recovery_category": cat,
            "food_security_stress_index": fssi,
            "inundation_depth_m": float(i_depth) if i_depth is not None else None,
            "flood_duration_days": float(f_dur) if f_dur is not None else None,
            "stress_index": fssi, # Same as FSSI
            "district_annual_production_tons": float(pd) if pd is not None else None,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("crop_stress", out)

if __name__ == "__main__":
    mod = AgriRecovery()
    mod.start()
