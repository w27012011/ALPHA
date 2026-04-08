"""
M-30 AGRI-LOSS
Crop Loss Predictor
"""

import os
import json
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class AgriLoss(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-30",
            input_topics=["agri.harvest_status", "flood_predictions", "raw.weather_fields"],
            output_topics=["agri.loss_estimate"]
        )
        self.flood = {}
        self.weather = {}
        self.prod = self._load("district_crop_production.json")

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
            
        if topic == "raw.weather_fields":
            for d in message.get("districts", []):
                self.weather[d.get("district_code")] = d
            return
            
        if topic == "agri.harvest_status":
            self._process_loss(message)

    def _process_loss(self, harv_msg):
        d_code = harv_msg.get("district_code")
        if not d_code: return
        
        c_type = harv_msg.get("crop_type", "UNKNOWN")
        
        # Load env
        f_msg = self.flood.get(d_code, {})
        f_prob = f_msg.get("flood_probability")
        f_dur = f_msg.get("duration_days")
        
        w_msg = self.weather.get(d_code, {})
        temp = w_msg.get("surface_temp_celsius")
        hum = w_msg.get("relative_humidity_pct")
        
        c_est = False
        
        # Scenario
        act = harv_msg.get("active_season", False)
        comp = harv_msg.get("harvest_completable_before_flood", True)
        
        scen = "NONE"
        if not act or (f_prob is None or f_prob < 0.20):
            # Check humidity spoilage
            if act and (hum is not None and hum > 85.0) and (temp is not None and temp > 30.0):
                scen = "STORAGE_SPOILAGE"
            else:
                scen = "NONE"
        elif not comp:
            scen = "PRE_HARVEST_TOTAL"
        elif comp and f_prob is not None and f_prob >= 0.20:
            scen = "POST_HARVEST_PARTIAL"
            
        if scen == "NONE":
            self._publish_none(harv_msg)
            return

        conf = 1.0
        
        if hum is None:
            c_est = True
            conf -= 0.15
            if "RICE" in c_type: hum = 70.0
            elif "WHEAT" in c_type: hum = 60.0
            else: hum = 80.0
            
        S_frac = max(0.0, min(1.0, float(hum) / 100.0))
        
        par_v = 0.08; par_km = 0.35; par_n0 = 1000; par_lag = 6.0
        if "WHEAT" in c_type:
            par_v = 0.10; par_km = 0.30; par_n0 = 500; par_lag = 8.0
        elif "JUTE" in c_type:
            par_v = 0.05; par_km = 0.40; par_n0 = 2000; par_lag = 4.0
        elif "VEGETABLE" in c_type:
            par_v = 0.15; par_km = 0.25; par_n0 = 5000; par_lag = 3.0
            
        # MM Spoilage
        v_sp = (par_v * S_frac) / (par_km + S_frac)
        v_sp = max(0.0, v_sp)
        
        s_days = 7.0
        if f_dur is not None: s_days = float(f_dur)
        elif harv_msg.get("days_to_harvest_end") is not None: s_days = max(1.0, float(harv_msg.get("days_to_harvest_end")))
        
        p_sp = min(1.0, v_sp * s_days)
        
        # BR Microbes
        p_cont = None
        if temp is None:
            conf -= 0.20
        else:
            if temp <= 2.0:
                p_cont = 0.0
            else:
                mu_max = (0.031 * (temp - 2.0))**2
                f_arr = harv_msg.get("flood_arrival_hours")
                t_hrs = float(f_arr) if f_arr is not None else 0.0
                if t_hrs < 0: t_hrs = 0.0
                
                exp_mu_t = mu_max * t_hrs
                if exp_mu_t > 30: # pre-overflow guard
                    p_cont = 1.0
                else:
                    try:
                        n_ratio = math.exp(exp_mu_t) - math.log(1.0 + (math.exp(exp_mu_t) - 1.0) / math.exp(mu_max * par_lag))
                        n_ratio = math.exp(n_ratio)
                        
                        n_val = par_n0 * n_ratio
                        p_cont = min(1.0, n_val / 1000000.0)
                    except OverflowError:
                        p_cont = 1.0
                        
        p_tim = 0.0
        if scen == "PRE_HARVEST_TOTAL": p_tim = 1.0
        elif scen == "POST_HARVEST_PARTIAL": p_tim = 0.5
        
        if p_cont is None:
            wt_t = 0.625; wt_s = 0.375; wt_c = 0.0
            p_cont_val = 0.0
        else:
            wt_t = 0.5; wt_s = 0.3; wt_c = 0.2
            p_cont_val = p_cont
            
        p_loss = (wt_t * p_tim) + (wt_s * p_sp) + (wt_c * p_cont_val)
        p_loss = max(0.0, min(1.0, p_loss))
        
        # Tonnage
        tr = None
        pd = self.prod.get(f"{d_code}_{c_type}")
        if pd is not None and f_prob is not None:
            tr = float(pd) * p_loss * f_prob
            
        if tr is None:
            conf -= 0.10
        if f_prob is None:
            conf -= 0.15
            
        out = {
            "district_code": d_code,
            "district_name": harv_msg.get("district_name", d_code),
            "crop_type": c_type,
            "loss_probability": float(p_loss),
            "loss_scenario": scen,
            "tonnage_at_risk_metric_tons": float(tr) if tr is not None else None,
            "spoilage_rate_pct_per_day": float(v_sp * 100),
            "contamination_risk": float(p_cont) if p_cont is not None else None,
            "p_timing": float(p_tim),
            "p_spoilage": float(p_sp),
            "p_contamination": float(p_cont) if p_cont is not None else None,
            "temperature_used_celsius": float(temp) if temp is not None else None,
            "humidity_used_pct": float(hum) if hum is not None else None,
            "flood_probability": float(f_prob) if f_prob is not None else None,
            "flood_duration_days": float(f_dur) if f_dur is not None else None,
            "confidence": float(max(0.1, conf)),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("agri.loss_estimate", out)

    def _publish_none(self, harv_msg):
        out = {
            "district_code": harv_msg.get("district_code"),
            "district_name": harv_msg.get("district_name"),
            "crop_type": harv_msg.get("crop_type"),
            "loss_probability": 0.0,
            "loss_scenario": "NONE",
            "tonnage_at_risk_metric_tons": 0.0,
            "spoilage_rate_pct_per_day": 0.0,
            "contamination_risk": 0.0,
            "p_timing": 0.0,
            "p_spoilage": 0.0,
            "p_contamination": 0.0,
            "temperature_used_celsius": None,
            "humidity_used_pct": None,
            "flood_probability": 0.0,
            "flood_duration_days": 0.0,
            "confidence": 1.0,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("agri.loss_estimate", out)

if __name__ == "__main__":
    mod = AgriLoss()
    mod.start()
