"""
M-29 AGRI-HARVEST
Harvest Calendar Checker
"""

import os
import json
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class AgriHarvest(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-29",
            input_topics=["agri.ndvi_processed", "flood_predictions"],
            output_topics=["agri.harvest_status"]
        )
        self.flood = {}
        self.cal = self._load("harvest_calendar.json")

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        if topic == "flood_predictions":
            self.flood[message.get("district_code")] = message
            return
            
        if topic == "agri.ndvi_processed":
            self._process_harvest(message)

    def _process_harvest(self, ndvi_msg):
        d_code = ndvi_msg.get("district_code")
        c_type = ndvi_msg.get("dominant_crop_type")
        if not d_code or not c_type: return
        
        # Load constraints
        dr_cal = self.cal.get(d_code, {})
        if c_type not in dr_cal:
            self.logger.warning("HARVEST_CALENDAR_MISSING_FOR_CROP")
            return
            
        cc = dr_cal[c_type]
        now_ts = datetime.now(timezone.utc)
        
        d_sp = ndvi_msg.get("days_since_planting")
        stage = ndvi_msg.get("growth_stage", "UNKNOWN")
        
        h_s_str = cc.get("harvest_start")
        h_e_str = cc.get("harvest_end")
        
        # Parse logic
        def p_d(mm_dd):
            m = int(mm_dd.split("-")[0])
            d_day = int(mm_dd.split("-")[1])
            # Align around current year
            res = datetime(now_ts.year, m, d_day, tzinfo=timezone.utc)
            return res
            
        try:
            h_start = p_d(h_s_str)
            h_end = p_d(h_e_str)
            if h_end < h_start:
                h_end = datetime(now_ts.year+1, h_end.month, h_end.day, tzinfo=timezone.utc)
            
            # Re-align if window is totally in the past (e.g. we are Dec, harvest was Jan-Mar)
            # Simplistic push forward logic to ensure we are in the closest window
            if h_end < now_ts and (now_ts - h_end).days > 300:
                h_start = datetime(now_ts.year+1, h_start.month, h_start.day, tzinfo=timezone.utc)
                h_end = datetime(now_ts.year+1, h_end.month, h_end.day, tzinfo=timezone.utc)
                
            active = (h_start <= now_ts <= h_end)
            dhe = (h_end - now_ts).days if active else None
            
        except Exception:
            return
            
        # load flood
        f_msg = self.flood.get(d_code, {})
        f_prob = f_msg.get("flood_probability")
        f_arr = f_msg.get("lead_time_hours")
        
        if f_prob is None or f_prob < 0.20:
            f_arr = None
            f_prob = None
            
        e_win = None
        comp = True
        
        if active and f_arr is not None and dhe is not None:
            e_win = (float(dhe) * 24.0) - float(f_arr)
            comp = (e_win > 0)
        elif not active:
            dhe = None
            comp = True
            e_win = None
            
        out = {
            "district_code": d_code,
            "district_name": ndvi_msg.get("district_name", d_code),
            "crop_type": c_type,
            "harvest_window_start": h_start.isoformat().replace("+00:00", "Z"),
            "harvest_window_end": h_end.isoformat().replace("+00:00", "Z"),
            "days_to_harvest_end": dhe,
            "flood_probability": float(f_prob) if f_prob is not None else None,
            "flood_arrival_hours": float(f_arr) if f_arr is not None else None,
            "emergency_harvest_window_hours": float(e_win) if e_win is not None else None,
            "harvest_completable_before_flood": comp,
            "growth_stage": stage,
            "days_since_planting": d_sp,
            "active_season": active,
            "timestamp": now_ts.isoformat().replace("+00:00", "Z")
        }
        self.publish("agri.harvest_status", out)

if __name__ == "__main__":
    mod = AgriHarvest()
    mod.start()
