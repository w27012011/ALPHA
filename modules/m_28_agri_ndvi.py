"""
M-28 AGRI-NDVI
MODIS NDVI Crop Health Processor
"""

import os
import json
from datetime import datetime, timezone
import dateutil.parser

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class AgriNdvi(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-28",
            input_topics=["raw.ndvi_grid", "raw.weather_fields"],
            output_topics=["agri.ndvi_processed"]
        )
        self.weather = {}
        self.ndvi_history = {}
        
        self.base_ndvi = self._load("ndvi_baseline.json")
        self.cal = self._load("harvest_calendar.json")
        self.dom = self._load("dominant_crop.json")

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        if topic == "raw.weather_fields":
            for d in message.get("districts", []):
                self.weather[d.get("district_code")] = d
            return
            
        if topic == "raw.ndvi_grid":
            self._process_ndvi(message)

    def _process_ndvi(self, ndvi_msg):
        c_date_str = ndvi_msg.get("composite_date")
        if not c_date_str: return
        
        try:
            dt = dateutil.parser.isoparse(c_date_str)
        except Exception:
            return
            
        c_pct = ndvi_msg.get("cloud_cover_pct", 0.0)
        stale = False
        now_ts = datetime.now(timezone.utc)
        if (now_ts - dt).days > 8: stale = True
        
        month = f"{dt.month:02d}"
        
        for d in ndvi_msg.get("districts", []):
            code = d.get("district_code")
            if not code: continue
            
            n_curr = d.get("ndvi_mean")
            lst = d.get("lst_celsius")
            
            w_d = self.weather.get(code, {})
            if lst is None: lst = w_d.get("surface_temp_celsius")
            s_m = w_d.get("soil_moisture_m3m3")
            
            if code not in self.ndvi_history:
                self.ndvi_history[code] = []
                
            use_stale_flag = stale
            ch_idx = None
            stage = "UNKNOWN"
            d_sp = None
            
            if c_pct > 70.0:
                use_stale_flag = True
                if self.ndvi_history[code]:
                    n_curr = self.ndvi_history[code][-1]
                    self.logger.debug("HIGH_CLOUD_COVER_USING_PREVIOUS")
                else:
                    n_curr = None
            
            if n_curr is not None:
                n_curr = max(-1.0, min(1.0, float(n_curr)))
                ch_idx = max(0.0, min(1.0, (n_curr - (-0.1)) / (0.9 - (-0.1))))
                
                # Append to history
                self.ndvi_history[code].append(n_curr)
                if len(self.ndvi_history[code]) > 5:
                    self.ndvi_history[code].pop(0)

            # Baseline
            n_30 = None
            if self.ndvi_history[code]:
                n_30 = sum(self.ndvi_history[code]) / len(self.ndvi_history[code])
                
            n_base = self.base_ndvi.get(f"{code}_{month}")
            n_anom = None
            if n_curr is not None and n_base is not None:
                n_anom = n_curr - n_base
                
            # Crop
            c_type = self.dom.get(f"{code}_{month}", "UNKNOWN")
            
            # Growth Stage
            if c_type in self.cal.get(code, {}):
                cc = self.cal[code][c_type]
                try:
                    def p_d(mm_dd):
                        # Extract month. If mm_dd represents a crossover, we assign years properly
                        m = int(mm_dd.split("-")[0])
                        d_day = int(mm_dd.split("-")[1])
                        # Approximation for current year
                        return datetime(now_ts.year, m, d_day, tzinfo=timezone.utc)

                    p_start = p_d(cc["plant_start"])
                    p_end = p_d(cc["plant_end"])
                    h_start = p_d(cc["harvest_start"])
                    h_end = p_d(cc["harvest_end"])
                    
                    if p_end < p_start: p_end = datetime(now_ts.year+1, p_end.month, p_end.day, tzinfo=timezone.utc)
                    if h_start < p_end: h_start = datetime(now_ts.year+1, h_start.month, h_start.day, tzinfo=timezone.utc)
                    if h_end < h_start: h_end = datetime(now_ts.year+1, h_end.month, h_end.day, tzinfo=timezone.utc)
                    
                    # Assume we are aligning to closest timeline
                    
                    if p_start <= now_ts <= h_end:
                        d_sp = (now_ts - p_start).days
                        if n_curr is not None:
                            if n_curr < 0.2 and d_sp < 20: stage = "GERMINATION"
                            elif 0.2 <= n_curr < 0.6: stage = "VEGETATIVE"
                            elif 0.6 <= n_curr < 0.8: stage = "REPRODUCTIVE"
                            elif n_curr >= 0.8 and d_sp < (h_start - p_start).days - 14: stage = "RIPENING"
                            elif n_curr >= 0.8: stage = "HARVEST_READY"
                    else:
                        stage = "FALLOW"
                except Exception:
                    pass
            elif n_curr is not None:
                stage = "UNKNOWN"

            out = {
                "district_code": code,
                "district_name": d.get("district_name", code),
                "dominant_crop_type": c_type,
                "ndvi_current": n_curr,
                "ndvi_baseline_30day": n_30,
                "ndvi_anomaly": n_anom,
                "ndvi_baseline_5yr_mean": n_base,
                "lst_current_celsius": lst,
                "soil_moisture_current_m3m3": s_m,
                "growth_stage": stage,
                "crop_health_index": ch_idx,
                "days_since_planting": d_sp,
                "composite_date": c_date_str,
                "cloud_cover_pct": c_pct,
                "stale_ndvi": use_stale_flag,
                "timestamp": now_ts.isoformat().replace("+00:00", "Z")
            }
            self.publish("agri.ndvi_processed", out)

if __name__ == "__main__":
    mod = AgriNdvi()
    mod.start()
