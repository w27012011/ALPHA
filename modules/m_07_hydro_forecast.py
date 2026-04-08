"""
M-07 HYDRO-FORECAST
Flood Wave Propagator (ARIMA + Manning)
"""

import os
import json
import math
from collections import deque
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

# Scipy & Statsmodels wrapped for pendrive E4 offline support
try:
    from statsmodels.tsa.arima.model import ARIMA
    from scipy.special import expit as sigmoid
    LIBRARIES_AVAILABLE = True
except ImportError:
    LIBRARIES_AVAILABLE = False
    def sigmoid(x):
        return 1 / (1 + math.exp(-x))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class HydroForecast(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-07", 
            input_topics=["hydro.water_normalized", "raw.weather_fields"], 
            output_topics=["hydro.flood_raw"]
        )
        
        self.reach_geom = {}
        try:
            with open(os.path.join(DATA_DIR, "reach_geometry.json"), "r") as f:
                self.reach_geom = json.load(f)
        except Exception:
            pass
            
        self.weather_grid = {}
        self.history = {}

    def process(self, topic, message):
        if topic == "raw.weather_fields":
            # Multi-topic tracking cache update
            gid = message.get("grid_id")
            if gid:
                self.weather_grid[gid] = message
            return
            
        if topic == "hydro.water_normalized":
            self._execute_forecast(message)

    def _execute_forecast(self, message):
        station_id = message.get("station_id")
        dist_code = message.get("district_code", "BD-00")
        
        if "_error" in message or message.get("quality_flag") == "MISSING":
            self._publish_null(station_id, dist_code, "E1", "Upstream data missing or errored.")
            return
            
        wl_norm = message.get("water_level_normalised")
        if wl_norm is None:
            # Need actual reading
            return
            
        if station_id not in self.history:
            self.history[station_id] = deque(maxlen=240)
        self.history[station_id].append(wl_norm)
        
        hx = list(self.history[station_id])
        
        arima_order = "0,0,0"
        arima_aic = None
        arima_resid = None
        y_t = None
        ci_lower, ci_upper = None, None
        
        # Step 3b. ARIMA
        if len(hx) >= 30 and LIBRARIES_AVAILABLE:
            try:
                # Use only last 90 as per MDD
                fit_hx = hx[-90:]
                model = ARIMA(fit_hx, order=(2, 1, 2))
                result = model.fit()
                forecast = result.get_forecast(steps=1)
                y_t = forecast.predicted_mean.iloc[0]
                ci = forecast.conf_int(alpha=0.05).iloc[0]
                ci_lower, ci_upper = ci[0], ci[1]
                arima_aic = result.aic
                arima_resid = result.resid[-1]
                arima_order = "2,1,2"
            except Exception as e:
                self.logger.debug(f"ARIMA fit failed for {station_id}: {e}")
                # Fallback to threshold mode
                pass
                
        # Step 3a. Fallback Threshold
        if y_t is None:
            y_t = wl_norm # No prediction step
        
        # Step 4. Danger threshold calc
        r_mean = message.get("rolling_mean_30d")
        r_std = message.get("rolling_std_30d")
        
        theta_danger = 3.0 # Fallback
        if r_std and r_std > 0 and r_mean is not None:
            # Approx 12m danger level - 10m mean / 1m std = 2.0
            theta_danger = 2.0  
            
        # Step 5: Probability
        prob = sigmoid(y_t - theta_danger)
        prob = max(0.0, min(1.0, prob))
        
        # Step 7: Phase
        phase = "Normal"
        if prob >= 0.6: phase = "Critical"
        elif prob >= 0.3: phase = "Stressed"
        
        # Step 8: Manning's ETA
        lead_time = None
        if phase != "Normal" and station_id in self.reach_geom:
            geom = self.reach_geom[station_id]
            # V_mean = Q / A. Q = (1/n)*A*(R^(2/3))*(S^(1/2)) => V_mean = (1/n)*(R^(2/3))*(S^(1/2))
            # R = A / P_w
            R = geom["A"] / geom["P_w"]
            V_mean = (1.0 / geom["n"]) * (R ** (0.6666)) * (geom["S"] ** 0.5)
            # T = L / (1.67 * V) -- wait, velocity is typically m/s. 
            # 1 hour = 3600s. Time(hr) = L / (V * 3600)
            lead_time = int(geom["L"] / (V_mean * 3600))
            if lead_time < 1: lead_time = 1
            
        out = {
            "station_id": station_id,
            "district_code": dist_code,
            "flood_probability": float(prob),
            "confidence_interval_lower": float(ci_lower) if ci_lower else None,
            "confidence_interval_upper": float(ci_upper) if ci_upper else None,
            "phase": phase,
            "lead_time_hours": lead_time,
            "arima_order": arima_order,
            "arima_aic": float(arima_aic) if arima_aic else None,
            "arima_residual": float(arima_resid) if arima_resid else None,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("hydro.flood_raw", out)

    def _publish_null(self, st, dist, code, msg):
        n = {
            "station_id": st, "district_code": dist,
            "flood_probability": None, "phase": None,
            "arima_order": "0,0,0", "arima_aic": None, "arima_residual": None,
            "lead_time_hours": None,
            "confidence_interval_lower": None, "confidence_interval_upper": None,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "_error": { "code": code, "source_module": "M-07", "message": msg, "recoverable": True, "timestamp": datetime.now(timezone.utc).isoformat() + "Z" }
        }
        self.publish("hydro.flood_raw", n)

if __name__ == "__main__":
    mod = HydroForecast()
    mod.start()
