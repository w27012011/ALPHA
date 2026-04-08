"""
M-09 HYDRO-MIDAS
Hydrological Mixed-Data Sampling Nowcaster
"""

import os
import json
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

try:
    import numpy as np
    from scipy.linalg import pinv
    LIBS_AVAILABLE = True
except ImportError:
    LIBS_AVAILABLE = False
    def sigmoid(x):
        return 1 / (1 + math.exp(-x))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class HydroMidas(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-09",
            input_topics=["hydro.water_normalized", "raw.ndvi_grid"],
            output_topics=["hydro.nowcast_state"]
        )
        self.latest_ndvi = {}
        self.ndvi_baseline = {}
        
        try:
            with open(os.path.join(DATA_DIR, "ndvi_baseline.json"), "r") as f:
                self.ndvi_baseline = json.load(f)
        except Exception:
            self.logger.error("Missing baseline NDVI data.")
            
        self.kf_state = {}

    def process(self, topic, message):
        if topic == "raw.ndvi_grid":
            self.latest_ndvi["global"] = message
            return
            
        if topic == "hydro.water_normalized":
            self._execute_midas(message)
            
    def _execute_midas(self, gauge_msg):
        dist_code = gauge_msg.get("district_code", "BD-00")
        
        gauge_znorm = gauge_msg.get("water_level_normalised")
        if gauge_znorm is None:
            return
            
        ndvi_msg = self.latest_ndvi.get("global")
        # Staleness computation
        staleness_hours = 24.0 # default if absent
        
        state_est = None
        conf = 0.9
        proxy_used = "KF_ONLY"
        
        baseline_ndvi = self.ndvi_baseline.get(dist_code, 0.6)
        
        if LIBS_AVAILABLE and ndvi_msg: # Live Matrix Math Midas Model
            try:
                # E.g. find NDVI cell matching district
                cells = ndvi_msg.get("grid_cells", [])
                dist_cells = [c["ndvi"] for c in cells if c.get("district") == dist_code and c.get("ndvi") is not None]
                if dist_cells:
                    current_ndvi = sum(dist_cells) / len(dist_cells)
                else:
                    current_ndvi = baseline_ndvi
                    
                ndvi_proxy = -(current_ndvi - baseline_ndvi)
                
                # Synthetic MIDAS matrix solve simulating weight regression:
                # ŷ_t = α + Σ(β)
                # In full implementation, this uses historical matrices. For pendrive online block:
                # We simply approximate the weighted sum logic based on the equation formulas:
                alpha = 0.1
                beta_weighted = ndvi_proxy * 1.5 + (gauge_znorm * 0.8)
                y_t = alpha + beta_weighted
                
                # Sigmoid output
                state_est = 1 / (1 + np.exp(-y_t))
                proxy_used = "gauge_3h + NDVI_8d"
            except Exception as e:
                self.logger.warning(f"MIDAS regression failed: {e}")
                
        # KF Fallback / Carry Forward
        if state_est is None:
            # We predict state forward using previous KF state
            prev = self.kf_state.get(dist_code, 0.1) # identity F=I
            # We add slight Kalman update based on the raw gauge info
            gauge_simulated_prob = 1 / (1 + math.exp(-gauge_znorm))
            state_est = prev * 0.5 + gauge_simulated_prob * 0.5  # Filter equation
            conf = max(0.3, conf - 0.2)
            
        self.kf_state[dist_code] = state_est
        
        out = {
            "district_code": dist_code,
            "state_estimate": float(state_est),
            "confidence": float(conf),
            "proxy_used": proxy_used,
            "midas_coefficients": None, # Removed to save Redis size per MDD
            "staleness_hours": float(staleness_hours),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        
        self.publish("hydro.nowcast_state", out)

if __name__ == "__main__":
    mod = HydroMidas()
    mod.start()
