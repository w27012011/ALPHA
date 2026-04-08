"""
M-10 PBT-HPS
Hazard Pain Signal Calculator
"""

import os
import json
import math
from datetime import datetime, timezone

try:
    import numpy as np
    LIBS_AVAILABLE = True
except ImportError:
    LIBS_AVAILABLE = False

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class PbtHps(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-10",
            input_topics=[
                "flood_predictions", "arsenic_alerts", "erosion_alerts", 
                "crop_stress", "lightning_alerts", "economic_pressure"
            ],
            output_topics=["pbt.hps_raw"]
        )
        self.latest = {}
        
        # Load Baseline
        self.baseline = {}
        try:
            with open(os.path.join(DATA_DIR, "hps_historical_baseline.json"), "r") as f:
                self.baseline = json.load(f)
        except Exception:
            pass

    def process(self, topic, message):
        dist = message.get("district_code")
        if not dist:
            return
            
        if dist not in self.latest:
            self.latest[dist] = {}
            
        self.latest[dist][topic] = message
        self._calculate_district_hps(dist)

    def _calculate_district_hps(self, dist):
        ld = self.latest[dist]
        
        # 1. Gather components (HYDRO, AQUA, GEO, AGRI, ATMO, ECON)
        x_hydro, x_aqua, x_geo, x_agri, x_atmo, x_econ = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
        n_active = 0
        
        if "flood_predictions" in ld and ld["flood_predictions"].get("flood_probability") is not None:
            x_hydro = float(ld["flood_predictions"]["flood_probability"])
            n_active += 1
            
        if "arsenic_alerts" in ld:
            msg = ld["arsenic_alerts"]
            w_flagged = msg.get("wells_flagged", 0)
            w_total = msg.get("wells_assessed", max(1, w_flagged))
            x_aqua = w_flagged / max(1, w_total)
            n_active += 1
            
        if "erosion_alerts" in ld:
            msg = ld["erosion_alerts"]
            segs = msg.get("high_risk_segments", [])
            probs = [s.get("erosion_probability", 0.0) for s in segs]
            x_geo = max(probs) if probs else 0.0
            n_active += 1
            
        if "crop_stress" in ld:
            x_agri = float(ld["crop_stress"].get("stress_index", 0.0))
            n_active += 1
            
        if "lightning_alerts" in ld:
            dens = float(ld["lightning_alerts"].get("lightning_density_strikes_km2", 0.0))
            x_atmo = min(1.0, dens / 10.0)
            n_active += 1
            
        if "economic_pressure" in ld:
            x_econ = float(ld["economic_pressure"].get("crisis_score", 0.0))
            n_active += 1

        x = [x_hydro, x_aqua, x_geo, x_agri, x_atmo, x_econ]

        if n_active < 3:
            # INSUFFICIENT_DATA
            self._publish_insufficient(dist)
            return

        # 2. Mahalanobis Distance
        hps_val = 0.0
        cov_used = "UNIT"
        mu = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]

        if dist in self.baseline and LIBS_AVAILABLE:
            try:
                base = self.baseline[dist]
                mu = base["mean"]
                cov = np.array(base["covariance"])
                
                # Active projection
                # For pendrive fallback we skip partial projection math and do full 6D using zeroes for absent engines
                x_vec = np.array(x)
                mu_vec = np.array(mu)
                
                # Singular check
                if np.linalg.det(cov) < 1e-10:
                    cov = cov + 1e-6 * np.eye(6)
                    cov_used = "PARTIAL_NUGGET"
                else:
                    cov_used = "FULL"
                    
                inv_cov = np.linalg.inv(cov)
                diff = x_vec - mu_vec
                d_sq = np.dot(np.dot(diff.transpose(), inv_cov), diff)
                
                if d_sq > 0:
                    hps_val = math.sqrt(d_sq)
            except Exception as e:
                self.logger.warning(f"Linalg failed: {e}")
                
        # Status mapping
        status = "NORMAL"
        if hps_val >= 5.0: status = "CATASTROPHIC"
        elif hps_val >= 3.5: status = "CRISIS"
        elif hps_val >= 2.5: status = "ALERT"
        elif hps_val >= 1.0: status = "ELEVATED"

        out = {
            "district_code": dist,
            "district_name": ld.get("flood_predictions", {}).get("district_name", dist),
            "hps_value": float(hps_val),
            "hps_status": status,
            "contributing_engines": {
                "HYDRO": x_hydro if "flood_predictions" in ld else None,
                "AQUA": x_aqua if "arsenic_alerts" in ld else None,
                "GEO": x_geo if "erosion_alerts" in ld else None,
                "AGRI": x_agri if "crop_stress" in ld else None,
                "ATMO": x_atmo if "lightning_alerts" in ld else None,
                "ECON": x_econ if "economic_pressure" in ld else None
            },
            "historical_mean_vector": mu,
            "current_state_vector": x,
            "covariance_matrix_used": cov_used,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("pbt.hps_raw", out)
        
    def _publish_insufficient(self, dist):
        out = {
            "district_code": dist,
            "district_name": self.latest[dist].get("flood_predictions", {}).get("district_name", dist),
            "hps_value": None,
            "hps_status": "INSUFFICIENT_DATA",
            "contributing_engines": {"HYDRO": None, "AQUA": None, "GEO": None, "AGRI": None, "ATMO": None, "ECON": None},
            "historical_mean_vector": None,
            "current_state_vector": None,
            "covariance_matrix_used": "UNIT",
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("pbt.hps_raw", out)

if __name__ == "__main__":
    mod = PbtHps()
    mod.start()
