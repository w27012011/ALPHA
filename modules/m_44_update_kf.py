"""
M-44 UPDATE-KF
Standard Kalman Filter
"""

import math
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class UpdateKf(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-44",
            input_topics=["nowcast_state", "flood_predictions", "arsenic_alerts", "seismic_events"],
            output_topics=["belief_updates"]
        )
        self.state_estimates = {}
        self.state_variances = {}
        self.innovations = {eng: [] for eng in ["AQUA", "GEO", "AGRI", "ATMO", "HYDRO"]}

    def process(self, topic, message):
        if topic == "nowcast_state":
            self._process_kf(message)

    def _process_kf(self, nowcast_msg):
        engines = nowcast_msg.get("per_engine_nowcast", {})
        
        for eng, data in engines.items():
            z_t = data.get("state_estimate", 0.0)
            conf = data.get("confidence", 0.5)
            
            # Predict
            x_pred = self.state_estimates.get(eng, 0.0)
            P_pred = self.state_variances.get(eng, 0.1) + 0.01 # Q process noise
            
            # Measurement noise R varies with confidence
            # If conf=1, noise is 0.01, if conf=0, noise is 1.01
            R = 1.0 - conf + 0.01 
            
            # Update (H=1)
            K = P_pred / (P_pred + R)
            innov = z_t - x_pred
            
            x_upd = x_pred + K * innov
            P_upd = (1.0 - K) * P_pred
            
            # Normality check surrogate
            # In a real Shapiro-Wilk we'd do a complex sort.
            # Here we track std dev of last 30 innovations
            norm = "GAUSSIAN"
            self.innovations[eng].append(innov)
            if len(self.innovations[eng]) > 30: self.innovations[eng].pop(0)
            
            if len(self.innovations[eng]) == 30:
                mu = sum(self.innovations[eng]) / 30.0
                var = sum((x - mu)**2 for x in self.innovations[eng]) / 30.0
                std = math.sqrt(var)
                # Skewness surrogate test
                skew = sum(((x - mu)**3) for x in self.innovations[eng]) / 30.0
                if std > 0: skew /= (std**3)
                
                # if skew is heavily non-gaussian
                if abs(skew) > 1.0:
                    norm = "NON_GAUSSIAN"
            
            hlth = "OPTIMAL"
            if norm == "NON_GAUSSIAN": hlth = "SWITCHING_RECOMMENDED"
            
            # 5 sigma divergence check
            sigma = math.sqrt(P_pred)
            if sigma > 0 and abs(innov) > 5.0 * sigma:
                x_upd = x_pred
                hlth = "DEGRADED"
                self.logger.warning(f"FILTER_{eng}_DIVERGENCE")
            else:
                self.state_estimates[eng] = x_upd
                self.state_variances[eng] = P_upd

            out = {
                "engine_name": eng,
                "filter_type": "KALMAN",
                "state_estimate": float(x_upd),
                "state_variance": float(P_upd),
                "kalman_gain": float(K),
                "innovation": float(innov),
                "residual_normality_test": norm,
                "filter_health": hlth,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            }
            self.publish("belief_updates", out)

if __name__ == "__main__":
    mod = UpdateKf()
    mod.start()
