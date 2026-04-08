"""
M-45 UPDATE-UKF
Unscented Kalman Filter
"""

import math
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class UpdateUkf(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-45",
            input_topics=["nowcast_state", "pbt.ns_score"],
            output_topics=["belief_updates"]
        )
        self.ns = 0.0
        self.state_estimates = {}
        self.state_covariances = {}

    def process(self, topic, message):
        if topic == "pbt.ns_score":
            self.ns = message.get("novelty_score", 0.0)
        elif topic == "nowcast_state":
            self._process_ukf(message)

    def _process_ukf(self, nowcast_msg):
        # We only run UKF if NS > 0.80 or specifically requested (assume NS check)
        if self.ns <= 0.80:
            return
            
        engines = nowcast_msg.get("per_engine_nowcast", {})
        
        for eng, data in engines.items():
            z_t = data.get("state_estimate", 0.0)
            
            x_pred = self.state_estimates.get(eng, 0.0)
            cov_pred = self.state_covariances.get(eng, 0.1) + 0.05
            if cov_pred <= 0:
                cov_pred = 1e-6
                self.logger.warning("COV_NON_POSITIVE")
                
            # Sigma points for 1D (n=1, 2n+1=3 points)
            # x0 = x, x1 = x + sqrt(cov), x2 = x - sqrt(cov)
            n_sg = 3
            sg = math.sqrt(cov_pred)
            x_pts = [x_pred, x_pred + sg, x_pred - sg]
            
            # Non-linear assumed f(x): clamp to 0..1 curve slightly
            def f_x(val):
                return max(0.0, min(1.0, val * 1.05)) # Simple mild non-linearity
                
            y_pts = [f_x(pt) for pt in x_pts]
            
            # Weights W0=0, W1=0.5, W2=0.5
            x_mean = 0 * y_pts[0] + 0.5 * y_pts[1] + 0.5 * y_pts[2]
            
            cov_mean = 0.5 * (y_pts[1] - x_mean)**2 + 0.5 * (y_pts[2] - x_mean)**2
            
            # Non-linearity score measurement divergence
            nl = abs(x_mean - f_x(x_pred)) / (sg + 1e-6)
            nl_score = min(1.0, nl)
            
            # Apply measurement
            R = 1.0 - data.get("confidence", 0.5) + 0.01
            K = cov_mean / (cov_mean + R)
            
            x_upd = x_mean + K * (z_t - x_mean)
            cov_upd = (1.0 - K) * cov_mean
            
            hlth = "OPTIMAL"
            if nl_score > 0.8:
                hlth = "SWITCHING_RECOMMENDED"
                
            self.state_estimates[eng] = x_upd
            self.state_covariances[eng] = max(1e-6, cov_upd)
            
            out = {
                "engine_name": eng,
                "filter_type": "UNSCENTED_KALMAN",
                "state_estimate": float(x_upd),
                "state_covariance": float(self.state_covariances[eng]),
                "sigma_points_used": n_sg,
                "nonlinearity_score": float(nl_score),
                "filter_health": hlth,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            }
            self.publish("belief_updates", out)

if __name__ == "__main__":
    mod = UpdateUkf()
    mod.start()
