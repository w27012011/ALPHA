"""
M-18 CASC-DETECT
Active Cascade Detector
"""

import time
from datetime import datetime, timezone
import json

from modules.base_module import AlphaBaseModule

class CascDetect(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-18",
            input_topics=["casc.regime_state", "casc.transmission_map"],
            output_topics=["casc.active_transmissions"]
        )
        self.latest = {}

    def process(self, topic, message):
        self.latest[topic] = message
        
        if topic == "casc.regime_state":
            self._detect_cascades(message)

    def _detect_cascades(self, regime_msg):
        dist = regime_msg.get("district_code")
        active = []
        
        tmap = self.latest.get("casc.transmission_map")
        if not tmap:
            self._publish_empty(dist, regime_msg)
            return

        active_engines = regime_msg.get("active_cascade_engines", [])
        if not active_engines:
            self._publish_empty(dist, regime_msg)
            return

        updates = regime_msg.get("transition_probability_updates", {})
        per_regime = regime_msg.get("per_engine_regime", {})
        
        for p in tmap.get("pairs", []):
            sh = p.get("source_hazard")
            th = p.get("target_hazard")
            if sh in active_engines:
                p_base = float(p.get("base_transmission_probability", 0))
                lstd = float(p.get("lag_std_days", 0))
                lmean = float(p.get("lag_mean_days", 1))
                if lmean == 0: lmean = 0.001
                
                sig_hist = p_base * (lstd / lmean)
                p_upd = float(updates.get(th, p_base))
                
                if p_upd > p_base + sig_hist:
                    active.append({
                        "source_hazard": sh, "target_hazard": th,
                        "transmission_probability": p_upd,
                        "lag_peak_days": float(p.get("lag_peak_days", 0)),
                        "lag_distribution": {
                            "min_days": max(0.0, float(p.get("lag_peak_days", 0)) - lstd),
                            "peak_days": float(p.get("lag_peak_days", 0)),
                            "max_days": float(p.get("lag_peak_days", 0)) + 2*lstd
                        },
                        "mechanism_description": p.get("mechanism_description", ""),
                        "source_engine_regime": "CASCADE",
                        "target_engine_current_regime": per_regime.get(th, "UNKNOWN")
                    })

        out = {
            "district_code": dist,
            "district_name": regime_msg.get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "active_count": len(active),
            "active_transmissions": active
        }
        self.publish("casc.active_transmissions", out)

    def _publish_empty(self, dist, msg):
        out = {
            "district_code": dist,
            "district_name": msg.get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "active_count": 0,
            "active_transmissions": []
        }
        self.publish("casc.active_transmissions", out)

if __name__ == "__main__":
    mod = CascDetect()
    mod.start()
