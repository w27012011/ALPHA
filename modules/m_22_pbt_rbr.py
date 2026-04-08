"""
M-22 PBT-RBR
Resilience Buffer Ratio Calculator
"""

import os
import json
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class PbtRbr(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-22",
            input_topics=["pbt.hps_raw", "cascade_events"],
            output_topics=["pbt.rbr_score"]
        )
        self.latest = {}
        self.capacity = {}
        try:
            with open(os.path.join(DATA_DIR, "district_capacity.json"), "r") as f:
                self.capacity = json.load(f)
        except Exception:
            pass
            
        self.defaults = {
            "BD-26": 0.75, "BD-15": 0.75, "BD-06": 0.75, "BD-55": 0.75,
            "BD-20": 0.30, "BD-21": 0.30, "BD-36": 0.30, "BD-38": 0.30, "BD-40": 0.30
        }

    def process(self, topic, message):
        self.latest[topic] = message
        if topic == "pbt.hps_raw":
            self._compute_rbr(message)

    def _compute_rbr(self, hps_msg):
        dist = hps_msg.get("district_code")
        if not dist: return
        
        hps = hps_msg.get("hps_value")
        
        if hps is None or hps == 0.0:
            out = {
                "district_code": dist, "district_name": hps_msg.get("district_name", dist),
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "rbr_value": None, "local_capacity_score": 0.0, "local_capacity_source": "ESTIMATED",
                "hps_value": hps, "cascade_depth": 1, "escalation_required": False, "escalation_level": None
            }
            self.publish("pbt.rbr_score", out)
            return
            
        c_source = "ESTIMATED"
        c_local = self.defaults.get(dist, 0.50)
        if dist in self.capacity:
            c_local = float(self.capacity[dist])
            c_source = "LOOKUP"
            
        casc = self.latest.get("cascade_events", {})
        d_casc = 1
        if casc.get("district_code") == dist:
            d_casc = max(1, casc.get("max_cascade_depth", 1))
            
        rbr = c_local / (hps * d_casc)
        
        esc_req = False
        esc_lvl = "NONE"
        
        if rbr < 0.5:
            esc_req = True
            esc_lvl = "NATIONAL"
        elif rbr < 1.0:
            esc_req = True
            esc_lvl = "DISTRICT"
            
        out = {
            "district_code": dist,
            "district_name": hps_msg.get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "rbr_value": float(rbr),
            "local_capacity_score": float(c_local),
            "local_capacity_source": c_source,
            "hps_value": float(hps),
            "cascade_depth": int(d_casc),
            "escalation_required": esc_req,
            "escalation_level": esc_lvl
        }
        self.publish("pbt.rbr_score", out)

if __name__ == "__main__":
    mod = PbtRbr()
    mod.start()
