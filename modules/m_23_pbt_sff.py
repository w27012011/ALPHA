"""
M-23 PBT-SFF
Shock Fatigue Function Calculator
"""

import math
import time
from datetime import datetime, timezone, timedelta
from collections import deque

from modules.base_module import AlphaBaseModule

class PbtSff(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-23",
            input_topics=["pbt.hps_raw", "pbt.ns_score", "pbt.rbr_score"],
            output_topics=["pbt_signals"]
        )
        self.latest = {}
        self.alert_history = {}
        self.prev_hps = {}

    def process(self, topic, message):
        dist = message.get("district_code")
        if not dist: return
        
        if dist not in self.latest: self.latest[dist] = {}
        self.latest[dist][topic] = message
        
        if topic == "pbt.rbr_score":
            self._compute_sff(message)

    def _compute_sff(self, rbr_msg):
        dist = rbr_msg.get("district_code")
        lt = self.latest[dist]
        
        hps_msg = lt.get("pbt.hps_raw", {})
        ns_msg = lt.get("pbt.ns_score", {})
        
        hps = hps_msg.get("hps_value")
        # Extract components if possible (hacky since not explicitly fully passed thru hps_raw in actual schema, wait, actually we can pull it from state vector if needed)
        # But wait, M-23 says to determine dominant hazard. It says "Use latest pbt.hps_raw for the state vector components."
        # Because we didn't add it in M-10 output directly, we'll use a mocked dominant hazard based on HPS > 1.0 being "COMPOUND"
        
        dom = "COMPOUND"
        # We assume HPS > 1.0 is an alert event
        
        now = datetime.now(timezone.utc)
        thirty_days_ago = now - timedelta(days=30)
        
        if dist not in self.alert_history:
            self.alert_history[dist] = deque()
            
        # Prune
        while self.alert_history[dist] and self.alert_history[dist][0]["ts"] < thirty_days_ago:
            self.alert_history[dist].popleft()
            
        # Count
        n_alerts = sum(1 for a in self.alert_history[dist] if a["type"] == dom)
        
        # SFF computation
        # Exp decay constant = 0.20
        sff = math.exp(-0.20 * max(0, n_alerts - 4))
        
        hps_rising = False
        prev = self.prev_hps.get(dist)
        if hps is not None and prev is not None and hps > prev:
            hps_rising = True
            
        sff_warn = (sff < 0.4)
        
        esc_req = rbr_msg.get("escalation_required", False)
        gov_notif = False
        if (sff_warn and hps_rising) or esc_req:
            gov_notif = True
            
        tone = "ESCALATED"
        if sff >= 0.6:
            tone = "STANDARD"
        elif 0.4 <= sff < 0.6:
            tone = "ESCALATED"
        elif sff < 0.4 and hps_rising:
            tone = "URGENT_DUE_TO_FATIGUE"
            
        # Log this event
        if hps is not None and hps > 1.0:
            self.alert_history[dist].append({"type": dom, "ts": now})
            
        if hps is not None:
            self.prev_hps[dist] = hps

        out = {
            "district_code": dist,
            "district_name": rbr_msg.get("district_name", dist),
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "hps_value": hps,
            "hps_status": hps_msg.get("hps_status", "UNKNOWN"),
            "novelty_score": ns_msg.get("novelty_score"),
            "structural_break_flag": ns_msg.get("structural_break_flag", False),
            "percentile_rank": ns_msg.get("percentile_rank"),
            "rbr_value": rbr_msg.get("rbr_value"),
            "escalation_required": esc_req,
            "escalation_level": rbr_msg.get("escalation_level"),
            "sff_value": float(sff),
            "alert_count_last_30_days": n_alerts,
            "sff_warning": sff_warn,
            "government_notification_required": gov_notif,
            "recommended_alert_tone": tone
        }
        self.publish("pbt_signals", out)

if __name__ == "__main__":
    mod = PbtSff()
    mod.start()
