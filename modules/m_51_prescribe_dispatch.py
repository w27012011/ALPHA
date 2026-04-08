"""
M-51 PRESCRIBE-DISPATCH
PDF & Alert Dispatcher
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class PrescribeDispatch(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-51",
            input_topics=["prescribe.optimised_sequence", "pbt_signals"],
            output_topics=["intervention_sequence", "alert_message"]
        )
        self.pbt = {}

    def process(self, topic, message):
        if topic == "pbt_signals":
            self.pbt = message
        elif topic == "prescribe.optimised_sequence":
            self._process_disp(message)

    def _process_disp(self, seq_msg):
        dist = seq_msg.get("district_code")
        seq = seq_msg.get("optimised_sequence", [])
        
        stat = self.pbt.get("status", "ORANGE")
        hps = self.pbt.get("hps", 3.0)
        
        bld_pdf = False
        if stat in ["RED", "CRITICAL"] or hps >= 3.5:
            bld_pdf = True
            
        # Topic 1
        out1 = {
            "district_code": dist,
            "status": stat,
            "hps_value": float(hps),
            "rbr_value": self.pbt.get("rbr_value", 0.0),
            "sff_value": self.pbt.get("sff_score", 0.0),
            "priority_interventions": seq,
            "resources_required": [x["resources_required"] for x in seq],
            "pdf_report_generated": bld_pdf,
            "pdf_report_path": f"/reports/{dist}_{datetime.now().strftime('%Y%m%d')}.pdf" if bld_pdf else None,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("intervention_sequence", out1)
        
        # Topic 2: Alerts
        resps = []
        if bld_pdf:
            resps.append("SMS_BROADCAST")
            resps.append("DISTRICT_DC")
        resps.append("API")
        
        str_msg = f"ALERT {stat}: Execute "
        if seq:
            str_msg += seq[0]["intervention_type"]
        str_msg += f". See API."
        
        for r in resps:
            out2 = {
                "recipient_type": r,
                "message_language": "BANGLA" if r != "API" else "ENGLISH",
                "message_text": str_msg[:160],
                "severity": stat,
                "district_code": dist,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            }
            self.publish("alert_message", out2)

if __name__ == "__main__":
    mod = PrescribeDispatch()
    mod.start()
