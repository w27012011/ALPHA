"""
M-49 PRESCRIBE-SELECT
Intervention Selector
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class PrescribeSelect(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-49",
            input_topics=["pbt_signals", "cascade_events", "flood_predictions", "arsenic_alerts", "erosion_alerts", "crop_stress", "economic_pressure"],
            output_topics=["prescribe.ranked_interventions"]
        )
        self.casc = {}
        self.flood = {}
        self.rbr = 0.5 
        self.sff = 0.5
        
        # Library
        self.lib = [
            {"type": "ACTIVATE_SHELTER", "lead": 12.0, "res": 3, "haz": "CYCLONE", "lives": 100},
            {"type": "DEPLOY_SANDBAGS", "lead": 48.0, "res": 1, "haz": "FLOOD", "lives": 20},
            {"type": "SUPPLY_WATER_FILTERS", "lead": 24.0, "res": 2, "haz": "ARSENIC", "lives": 50},
            {"type": "EMERGENCY_HARVEST", "lead": 24.0, "res": 4, "haz": "CROP", "lives": 80},
            {"type": "RELEASE_STRATEGIC_FUEL", "lead": 72.0, "res": 5, "haz": "ECON", "lives": 0}
        ]

    def process(self, topic, message):
        if topic == "cascade_events": self.casc = message
        elif topic == "economic_pressure": self.rbr = 0.8 # Mock link
        elif topic == "pbt_signals": 
            self._process_select(message)

    def _process_select(self, pbt_msg):
        status = pbt_msg.get("status", "GREEN")
        
        if status not in ["ORANGE", "RED"]:
            return
            
        rbr = pbt_msg.get("rbr_value", 1.0)
        sff = pbt_msg.get("sff_score", 1.0)
        hps = pbt_msg.get("hps", 3.0)
        
        av_time = 48.0 # assume 48 hr warning
        
        feas = []
        for i in self.lib:
            f_sc = 0.0
            t_f = 1.0 if av_time >= i["lead"] else 0.0
            
            f_sc = min(1.0, rbr * 1.0 * t_f)
            if rbr == 0 and i["res"] < 4:
                f_sc = 0.0 # No local resources
            elif rbr == 0 and i["res"] >= 4:
                f_sc = 1.0 # National 
                
            if f_sc > 0:
                feas.append({
                    "intervention_type": i["type"],
                    "lead_time_required_hours": i["lead"],
                    "resource_level": "LEVEL_" + str(i["res"]),
                    "applicable_hazard": i["haz"],
                    "feasibility_score": float(f_sc)
                })

        out = {
            "district_code": pbt_msg.get("district_code", "BD-XX"),
            "status": status,
            "hps_value": float(hps),
            "rbr_value": float(rbr),
            "sff_value": float(sff),
            "feasible_interventions": feas,
            "cascade_paths_targeted": [],
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("prescribe.ranked_interventions", out)

if __name__ == "__main__":
    mod = PrescribeSelect()
    mod.start()
