"""
M-50 PRESCRIBE-OPT
Intervention Optimiser
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class PrescribeOpt(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-50",
            input_topics=["prescribe.ranked_interventions", "pbt_signals"],
            output_topics=["prescribe.optimised_sequence"]
        )

    def process(self, topic, message):
        if topic == "prescribe.ranked_interventions":
            self._process_opt(message)

    def _process_opt(self, r_msg):
        feas = r_msg.get("feasible_interventions", [])
        if not feas: return
        
        sff = r_msg.get("sff_value", 1.0)
        sff_w = max(0.5, sff)
        
        lst = []
        for f in feas:
            # Mock Lives
            lives = 0
            t = f["intervention_type"]
            rl = int(f["resource_level"].split("_")[1])
            if t == "ACTIVATE_SHELTER": lives = 100
            elif t == "DEPLOY_SANDBAGS": lives = 20
            elif t == "SUPPLY_WATER_FILTERS": lives = 50
            elif t == "EMERGENCY_HARVEST": lives = 80
            
            c_bonus = 1.0
            if "FLOOD" in f["applicable_hazard"]: c_bonus = 1.5
            
            sc = (lives / max(1, rl)) * sff_w * c_bonus
            
            lst.append({
                "intervention_type": t,
                "score": sc,
                "action_within_hours": f["lead_time_required_hours"],
                "resources_required": f["resource_level"],
                "sff_adjusted": bool(sff < 1.0)
            })
            
        lst.sort(key=lambda x: x["score"], reverse=True)
        
        seq = []
        p = 1
        for i in lst:
            seq.append({
                "priority_rank": p,
                "intervention_type": i["intervention_type"],
                "action_within_hours": i["action_within_hours"],
                "reason_for_ranking": "High Value/Cost",
                "resources_required": i["resources_required"],
                "if_no_action_consequence": "Severe",
                "sff_adjusted": i["sff_adjusted"]
            })
            p += 1
            
        out = {
            "district_code": r_msg.get("district_code"),
            "optimised_sequence": seq,
            "joint_expected_benefit_score": sum(x["score"] for x in lst),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("prescribe.optimised_sequence", out)

if __name__ == "__main__":
    mod = PrescribeOpt()
    mod.start()
