"""
M-13 AQUA-CLASSIFY
Arsenic Risk Classifier
"""

import os
import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

class AquaClassify(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-13",
            input_topics=["aqua.mobilisation_risk"],
            output_topics=["aqua.well_flags"]
        )

    def process(self, topic, message):
        if message.get("_error"):
            # propagate
            out = {
                "trigger_type": "UNKNOWN",
                "grid_points": [], 
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "_error": message["_error"]
            }
            self.publish("aqua.well_flags", out)
            return

        points_out = []
        
        for pt in message.get("grid_points", []):
            log_as = pt.get("log_arsenic_predicted")
            var_as = pt.get("kriging_variance")
            mob_prob = pt.get("mobilisation_probability")
            
            if log_as is None or var_as is None:
                continue

            # Core Execution: Back-Transform
            # predicted = exp(log_arsenic + 0.5 * variance)
            ug_l = None
            if log_as < 20: 
                ug_l = math.exp(log_as + 0.5 * var_as)
                if ug_l > 10000:
                    ug_l = None
            
            if ug_l is None:
                continue

            # Classification
            classification = "AT_RISK"
            if ug_l < 10.0 and (mob_prob is None or mob_prob < 0.20):
                classification = "SAFE"
            elif ug_l > 50.0 or (mob_prob is not None and mob_prob > 0.60):
                classification = "FLAGGED"

            # Confidence
            conf = 1.0 - (var_as / 3.0)
            if mob_prob is None:
                conf = min(conf, 0.4)
            conf = max(0.0, conf)

            points_out.append({
                "grid_id": pt.get("grid_id"),
                "district_code": pt.get("district_code"),
                "predicted_arsenic_ug_l": float(ug_l),
                "kriging_variance": float(var_as), # preserve log-space variance
                "mobilisation_probability": mob_prob,
                "classification": classification,
                "confidence": float(conf),
                "below_detection": pt.get("below_detection", False),
                "low_data_density": pt.get("low_data_density", False)
            })

        out = {
            "trigger_type": message.get("trigger_type"),
            "grid_points": points_out,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("aqua.well_flags", out)

if __name__ == "__main__":
    mod = AquaClassify()
    mod.start()
