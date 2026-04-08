"""
M-15 AQUA-FORMAT
AQUA Output Formatter
"""

import os
import json
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class AquaFormat(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-15",
            input_topics=["aqua.safe_wells", "aqua.well_flags"],
            output_topics=["arsenic_alerts"]
        )
        self.latest = {}
        self.meta = {}
        try:
            with open(os.path.join(DATA_DIR, "district_metadata.json"), "r") as f:
                self.meta = json.load(f)
        except Exception:
            self.logger.warning("No district_metadata.json found. Population fallbacks apply.")

    def process(self, topic, message):
        self.latest[topic] = message
        if topic == "aqua.safe_wells": # execute
            self._format_alerts()

    def _format_alerts(self):
        msg_flags = self.latest.get("aqua.well_flags", {})
        msg_safe = self.latest.get("aqua.safe_wells", {})

        if msg_flags.get("_error") and msg_safe.get("_error"):
            # Emit error for one generic district just to propagate on external bus
            out = {
                "district_code": "BD-00", "district_name": "UNKNOWN", "trigger_event": "UNKNOWN", "trigger_source_module": "M-15",
                "wells_assessed": 0, "wells_flagged": 0, "wells_critical": 0, "flagged_wells": [], "population_at_risk": 0,
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z", "_error": msg_flags["_error"]
            }
            self.publish("arsenic_alerts", out)
            return

        points = msg_flags.get("grid_points", [])
        safe_points = msg_safe.get("flagged_points", [])

        # Group by district
        d_map = {}
        for pt in points:
            dc = pt.get("district_code")
            if dc:
                d_map.setdefault(dc, []).append(pt)

        # Build safe index
        s_idx = {p["flagged_grid_id"]: p for p in safe_points}

        for dist, pts in d_map.items():
            w_assess = len(pts)
            w_flagged = sum(1 for p in pts if p.get("predicted_arsenic_ug_l", 0) > 50.0)
            w_crit = sum(1 for p in pts if p.get("predicted_arsenic_ug_l", 0) > 200.0)

            flag_records = []
            for p in pts:
                cls = p.get("classification")
                rl = "SAFE"
                vl = p.get("predicted_arsenic_ug_l", 0)
                
                if cls == "SAFE" and vl < 10: rl = "SAFE"
                elif cls == "AT_RISK" and vl < 50: rl = "ELEVATED"
                elif cls == "AT_RISK" and vl >= 50: rl = "UNSAFE"
                elif cls == "FLAGGED" and vl <= 200: rl = "UNSAFE"
                elif cls == "FLAGGED" and vl > 200: rl = "CRITICAL"
                elif vl > 50: rl = "UNSAFE"

                if rl in ("UNSAFE", "CRITICAL") and vl > 50.0:
                    gid = p["grid_id"]
                    try:
                        lt, ln = [float(x) for x in gid.split("_")]
                    except Exception:
                        lt, ln = 0.0, 0.0

                    rec = {
                        "well_id": gid,
                        "lat": lt,
                        "lon": ln,
                        "predicted_arsenic_ug_l": float(vl),
                        "baseline_arsenic_ug_l": None,
                        "mobilisation_delta_ug_l": None,
                        "risk_level": rl,
                        "confidence": float(p.get("confidence", 0)),
                        "nearest_safe_well_id": None,
                        "nearest_safe_well_distance_m": None
                    }

                    if gid in s_idx:
                        alts = s_idx[gid].get("alternatives", [])
                        if alts:
                            rec["nearest_safe_well_id"] = alts[0]["safe_grid_id"]
                            rec["nearest_safe_well_distance_m"] = alts[0]["distance_m"]

                    flag_records.append(rec)

            ttype = msg_safe.get("trigger_type", "SCHEDULED")
            tev = "SCHEDULED"
            tsrc = "M-15"
            if ttype in ("FLOOD", "BOTH", "FLOOD_DRAWDOWN"):
                tev, tsrc = "FLOOD", "M-08"
            elif ttype == "SEISMIC":
                tev, tsrc = "EARTHQUAKE", "M-26"

            pop = w_flagged * 25
            try:
                if dist in self.meta and "population" in self.meta[dist]:
                    pop = int(self.meta[dist]["population"] * (w_flagged / max(1, w_assess)))
            except Exception:
                pass

            out = {
                "district_code": dist,
                "district_name": self.meta.get(dist, {}).get("name", dist),
                "trigger_event": tev,
                "trigger_source_module": tsrc,
                "wells_assessed": w_assess,
                "wells_flagged": w_flagged,
                "wells_critical": w_crit,
                "flagged_wells": flag_records,
                "population_at_risk": pop,
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }

            self.publish("arsenic_alerts", out)

if __name__ == "__main__":
    mod = AquaFormat()
    mod.start()
