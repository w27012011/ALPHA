"""
M-14 AQUA-SAFEWELL
Safe Well Locator
"""

import math
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    a = math.sin((lat2-lat1)/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2-lon1)/2)**2
    return 2 * 6371000 * math.asin(math.sqrt(a))

class AquaSafewell(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-14",
            input_topics=["aqua.well_flags"],
            output_topics=["aqua.safe_wells"]
        )

    def process(self, topic, message):
        if message.get("_error"):
            # propagate
            out = {
                "trigger_type": "UNKNOWN", "flagged_points": [], "flagged_count": 0,
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z", "_error": message["_error"]
            }
            self.publish("aqua.safe_wells", out)
            return

        radius = 2000.0
        max_alt = 3
        points = message.get("grid_points", [])
        
        safe_set = []
        flagged_set = []
        
        for pt in points:
            cls = pt.get("classification")
            if cls == "SAFE":
                safe_set.append(pt)
            elif cls == "FLAGGED":
                flagged_set.append(pt)

        flagged_out = []
        for f in flagged_set:
            try:
                f_lat, f_lon = [float(x) for x in f["grid_id"].split("_")]
            except Exception:
                continue # Parse error
                
            nearby = []
            for s in safe_set:
                try:
                    s_lat, s_lon = [float(x) for x in s["grid_id"].split("_")]
                    dist_m = haversine(f_lat, f_lon, s_lat, s_lon)
                    if dist_m <= radius:
                        nearby.append((dist_m, s, s_lat, s_lon))
                except Exception:
                    pass
            
            nearby.sort(key=lambda x: x[0])
            alts = []
            for d, s, sl, slon in nearby[:max_alt]:
                alts.append({
                    "safe_grid_id": s["grid_id"],
                    "safe_lat": sl, "safe_lon": slon,
                    "safe_district": s["district_code"],
                    "predicted_arsenic_ug_l": s.get("predicted_arsenic_ug_l"),
                    "distance_m": float(d),
                    "confidence": s.get("confidence", 0.0)
                })
                
            flagged_out.append({
                "flagged_grid_id": f["grid_id"],
                "flagged_lat": f_lat, "flagged_lon": f_lon,
                "flagged_district": f["district_code"],
                "flagged_arsenic_ug_l": f.get("predicted_arsenic_ug_l"),
                "alternatives_found": len(alts),
                "alternatives": alts
            })

        out = {
            "trigger_type": message.get("trigger_type"),
            "flagged_points": flagged_out,
            "flagged_count": len(flagged_out),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        
        if len(flagged_out) > 0 and sum(1 for fo in flagged_out if fo["alternatives_found"] == 0) == len(flagged_out):
            # No safe points available anywhere
            pass
            
        self.publish("aqua.safe_wells", out)

if __name__ == "__main__":
    mod = AquaSafewell()
    mod.start()
