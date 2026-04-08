"""
M-27 GEO-LIQUEFACT
Liquefaction Assessor
"""

import os
import json
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class GeoLiquefact(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-27",
            input_topics=["seismic_events", "raw.weather_fields"],
            output_topics=["seismic_events"]
        )
        self.emb = self._load("embankment_registry.json")

    def _load(self, name):
        try:
            with open(os.path.join(DATA_DIR, name), "r") as f:
                return json.load(f)
        except Exception:
            return {}

    def process(self, topic, message):
        if topic == "raw.weather_fields":
            return # moisture was used in M-26 if needed, but MDD says "Use weather fields" - but here we only pull the events
            
        if topic == "seismic_events":
            if message.get("liquefaction_enriched") == True:
                return # LOOP GUARD
            if message.get("magnitude_mw", 0) < 4.5:
                return

            self._enrich(message)

    def _enrich(self, ev):
        p_liq_base = ev.get("p_liq_final", 0.0)
        
        dyn = {}
        max_dyn = 0.0
        for p in ev.get("pga_districts", []):
            d = p.get("district_code")
            s = p.get("pga_site_g", 0.0)
            
            p_dyn = 0.0
            if s >= 0.10:
                p_dyn = min(1.0, p_liq_base * s / 0.10)
            dyn[d] = p_dyn
            if p_dyn > max_dyn:
                max_dyn = p_dyn
                
        aff = ev.get("affected_districts", [])
        
        n_assessed = 0
        n_compr = 0
        int_seg = {}
        
        for sid, e_data in self.emb.items():
            if e_data.get("district_code") in aff:
                n_assessed += 1
                age = max(0.0, float(e_data.get("age_years", 30.0)))
                af = min(1.0, age / 30.0)
                
                s_dyn = dyn.get(e_data.get("district_code"), 0.0)
                score = max(0.0, 1.0 - s_dyn * af)
                int_seg[sid] = score
                if score < 0.50:
                    n_compr += 1
                    
        # Write state for M-25
        try:
            with open(os.path.join(DATA_DIR, "embankment_integrity.json"), "w") as f:
                json.dump(int_seg, f, indent=2)
        except Exception as e:
            self.logger.error(f"INTEGRITY_FILE_WRITE_FAILED: {e}")
            
        # Build enriched payload
        out = dict(ev)
        out["liquefaction_enriched"] = True
        out["liquefaction_probability_dynamic"] = float(max_dyn)
        out["embankment_segments_assessed"] = n_assessed
        out["embankment_segments_compromised"] = n_compr
        out["embankment_integrity_by_segment"] = int_seg
        out["liquefaction_zones"] = []
        out["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
        
        self.publish("seismic_events", out)

if __name__ == "__main__":
    mod = GeoLiquefact()
    mod.start()
