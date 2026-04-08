"""
M-17 CASC-REGIME
Cascade Regime Switcher
"""

import time
import sys
from datetime import datetime, timezone
import json
import os

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class CascRegime(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-17",
            input_topics=[
                "casc.transmission_map", "flood_predictions", "arsenic_alerts",
                "erosion_alerts", "seismic_events", "crop_stress",
                "lightning_alerts", "storm_forecast", "economic_pressure"
            ],
            output_topics=["casc.regime_state"],
            poll_interval=1.0
        )
        self.map_loaded = False
        self.tmap = None
        self.latest = {}
        self.last_ts = {
            "HYDRO": 0, "AQUA": 0, "GEO_E": 0, "GEO_S": 0,
            "AGRI": 0, "ATMO_L": 0, "ATMO_S": 0, "ECON": 0
        }
        self.prev_regime = {}

    def process(self, topic, message):
        if topic == "casc.transmission_map":
            self.tmap = message
            self.map_loaded = True
            return
            
        self.latest[topic] = message
        now = time.monotonic()
        
        # Determine engine tag
        mapping = {
            "flood_predictions": "HYDRO",
            "arsenic_alerts": "AQUA",
            "erosion_alerts": "GEO_E",
            "seismic_events": "GEO_S",
            "crop_stress": "AGRI",
            "lightning_alerts": "ATMO_L",
            "storm_forecast": "ATMO_S",
            "economic_pressure": "ECON"
        }
        
        tag = mapping.get(topic)
        if tag:
            self.last_ts[tag] = now

        if self.map_loaded:
            dist = message.get("district_code")
            if dist:
                self._compute_regimes(dist)

    def _get_regime(self, signal, t_stress, t_crit, t_casc):
        if signal < t_stress: return "STABLE"
        if signal < t_crit: return "STRESSED"
        if signal < t_casc: return "CRITICAL"
        return "CASCADE"

    def _compute_regimes(self, dist):
        now = time.monotonic()
        signals = {}
        
        # Safely pull signals if not stale
        # Hydro
        if now - self.last_ts["HYDRO"] < 21600:
            signals["HYDRO"] = self.latest.get("flood_predictions", {}).get("flood_probability", 0)
        # Aqua
        if now - self.last_ts["AQUA"] < 21600:
            aq = self.latest.get("arsenic_alerts", {})
            wa = max(1, aq.get("wells_assessed", 1))
            signals["AQUA"] = aq.get("wells_flagged", 0) / wa
        # Geo_E
        if now - self.last_ts["GEO_E"] < 21600:
            ero = self.latest.get("erosion_alerts", {})
            probs = [s.get("erosion_probability", 0) for s in ero.get("high_risk_segments", [])]
            signals["GEO_E"] = max(probs) if probs else 0
        # Geo_S
        if now - self.last_ts["GEO_S"] < 21600:
            seq = self.latest.get("seismic_events", {})
            # Simplified since seismic doesn't have district-level fields explicitly defined yet 
            signals["GEO_S"] = seq.get("mmi_intensity", 0)
        # Agri
        if now - self.last_ts["AGRI"] < 21600:
            signals["AGRI"] = self.latest.get("crop_stress", {}).get("stress_index", 0)
        # Atmo_L
        if now - self.last_ts["ATMO_L"] < 21600:
            signals["ATMO_L"] = self.latest.get("lightning_alerts", {}).get("lightning_density_strikes_km2", 0)
        # Atmo_S
        if now - self.last_ts["ATMO_S"] < 21600:
            stm = self.latest.get("storm_forecast", {})
            probs = [d.get("impact_probability", 0) for d in stm.get("districts_at_risk", [])]
            signals["ATMO_S"] = max(probs) if probs else 0
        # Econ
        if now - self.last_ts["ECON"] < 21600:
            signals["ECON"] = self.latest.get("economic_pressure", {}).get("crisis_score", 0)

        r = {}
        r["HYDRO"] = self._get_regime(signals.get("HYDRO", -1), 0.3, 0.6, 0.8) if "HYDRO" in signals else "UNKNOWN"
        r["AQUA"] = self._get_regime(signals.get("AQUA", -1), 0.2, 0.4, 0.6) if "AQUA" in signals else "UNKNOWN"
        r["AGRI"] = self._get_regime(signals.get("AGRI", -1), 0.2, 0.4, 0.6) if "AGRI" in signals else "UNKNOWN"
        r["ECON"] = self._get_regime(signals.get("ECON", -1), 0.2, 0.4, 0.6) if "ECON" in signals else "UNKNOWN"
        
        geo_e = self._get_regime(signals.get("GEO_E", -1), 0.3, 0.5, 0.7) if "GEO_E" in signals else "UNKNOWN"
        geo_s = self._get_regime(signals.get("GEO_S", -1), 4, 6, 8) if "GEO_S" in signals else "UNKNOWN"
        rank = {"UNKNOWN":0, "STABLE":1, "STRESSED":2, "CRITICAL":3, "CASCADE":4}
        r["GEO"] = geo_e if rank[geo_e] > rank[geo_s] else geo_s
        
        atmo_l = self._get_regime(signals.get("ATMO_L", -1), 1.0, 3.0, 6.0) if "ATMO_L" in signals else "UNKNOWN"
        atmo_s = self._get_regime(signals.get("ATMO_S", -1), 0.2, 0.5, 0.75) if "ATMO_S" in signals else "UNKNOWN"
        r["ATMO"] = atmo_s if rank[atmo_s] > rank[atmo_l] else atmo_l
        if rank[r["ATMO"]] == 0 and ("ATMO_S" in signals or "ATMO_L" in signals):
            r["ATMO"] = "STABLE"

        events = []
        if dist not in self.prev_regime:
            self.prev_regime[dist] = {k: "UNKNOWN" for k in r.keys()}
            
        for eng, regime in r.items():
            if regime != self.prev_regime[dist][eng]:
                events.append({
                    "engine": eng, "from_regime": self.prev_regime[dist][eng], "to_regime": regime,
                    "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
                })
                self.prev_regime[dist][eng] = regime

        active = [k for k, v in r.items() if v == "CASCADE"]
        
        updates = {}
        for act in active:
            for p in self.tmap.get("pairs", []):
                if p["source_hazard"] == act:
                    # Look for HPS or use default 1.0
                    updates[p["target_hazard"]] = min(1.0, p["base_transmission_probability"] * (1 + 1.0 / 5.0))

        out = {
            "district_code": dist,
            "district_name": self.latest.get("flood_predictions", {}).get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "per_engine_regime": r,
            "active_cascade_engines": active,
            "transition_probability_updates": updates,
            "regime_change_events": events
        }
        
        self.publish("casc.regime_state", out)

if __name__ == "__main__":
    mod = CascRegime()
    mod.start()
