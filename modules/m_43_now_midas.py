"""
M-43 NOW-MIDAS
System-Wide MIDAS Nowcaster
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class NowMidas(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-43",
            input_topics=["hydro.nowcast_state", "raw.weather_fields", "raw.ndvi_grid", "raw.sar_displacement", "raw.earthquake_events"],
            output_topics=["nowcast_state"]
        )
        self.hydro = {}
        self.w_fields = {}
        self.ndvi = {}
        self.sar = {}
        self.quake = {}
        self.last_ts = {
            "AQUA": datetime.min.replace(tzinfo=timezone.utc),
            "GEO": datetime.min.replace(tzinfo=timezone.utc),
            "AGRI": datetime.min.replace(tzinfo=timezone.utc),
            "ATMO": datetime.min.replace(tzinfo=timezone.utc),
            "HYDRO": datetime.min.replace(tzinfo=timezone.utc)
        }
        self.last_state = {
            "AQUA": 0.0,
            "GEO": 0.0,
            "AGRI": 0.0,
            "ATMO": 0.0,
            "HYDRO": 0.0
        }

    def process(self, topic, message):
        nt = datetime.now(timezone.utc)
        
        if topic == "hydro.nowcast_state":
            self.hydro = message
            self.last_state["HYDRO"] = message.get("state_estimate", self.last_state["HYDRO"])
            self.last_ts["HYDRO"] = nt
            
        elif topic == "raw.weather_fields":
            self.w_fields = message
            cape = message.get("cape_j_kg", 0.0)
            # Normalise approx cape 0 to 4000
            self.last_state["ATMO"] = min(1.0, float(cape) / 4000.0)
            self.last_ts["ATMO"] = nt
            
        elif topic == "raw.ndvi_grid":
            self.ndvi = message
            n_anom = message.get("ndvi_anomaly", 0.0)
            self.last_state["AGRI"] = min(1.0, abs(float(n_anom)))
            self.last_ts["AGRI"] = nt
            
        elif topic == "raw.sar_displacement":
            self.sar = message
            sd = message.get("displacement_rate", 0.0)
            self.last_state["GEO"] = min(1.0, abs(float(sd)))
            self.last_ts["GEO"] = nt
            
        elif topic == "raw.earthquake_events":
            self.quake = message
            mag = message.get("magnitude", 0.0)
            self.last_state["GEO"] = min(1.0, float(mag) / 8.0)
            self.last_ts["GEO"] = nt
            
        self._process_nowcast()

    def _process_nowcast(self):
        nt = datetime.now(timezone.utc)
        
        out = {
            "per_engine_nowcast": {},
            "timestamp": nt.isoformat().replace("+00:00", "Z")
        }
        
        for eng in ["AQUA", "GEO", "AGRI", "ATMO", "HYDRO"]:
            st_d = (nt - self.last_ts[eng]).total_seconds() / 3600.0 # hours
            stale = st_d if self.last_ts[eng] != datetime.min.replace(tzinfo=timezone.utc) else 999.0
            
            conf = 0.0
            if stale > 48.0: conf = 0.3
            else: conf = max(0.0, 1.0 - (stale / 100.0))
            
            p_used = "DEFAULT"
            if eng == "HYDRO": p_used = "hydro.nowcast_state"
            elif eng == "ATMO": p_used = "raw.weather_fields/CAPE"
            elif eng == "AGRI": p_used = "raw.ndvi_grid/ANOMALY"
            elif eng == "GEO": p_used = "raw.sar_displacement/EARTHQUAKE"
            elif eng == "AQUA": p_used = "flood_probability [STATIC]"
            
            p_ts = self.last_ts[eng].isoformat().replace("+00:00", "Z")
            if self.last_ts[eng] == datetime.min.replace(tzinfo=timezone.utc):
                p_ts = "DEFAULT"
            
            out["per_engine_nowcast"][eng] = {
                "state_estimate": float(self.last_state[eng]),
                "confidence": float(conf),
                "proxy_used": p_used,
                "proxy_timestamp": p_ts,
                "staleness_hours": float(stale)
            }
            
        self.publish("nowcast_state", out)

if __name__ == "__main__":
    mod = NowMidas()
    mod.start()
