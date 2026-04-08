"""
M-35 ATMO-STORM
Storm Tracker & Lightning Alert Generator
"""

import math
from uuid import uuid4
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

class AtmoStorm(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-35",
            input_topics=["atmo.lightning_processed", "atmo.cape_index", "atmo.weather_processed"],
            output_topics=["atmo.storm_detected", "lightning_alerts"]
        )
        self.cape = {}
        self.weather = {}

    def process(self, topic, message):
        dc = message.get("district_code")
        if topic == "atmo.cape_index":
            if dc: self.cape[dc] = message
            return
            
        if topic == "atmo.weather_processed":
            if dc: self.weather[dc] = message
            return
            
        if topic == "atmo.lightning_processed":
            self._process_storm(message)

    def _process_storm(self, lgt_msg):
        d_code = lgt_msg.get("district_code")
        if not d_code: return
        
        c_msg = self.cape.get(d_code, {})
        w_msg = self.weather.get(d_code, {})
        
        cape = c_msg.get("CAPE_J_per_kg")
        dens = lgt_msg.get("strike_density_per_km2")
        kalb = lgt_msg.get("kalbaishakhi_signature_detected", False)
        
        # Threat Probability
        p_lgt = None
        
        if cape is not None and dens is not None:
            kf = 1.5 if kalb else 1.0
            dens_norm = min(1.0, float(dens) / 5.0)
            p_lgt = min(1.0, (float(cape) / 2500) * dens_norm * kf)
        elif cape is not None and dens is None:
            self.logger.debug("WWLLN_MISSING_CAPE_ONLY")
            p_lgt = min(1.0, float(cape) / 4000)
            
        # Alert Level
        al = "UNKNOWN"
        if p_lgt is not None:
            if p_lgt < 0.20: al = "LOW"
            elif p_lgt < 0.50: al = "MODERATE"
            elif p_lgt < 0.80: al = "HIGH"
            else: al = "CRITICAL"
            
        # Action
        rec = None
        if al == "LOW": rec = "MONITOR"
        elif al == "MODERATE": rec = "SEEK_SHELTER"
        elif al in ["HIGH", "CRITICAL"]: rec = "EMERGENCY_SHELTER"
        
        # Storm Classification
        st_type = "NONE"
        isc = c_msg.get("instability_class")
        
        is_storm = False
        if cape is not None and cape > 1000:
            if (dens is not None and dens > 0.5) or kalb:
                is_storm = True
                
        if is_storm:
            if kalb: st_type = "PRE_KALBAISHAKHI"
            elif isc in ["SEVERE", "EXTREME"]: st_type = "SQUALL"
            elif isc == "MODERATE": st_type = "CONVECTIVE"

        # Wind & Movement
        ws = w_msg.get("wind_speed_10m_ms")
        wd = w_msg.get("wind_direction_10m_deg")
        
        m_dir = None
        m_spd = None
        t_dang = None
        
        d_path = []
        
        if st_type != "NONE" and ws is not None and wd is not None:
            m_dir = float(wd)
            m_spd = float(ws) * 3.6 * 1.5
            if m_spd > 300.0:
                self.logger.warning("STORM_SPEED_CAPPED")
                m_spd = 200.0
                
            c_lat = lgt_msg.get("cluster_centroid_lat")
            c_lon = lgt_msg.get("cluster_centroid_lon")
            
            # Simple district union approximation
            # Since we assume district itself is hit first
            d_path.append(d_code) 
            
            if m_spd > 0:
                t_dang = 5.0 / m_spd # Approx 5km to population centers (roughly 1h for 5kmh)
                # In actual deployment, Haversine checks distance to nearest city center here, we just use a small synthetic delay
                
        # Intensity
        st_int = None
        if st_type != "NONE" and cape is not None:
            if cape < 1000: st_int = "MILD"
            elif cape < 2500: st_int = "MODERATE"
            else: st_int = "SEVERE"

        st_id = str(uuid4()) if st_type != "NONE" else None
        
        ts_now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        
        # Lightning Alert Payload
        al_out = {
            "district_code": d_code,
            "district_name": lgt_msg.get("district_name", d_code),
            "lightning_probability_next_6h": float(p_lgt) if p_lgt is not None else None,
            "lightning_density_per_km2": float(dens) if dens is not None else None,
            "alert_level": al,
            "kalbaishakhi_signature": kalb,
            "recommended_action": rec,
            "time_to_dangerous_hours": float(t_dang) if t_dang is not None else None,
            "timestamp": ts_now
        }
        self.publish("lightning_alerts", al_out)
        
        # Storm Object Payload
        st_out = {
            "storm_cell_id": st_id,
            "storm_type": st_type,
            "location_lat": float(lgt_msg.get("cluster_centroid_lat", 0)) if lgt_msg.get("cluster_centroid_lat") else None,
            "location_lon": float(lgt_msg.get("cluster_centroid_lon", 0)) if lgt_msg.get("cluster_centroid_lon") else None,
            "source_district_code": d_code if st_type != "NONE" else None,
            "movement_direction_deg": float(m_dir) if m_dir is not None else None,
            "movement_speed_kmh": float(m_spd) if m_spd is not None else None,
            "intensity": st_int,
            "time_to_dangerous_conditions_hours": float(t_dang) if t_dang is not None else None,
            "districts_in_path": d_path,
            "CAPE_J_per_kg": float(cape) if cape is not None else None,
            "kalbaishakhi_signature_detected": kalb,
            "timestamp": ts_now
        }
        self.publish("atmo.storm_detected", st_out)

if __name__ == "__main__":
    mod = AtmoStorm()
    mod.start()
