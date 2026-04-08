"""
M-32 ATMO-ERA5
Atmospheric Field Preprocessor
"""

import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

class AtmoEra5(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-32",
            input_topics=["raw.weather_fields"],
            output_topics=["atmo.weather_processed"]
        )
        self.last_sst = None

    def process(self, topic, message):
        if topic == "raw.weather_fields":
            self._process_era5(message)

    def _process_era5(self, wf_msg):
        districts = wf_msg.get("districts", [])
        if not districts: return
        
        # M-02 pushes Bob SST inside the raw feed array sometimes, or we cached it
        sst = None
        for d in districts: 
            s = d.get("surface_temp_celsius") # Assume hack if M-02 passes a "BAY_OF_BENGAL" mock payload
            if d.get("district_code") == "BAY_OF_BENGAL" and s is not None:
                sst = float(s)
                break
                
        if sst is not None:
            self.last_sst = sst
        bay_sst = self.last_sst

        for d in districts:
            dc = d.get("district_code")
            if dc == "BAY_OF_BENGAL": continue
            if not dc: continue
            
            u_10 = d.get("u_wind_10m_ms")
            v_10 = d.get("v_wind_10m_ms")
            u_85 = d.get("u_wind_850hPa_ms")
            v_85 = d.get("v_wind_850hPa_ms")
            u_50 = d.get("u_wind_500hPa_ms")
            v_50 = d.get("v_wind_500hPa_ms")
            
            temp_sfc = d.get("surface_temp_celsius")
            temp_85 = d.get("temp_850hPa_celsius")
            temp_50 = d.get("temp_500hPa_celsius")
            rh = d.get("relative_humidity_pct")
            pres = d.get("surface_pressure_hPa")
            sm = d.get("soil_moisture_m3m3")
            precip = d.get("precipitation_mm_3h")

            # Validate Wind ranges
            def c_wd(val):
                if val is None: return None
                v = float(val)
                if abs(v) > 100.0:
                    self.logger.warning("WIND_COMPONENT_EXTREME")
                    v = max(-100.0, min(100.0, v))
                return v

            u_10, v_10 = c_wd(u_10), c_wd(v_10)
            u_85, v_85 = c_wd(u_85), c_wd(v_85)
            u_50, v_50 = c_wd(u_50), c_wd(v_50)

            ws_10 = None
            wd_10 = None
            if u_10 is not None and v_10 is not None:
                ws_10 = math.sqrt(u_10**2 + v_10**2)
                wd_10 = (math.degrees(math.atan2(-u_10, -v_10))) % 360

            ws_85 = None
            if u_85 is not None and v_85 is not None:
                ws_85 = math.sqrt(u_85**2 + v_85**2)

            ws_50 = None
            if u_50 is not None and v_50 is not None:
                ws_50 = math.sqrt(u_50**2 + v_50**2)

            wsh = None
            # Wind Shear 5km layer approx
            if ws_85 is not None and ws_50 is not None:
                wsh = abs(ws_85 - ws_50) / 5.0
                
            tdew = None
            # Magnus Dewpoint Approximation
            if temp_sfc is not None and rh is not None:
                if rh > 0:
                    rh_val = float(rh)
                    b = 17.625
                    c = 243.04
                    g = (b * temp_sfc) / (c + temp_sfc) + math.log(rh_val / 100.0)
                    if (b - g) != 0:
                        tdew = (c * g) / (b - g)
                else:
                    self.logger.debug("RELATIVE_HUMIDITY_AT_ZERO")

            twb = None
            # Stull Empirical Wet Bulb Formula
            if temp_sfc is not None and rh is not None and rh > 0:
                t = float(temp_sfc)
                r = float(rh)
                term1 = t * math.atan(0.151977 * math.sqrt(r + 8.313659))
                term2 = math.atan(t + r)
                term3 = - math.atan(r - 1.676331)
                term4 = 0.00391838 * (r ** 1.5) * math.atan(0.023101 * r)
                term5 = - 4.686035
                twb = term1 + term2 + term3 + term4 + term5

            out = {
                "district_code": dc,
                "district_name": d.get("district_name", dc),
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "surface_temp_celsius": float(temp_sfc) if temp_sfc is not None else None,
                "temp_850hPa_celsius": float(temp_85) if temp_85 is not None else None,
                "temp_500hPa_celsius": float(temp_50) if temp_50 is not None else None,
                "relative_humidity_pct": float(rh) if rh is not None else None,
                "surface_pressure_hPa": float(pres) if pres is not None else None,
                "wind_speed_10m_ms": float(ws_10) if ws_10 is not None else None,
                "wind_direction_10m_deg": float(wd_10) if wd_10 is not None else None,
                "wind_speed_850hPa_ms": float(ws_85) if ws_85 is not None else None,
                "wind_speed_500hPa_ms": float(ws_50) if ws_50 is not None else None,
                "wind_shear_ms_per_km": float(wsh) if wsh is not None else None,
                "dewpoint_celsius": float(tdew) if tdew is not None else None,
                "wet_bulb_celsius": float(twb) if twb is not None else None,
                "precipitation_mm_3h": float(precip) if precip is not None else None,
                "soil_moisture_m3m3": float(sm) if sm is not None else None,
                "bay_of_bengal_sst_celsius": float(bay_sst) if bay_sst is not None else None
            }
            self.publish("atmo.weather_processed", out)

if __name__ == "__main__":
    mod = AtmoEra5()
    mod.start()
