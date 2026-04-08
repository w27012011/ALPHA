"""
M-33 ATMO-CAPE
Convective Instability Calculator
"""

import math
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

class AtmoCape(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-33",
            input_topics=["atmo.weather_processed"],
            output_topics=["atmo.cape_index"]
        )

    def process(self, topic, message):
        if topic == "atmo.weather_processed":
            self._process_cape(message)

    def _process_cape(self, w_msg):
        dc = w_msg.get("district_code")
        if not dc: return
        
        t_sfc = w_msg.get("surface_temp_celsius")
        t_85  = w_msg.get("temp_850hPa_celsius")
        t_50  = w_msg.get("temp_500hPa_celsius")
        t_dew = w_msg.get("dewpoint_celsius")
        p_sfc = w_msg.get("surface_pressure_hPa", 1013.25)
        
        # null flags
        if t_dew is None:
            self._publish_empty(w_msg, "DEWPOINT_MISSING")
            return
            
        if t_85 is None or t_50 is None:
            self._publish_empty(w_msg, "SOUNDING_INCOMPLETE")
            return

        t_sfc_k = t_sfc + 273.15
        t_dew_k = t_dew + 273.15
        t_85_k = t_85 + 273.15
        t_50_k = t_50 + 273.15
        
        # Bolton LCL Approximation
        try:
            val = (1.0 / (t_dew_k - 56.0)) + (math.log(t_sfc_k / t_dew_k) / 800.0)
            t_lcl_k = (1.0 / val) + 56.0
            p_lcl = p_sfc * ((t_lcl_k / t_sfc_k) ** 3.5)
        except Exception:
            self._publish_empty(w_msg, "DEWPOINT_MISSING")
            return

        # MALR Approximation params
        # To avoid hyper-complex psychrometric array iterations, use a simplified environmental MALR constant for tropics
        g = 9.81
        gamma_m = 0.005 # ~ 5 K/km standard moist adiabatic lapse rate average in lower trop
        
        z_sfc_850 = 1500.0
        z_850_500 = 4000.0
        
        # Parcel ascent temperatures
        t_parcel_850 = t_lcl_k - (gamma_m * z_sfc_850)
        t_parcel_500 = t_parcel_850 - (gamma_m * z_850_500)
        
        cape = 0.0
        lfc_p = None
        cin = 0.0
        
        # 3-level integration
        if t_parcel_850 > t_85_k:
            cape += g * ((t_parcel_850 - t_85_k) / t_85_k) * z_sfc_850
            lfc_p = 850.0
        else:
            cin += g * ((t_85_k - t_parcel_850) / t_85_k) * z_sfc_850
            
        if t_parcel_500 > t_50_k:
            cape += g * ((t_parcel_500 - t_50_k) / t_50_k) * z_850_500
            
        if cape < 0:
            cape = 0.0
            self.logger.debug("NEGATIVE_CAPE_CLAMPED")
            
        # Lifted Index
        li = t_50 - (t_parcel_500 - 273.15)
        
        el_p = 500.0 if cape > 0 else None
        
        if cape == 0.0 and cin == 0.0:
             # completely neutral
             isc = "STABLE"
        elif cape < 300: isc = "STABLE"
        elif cape < 1000: isc = "MARGINAL"
        elif cape < 2500: isc = "MODERATE"
        elif cape < 4000: isc = "SEVERE"
        else: isc = "EXTREME"

        out = {
            "district_code": dc,
            "district_name": w_msg.get("district_name", dc),
            "CAPE_J_per_kg": float(cape),
            "CIN_J_per_kg": float(cin),
            "lifted_index": float(li),
            "instability_class": isc,
            "lfc_pressure_hPa": float(lfc_p) if lfc_p is not None else None,
            "el_pressure_hPa": float(el_p) if el_p is not None else None,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("atmo.cape_index", out)

    def _publish_empty(self, w_msg, flag):
        out = {
            "district_code": w_msg.get("district_code"),
            "district_name": w_msg.get("district_name"),
            "CAPE_J_per_kg": None,
            "CIN_J_per_kg": None,
            "lifted_index": None,
            "instability_class": "UNKNOWN",
            "lfc_pressure_hPa": None,
            "el_pressure_hPa": None,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.logger.warning(flag)
        self.publish("atmo.cape_index", out)

if __name__ == "__main__":
    mod = AtmoCape()
    mod.start()
