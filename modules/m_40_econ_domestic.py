"""
M-40 ECON-DOMESTIC
ECON-DOMESTIC Engine
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class EconDomestic(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-40",
            input_topics=["econ.pps_signal", "flood_predictions", "storm_forecast", "crop_stress", "arsenic_alerts", "econ.demand_profile", "econ.reserve_status"],
            output_topics=["econ.domestic_output"]
        )
        self.pps = {}
        self.flood = {}
        self.storm = {}
        self.crop = {}
        self.arsenic = {}
        self.dem = {}
        self.res = {}

    def process(self, topic, message):
        if topic == "flood_predictions": self.flood = message
        elif topic == "storm_forecast": self.storm = message
        elif topic == "crop_stress": self.crop = message
        elif topic == "arsenic_alerts": self.arsenic = message
        elif topic == "econ.demand_profile": self.dem[message.get("commodity")] = message
        elif topic == "econ.reserve_status": self.res[message.get("commodity")] = message
        elif topic == "econ.pps_signal":
            self.pps[message.get("commodity")] = message
            self._process_domestic()

    def _process_domestic(self):
        f_p = self.flood.get("flood_probability", 0.0)
        c_act = self.storm.get("cyclone_active", False) or self.storm.get("chittagong_port_operability", "NORMAL") != "NORMAL"
        cr_s = self.crop.get("stress_index", 0.0)
        a_p = self.arsenic.get("district_mobilisation_probability", 0.0)
        
        c_count = 0
        if f_p >= 0.30: c_count += 1
        if c_act: c_count += 1
        if cr_s is not None and cr_s >= 0.20: c_count += 1
        if a_p is not None and a_p >= 0.30: c_count += 1
        
        scen = "NONE"
        if c_count >= 2: scen = "COMBINED"
        elif f_p >= 0.30: scen = "FLOOD_ROAD_DISRUPTION"
        elif c_act: scen = "CYCLONE_PORT_DISRUPTION"
        elif cr_s is not None and cr_s >= 0.20: scen = "CROP_LOSS"
        elif a_p is not None and a_p >= 0.30: scen = "ARSENIC_CRISIS"
        
        # Fuel Disruption
        r_elv = 0.30
        r_dmg = f_p * (1.0 - r_elv)
        
        i_dep_fuel = 1.0 # Assume 100% config M-37
        if "PETROLEUM" in self.dem:
            i_dep_fuel = self.dem["PETROLEUM"].get("import_dependence_pct", 1.0)
            
        fuel_dis = r_dmg * i_dep_fuel
        
        if c_act:
            c_cap = self.storm.get("import_capacity_pct", 100)
            p_dis = 1.0 - (float(c_cap) / 100.0)
            fuel_dis = min(1.0, fuel_dis + p_dis * i_dep_fuel)
            
        # Surges
        sf_fuel = 1.0
        sf_food = 1.0
        
        # Pull baseline multipliers
        sf_fac_fuel = {"FLOOD_MAJOR": 2.00, "CYCLONE_PORT": 1.35, "CROP_FAILURE": 1.05}
        if "PETROLEUM" in self.dem:
            sf_fac_fuel.update(self.dem["PETROLEUM"].get("demand_surge_factors", {}))
            
        sf_fac_food = {"FLOOD_MAJOR": 1.23, "CYCLONE_PORT": 1.15, "CROP_FAILURE": 1.40}
        if "FOOD_STAPLES" in self.dem:
            sf_fac_food.update(self.dem["FOOD_STAPLES"].get("demand_surge_factors", {}))
            
        if scen == "FLOOD_ROAD_DISRUPTION":
            sf_fuel = float(sf_fac_fuel["FLOOD_MAJOR"])
            sf_food = float(sf_fac_food["FLOOD_MAJOR"])
        elif scen == "CYCLONE_PORT_DISRUPTION":
            sf_fuel = float(sf_fac_fuel["CYCLONE_PORT"])
            sf_food = float(sf_fac_food["CYCLONE_PORT"])
        elif scen == "COMBINED":
            sf_fuel = min(5.0, float(sf_fac_fuel["FLOOD_MAJOR"]) * float(sf_fac_fuel["CYCLONE_PORT"]))
            str_val = cr_s if cr_s is not None else 0.0
            sf_food = min(5.0, float(sf_fac_food["FLOOD_MAJOR"]) * float(sf_fac_food["CYCLONE_PORT"]) * (1.0 + str_val))
        elif scen == "CROP_LOSS":
            sf_fuel = 1.05
            sf_food = float(sf_fac_food["CROP_FAILURE"])
        elif scen == "ARSENIC_CRISIS":
            sf_fuel = 1.02
            sf_food = 1.10
            
        # Forex 
        gdp = 4.6e11
        f_surge = 0.0
        p_surge = 0.0
        
        l_pr = self.crop.get("loss_probability", 0.0)
        t_rsk = self.crop.get("tonnage_at_risk_metric_tons", 0.0)
        if l_pr is not None and t_rsk is not None:
            f_surge = float(l_pr) * float(t_rsk) * 450.0 / gdp
            
        p_surge = fuel_dis * 23000.0 * 365.0 * 75.0 / gdp
        fx_score = min(1.0, (f_surge + p_surge) / 3.0) # Using 3x normalisation
        
        # Inflation
        pps_pet = self.pps.get("PETROLEUM", {}).get("PPS_value", 0.0)
        pps_fod = self.pps.get("FOOD_STAPLES", {}).get("PPS_value", 0.0)
        
        if pps_pet is None: pps_pet = 0.0
        if pps_fod is None: pps_fod = 0.0
        
        inf = (pps_pet * 0.08 + pps_fod * 0.56) * 100.0
        if inf > 100.0: inf = 100.0
        
        # Compression
        d_comp = False
        if fuel_dis > 0.20 and cr_s is not None and cr_s >= 0.20:
            d_comp = True
            
        # Severity
        sev_sc = max(fuel_dis, fx_score, min(1.0, inf / 20.0))
        sev = "LOW"
        if sev_sc >= 0.75: sev = "SEVERE"
        elif sev_sc >= 0.50: sev = "HIGH"
        elif sev_sc >= 0.20: sev = "MODERATE"
        
        dist = []
        if self.flood.get("affected_districts"):
            dist.extend(self.flood["affected_districts"])
        if scen == "CYCLONE_PORT_DISRUPTION" and self.storm.get("districts_at_risk"):
            dist.extend(self.storm["districts_at_risk"])
        dist = list(set(dist))
        
        dur = None
        d1 = self.pps.get("PETROLEUM", {}).get("duration_estimate_days")
        d2 = self.pps.get("FOOD_STAPLES", {}).get("duration_estimate_days")
        if d1 is not None and d2 is not None: dur = min(d1, d2)
        elif d1 is not None: dur = d1
        elif d2 is not None: dur = d2
        
        out = {
            "scenario_type": scen,
            "fuel_distribution_disruption_pct": float(fuel_dis),
            "road_damage_index": float(r_dmg),
            "demand_surge_fuel_multiplier": float(sf_fuel),
            "demand_surge_food_multiplier": float(sf_food),
            "forex_pressure_score": float(fx_score),
            "inflation_acceleration_pct": float(inf),
            "pps_petroleum": float(pps_pet) if pps_pet > 0 else None,
            "pps_food": float(pps_fod) if pps_fod > 0 else None,
            "petroleum_price_range_lower_pct": self.pps.get("PETROLEUM", {}).get("price_pressure_lower_bound_pct"),
            "petroleum_price_range_upper_pct": self.pps.get("PETROLEUM", {}).get("price_pressure_upper_bound_pct"),
            "food_price_range_lower_pct": self.pps.get("FOOD_STAPLES", {}).get("price_pressure_lower_bound_pct"),
            "food_price_range_upper_pct": self.pps.get("FOOD_STAPLES", {}).get("price_pressure_upper_bound_pct"),
            "affected_districts": dist,
            "severity": sev,
            "duration_estimate_days": dur,
            "double_compression_flag": d_comp,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("econ.domestic_output", out)

if __name__ == "__main__":
    mod = EconDomestic()
    mod.start()
