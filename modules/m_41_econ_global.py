"""
M-41 ECON-GLOBAL
ECON-GLOBAL Engine
"""

from uuid import uuid4
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class EconGlobal(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-41",
            input_topics=["raw.fuel_inventory", "raw.economic_indicators", "econ.demand_profile"],
            output_topics=["econ.global_output"]
        )
        self.fuel = {}
        self.econ = {}
        self.dem = {}

    def process(self, topic, message):
        if topic == "econ.demand_profile":
            self.dem[message.get("commodity")] = message
            return
            
        if topic == "raw.fuel_inventory":
            self.fuel = message
            self._process_global()
        elif topic == "raw.economic_indicators":
            self.econ = message
            self._process_global()

    def _process_global(self):
        ts_now = datetime.now(timezone.utc)
        
        d_ev = False
        m_src = None
        if self.fuel.get("disruption_event_active"):
            d_ev = True
            m_src = self.fuel
        elif self.econ.get("disruption_event_active"):
            d_ev = True
            m_src = self.econ
            
        if not d_ev:
            out = {
                "disruption_event_id": None,
                "disruption_type": "NONE",
                "affected_nation": None,
                "disrupted_commodity": None,
                "disrupted_volume_mbpd": None,
                "global_supply_buffer_score": None,
                "substitutability_index": 0.0,
                "demand_inelasticity_factor": 1.0,
                "geopolitical_risk_multiplier": 1.0,
                "Global_PPS": 0.0,
                "price_range_lower_usd": None,
                "price_range_upper_usd": None,
                "confidence_interval": None,
                "timestamp": ts_now.isoformat().replace("+00:00", "Z")
            }
            self.publish("econ.global_output", out)
            return

        c_type = m_src.get("disrupted_commodity", "NONE")
        d_type = m_src.get("disruption_type", "NONE")
        nat = m_src.get("affected_nation")
        
        # Buffer
        buf = 0.5
        if c_type in ["PETROLEUM", "NATURAL_GAS"]:
            str_r = self.fuel.get("strategic_reserve_iea_bbl", 0.0)
            if str_r is None: str_r = 0.0
            s_cap = (float(str_r) / 365.0) / 1000000.0 + 2.0
            buf = min(1.0, s_cap / 5.0)
        buf = max(0.05, buf)
            
        # SI Map
        si = 0.1
        if c_type == "FOOD_STAPLES": si = 0.4
        elif c_type == "MEDICINE": si = 0.5
        elif c_type == "CONSTRUCTION": si = 0.7
        
        # Disrupted Vol
        v_fr = 0.0
        v_val = None
        if c_type == "PETROLEUM":
            v_val = m_src.get("disrupted_volume_mbpd")
            if v_val is not None: v_fr = float(v_val) / 100.0
        elif c_type == "FOOD_STAPLES":
            v_val = m_src.get("disrupted_volume_tons")
            if v_val is not None: v_fr = float(v_val) / 2500000.0
            
        # Elas
        elas = 0.1
        if c_type == "PETROLEUM": elas = self.dem.get("PETROLEUM", {}).get("demand_elasticity", 0.08)
        elif c_type == "FOOD_STAPLES": elas = self.dem.get("FOOD_STAPLES", {}).get("demand_elasticity", 0.15)
        elif c_type == "MEDICINE": elas = self.dem.get("MEDICINE", {}).get("demand_elasticity", 0.05)
        elif c_type == "CONSTRUCTION": elas = self.dem.get("CONSTRUCTION", {}).get("demand_elasticity", 0.60)
        
        # GRM
        grm = 1.0
        if d_type == "GEOPOLITICAL": grm = 3.0
        elif d_type == "SANCTIONS": grm = 2.0
            
        g_pps = (v_fr / buf) * (1.0 - si) * (1.0 / elas) * grm
        
        p_low = None
        p_up = None
        ci = None
        
        if c_type in ["PETROLEUM", "NATURAL_GAS"]:
            c_p = self.fuel.get("brent_spot_price_usd")
            if c_p is None: c_p = 75.0
            else: c_p = float(c_p)
            
            e_inc = c_p * (g_pps / 10.0)
            p_low = c_p + (e_inc * 0.75)
            p_up = c_p + (e_inc * 1.25)
            ci = [float(p_low), float(p_up)]

        out = {
            "disruption_event_id": str(uuid4()),
            "disruption_type": d_type,
            "affected_nation": nat,
            "disrupted_commodity": c_type,
            "disrupted_volume_mbpd": float(v_val) if c_type == "PETROLEUM" and v_val is not None else None,
            "global_supply_buffer_score": float(buf),
            "substitutability_index": float(si),
            "demand_inelasticity_factor": 1.0 / float(elas),
            "geopolitical_risk_multiplier": float(grm),
            "Global_PPS": float(g_pps),
            "price_range_lower_usd": float(p_low) if p_low is not None else None,
            "price_range_upper_usd": float(p_up) if p_up is not None else None,
            "confidence_interval": ci,
            "timestamp": ts_now.isoformat().replace("+00:00", "Z")
        }
        self.publish("econ.global_output", out)

if __name__ == "__main__":
    mod = EconGlobal()
    mod.start()
