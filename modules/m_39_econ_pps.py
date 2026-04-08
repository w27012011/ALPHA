"""
M-39 ECON-PPS
Price Pressure Signal Calculator
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class EconPps(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-39",
            input_topics=["econ.reserve_status", "econ.demand_profile", "flood_predictions", "storm_forecast", "crop_stress"],
            output_topics=["econ.pps_signal"]
        )
        self.dem = {}
        self.flood = False
        self.storm = False
        self.crop = {}

    def process(self, topic, message):
        if topic == "flood_predictions":
            self.flood = True
            return
        if topic == "storm_forecast":
            self.storm = True
            return
        if topic == "crop_stress":
            if message.get("stress_index") is not None:
                self.crop["max_stress"] = max(self.crop.get("max_stress", 0.0), message.get("stress_index"))
            return
            
        if topic == "econ.demand_profile":
            self.dem[message.get("commodity")] = message
            return
            
        if topic == "econ.reserve_status":
            self._process_pps(message)

    def _process_pps(self, res_msg):
        comm = res_msg.get("commodity")
        if not comm: return
        
        d_msg = self.dem.get(comm, {})
        m_in = []
        is_p = False
        
        if not d_msg:
            m_in.append("DEMAND_PROFILE")
            is_p = True
            # Defaults
            elas = 0.08 if comm == "PETROLEUM" else 0.15
            idep = 1.0 if comm == "PETROLEUM" else 0.15
        else:
            elas = d_msg.get("demand_elasticity", 0.1)
            idep = d_msg.get("import_dependence_pct", 1.0)
            
        if not self.flood:
            m_in.append("FLOOD_PREDICTIONS")
            is_p = True

        d_prob = res_msg.get("disruption_probability", 0.0)
        s_fac = res_msg.get("demand_surge_factor", 1.0)
        
        d_risk = d_prob * idep * s_fac
        
        if comm == "FOOD_STAPLES":
            c_stress = self.crop.get("max_stress")
            if c_stress is not None:
                prod_loss = c_stress * (1.0 - idep)
                d_risk = min(1.0, d_risk + prod_loss)
                
        # S_available & Reserve
        s_avail = max(0.0, 1.0 - d_risk)
        
        dc_adj = res_msg.get("adjusted_days_coverage")
        dc_base = res_msg.get("days_coverage_baseline")
        
        rbr = None
        if dc_adj is not None and dc_base is not None and dc_base > 0:
            rbr = dc_adj / dc_base
        elif s_fac > 0:
            rbr = (1.0 - d_prob) / s_fac
            
        pps = None
        up_b = 0.0
        lw_b = 0.0
        p_dir = "STABLE"
        drv = "NONE"
        dur = dc_adj
        
        if rbr is None:
            is_p = True
            m_in.append("RESERVE_DATA")
        else:
            if s_avail == 0 and rbr == 0:
                pps = None
                p_dir = "UP"
                up_b = 999.9
                self.logger.warning("PPS_UNDEFINED_TOTAL_DISRUPTION")
            else:
                pps = (d_risk * (1.0 / elas)) / (s_avail * rbr)
                if pps > 100.0:
                    pps = 100.0
                    self.logger.warning("PPS_EXTREME_CLAMPED")
                    
                lw_b = pps * 0.70 * 100.0
                up_b = pps * 1.30 * 100.0
                
                if pps > 0.10: p_dir = "UP"
                elif pps < -0.10: p_dir = "DOWN"
                
                if d_risk > 0.3 and rbr < 0.5: drv = "BOTH"
                elif d_risk > 0.3: drv = "DEMAND_SURGE"
                elif rbr < 0.5: drv = "RESERVE_DEPLETION"
                elif d_prob > 0.3: drv = "SUPPLY_DISRUPTION"

        out = {
            "commodity": comm,
            "PPS_value": float(pps) if pps is not None else None,
            "price_pressure_direction": p_dir,
            "price_pressure_lower_bound_pct": float(lw_b),
            "price_pressure_upper_bound_pct": float(up_b),
            "duration_estimate_days": int(dur) if dur is not None else None,
            "primary_driver": drv,
            "D_at_risk": float(d_risk),
            "S_available": float(s_avail),
            "RBR_reserve": float(rbr) if rbr is not None else None,
            "partial_pps_flag": is_p,
            "missing_inputs": m_in,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("econ.pps_signal", out)

if __name__ == "__main__":
    mod = EconPps()
    mod.start()
