"""
M-38 ECON-RESERVE
Reserve Coverage Calculator
"""

from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

class EconReserve(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-38",
            input_topics=["econ.demand_profile", "flood_predictions", "storm_forecast", "raw.fuel_inventory"],
            output_topics=["econ.reserve_status"]
        )
        self.dem = {}
        self.flood = {}
        self.storm = {}
        self.fuel = {}
        
        self.lim = {
            "PETROLEUM":    [30, 15, 7, 3],
            "FOOD_STAPLES": [60, 30, 15, 7],
            "MEDICINE":     [90, 45, 21, 7]
        }

    def process(self, topic, message):
        if topic == "flood_predictions":
            # Just take the national max or assume incoming msg is latest national map
            # M-08 posts per district so we just assume highest threat prob
            # Actually, we'll store max flood prob
            fp = message.get("flood_probability", 0.0)
            if "max" not in self.flood: self.flood["max"] = 0.0
            self.flood["max"] = max(self.flood["max"], fp)
            return

        if topic == "storm_forecast":
            self.storm = message
            return
            
        if topic == "raw.fuel_inventory":
            self.fuel = message
            return
            
        if topic == "econ.demand_profile":
            self.dem[message.get("commodity")] = message
            self._process_reserve(message.get("commodity"))

    def _process_reserve(self, comm):
        if comm not in self.dem: return
        dmsg = self.dem[comm]
        
        q_res = None
        if comm == "PETROLEUM":
            bbl = self.fuel.get("bpc_strategic_reserve_bbl")
            if bbl is not None: q_res = float(bbl)
            else: self.logger.warning("RESERVE_DATA_MISSING")
            
        # baseline docs 
        u_rate = dmsg.get("baseline_daily_demand", 1.0)
        
        bl_cov_d = None
        if q_res is not None and u_rate > 0:
            bl_cov_d = q_res / u_rate
            
        # disruptions
        f_p = self.flood.get("max", 0.0)
        idep = dmsg.get("import_dependence_pct", 0.0)
        
        f_dis = f_p * idep
        
        cap = self.storm.get("import_capacity_pct", 100)
        c_dis = (1.0 - (float(cap) / 100.0)) * idep
        
        d_p = 0.0
        d_src = "NONE"
        if f_dis > 0 and c_dis > 0:
            d_p = 1.0 - (1.0 - f_dis) * (1.0 - c_dis)
            d_src = "BOTH"
        elif f_dis > c_dis:
            d_p = f_dis
            d_src = "FLOOD"
        elif c_dis > 0:
            d_p = c_dis
            d_src = "CYCLONE"
            
        # surge
        s_obj = dmsg.get("demand_surge_factors", {})
        s_fac = 1.0
        
        if d_src == "FLOOD": s_fac = float(s_obj.get("FLOOD_MAJOR", 1.0))
        elif d_src == "CYCLONE": s_fac = float(s_obj.get("CYCLONE_PORT", 1.0))
        elif d_src == "BOTH":
            s_fac = float(s_obj.get("FLOOD_MAJOR", 1.0)) * float(s_obj.get("CYCLONE_PORT", 1.0))
            s_fac = min(5.0, s_fac)
            
        adj_cov = None
        st = "UNKNOWN"
        if bl_cov_d is not None:
            adj_cov = bl_cov_d * (1.0 - d_p) / max(1.0, s_fac)
            if adj_cov < 0: adj_cov = 0.0
            
            lsc = self.lim.get(comm)
            if lsc:
                if adj_cov > lsc[0]: st = "SECURE"
                elif adj_cov > lsc[1]: st = "WATCH"
                elif adj_cov > lsc[2]: st = "STRESSED"
                elif adj_cov > lsc[3]: st = "CRITICAL"
                else: st = "CRISIS"
        
        out = {
            "commodity": comm,
            "strategic_reserve_quantity": float(q_res) if q_res is not None else None,
            "reserve_unit": dmsg.get("demand_unit", ""),
            "daily_consumption_rate": float(u_rate),
            "days_coverage_baseline": float(bl_cov_d) if bl_cov_d is not None else None,
            "disruption_probability": float(d_p),
            "demand_surge_factor": float(s_fac),
            "adjusted_days_coverage": float(adj_cov) if adj_cov is not None else None,
            "status": st,
            "disruption_source": d_src,
            "chittagong_port_status": self.storm.get("chittagong_port_operability", "NORMAL"),
            "import_capacity_pct": int(cap),
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("econ.reserve_status", out)

if __name__ == "__main__":
    mod = EconReserve()
    mod.start()
