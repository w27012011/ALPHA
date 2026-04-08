"""
M-37 ECON-DEMAND
Demand Profile Updater
"""

from datetime import datetime, timezone, timedelta
from modules.base_module import AlphaBaseModule

class EconDemand(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-37",
            input_topics=["raw.fuel_inventory", "raw.economic_indicators"],
            output_topics=["econ.demand_profile"]
        )
        # Elasticities
        self.el = {
            "PETROLEUM": 0.08,
            "FOOD_STAPLES": 0.15,
            "MEDICINE": 0.05,
            "CONSTRUCTION": 0.60
        }
        
        # Baselines
        self.bl = {
            "PETROLEUM": 23000.0,
            "FOOD_STAPLES": 55000.0,
            "MEDICINE": 1.0,
            "CONSTRUCTION": 1.0
        }
        
        self.u = {
            "PETROLEUM": "barrels/day",
            "FOOD_STAPLES": "metric_tons/day",
            "MEDICINE": "index",
            "CONSTRUCTION": "index"
        }
        
        self.dep = {
            "PETROLEUM": 1.0,
            "FOOD_STAPLES": 0.15,
            "MEDICINE": 0.70,
            "CONSTRUCTION": 0.40
        }
        
        self.surge = {
            "PETROLEUM":    {"FLOOD_MAJOR": 2.00, "CYCLONE_PORT": 1.35, "CROP_FAILURE": 1.05, "ARSENIC_CRISIS": 1.02},
            "FOOD_STAPLES": {"FLOOD_MAJOR": 1.23, "CYCLONE_PORT": 1.15, "CROP_FAILURE": 1.40, "ARSENIC_CRISIS": 1.10},
            "MEDICINE":     {"FLOOD_MAJOR": 1.0, "CYCLONE_PORT": 1.0, "CROP_FAILURE": 1.0,  "ARSENIC_CRISIS": 1.0},
            "CONSTRUCTION": {"FLOOD_MAJOR": 1.0, "CYCLONE_PORT": 1.0, "CROP_FAILURE": 1.0,  "ARSENIC_CRISIS": 1.0}
        }
        
        self.f_data = {}
        self.e_data = {}
    
    def process(self, topic, message):
        if topic == "raw.fuel_inventory":
            self.f_data = message
        elif topic == "raw.economic_indicators":
            self.e_data = message
            
        self._process_demand()
            
    def _process_demand(self):
        ts_now = datetime.now(timezone.utc)
        
        # Parse inputs
        fbbl = self.f_data.get("bpc_daily_imports_bbl")
        bbs = self.e_data.get("bbs_food_consumption_index")
        
        dv_f = self.f_data.get("data_vintage_date")
        dv_e = self.e_data.get("data_vintage_date")
        
        def is_stale(vindate):
            if not vindate: return True
            if vindate == "DEFAULT": return True
            try:
                import dateutil.parser
                dt = dateutil.parser.isoparse(vindate)
                if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                if (ts_now - dt).days >= 30: return True
                return False
            except:
                return True
                
        # Petroleum Process
        p_st = is_stale(dv_f)
        if not p_st and fbbl is not None and float(fbbl) > 0:
            self.bl["PETROLEUM"] = float(fbbl)
            p_src = "BPC"
        else:
            p_src = "DEFAULT"
            self.bl["PETROLEUM"] = 23000.0
            
        # Food Process
        f_st = is_stale(dv_e)
        if not f_st and bbs is not None:
            self.bl["FOOD_STAPLES"] = float(bbs) * 55000.0
            f_src = "BBS"
        else:
            f_src = "DEFAULT"
            self.bl["FOOD_STAPLES"] = 55000.0
            
        wb_dep = self.e_data.get("world_bank_import_dep_pct")
        if wb_dep is not None:
            self.dep["PETROLEUM"] = float(wb_dep)
            
        for comm in ["PETROLEUM", "FOOD_STAPLES", "MEDICINE", "CONSTRUCTION"]:
            st = p_st if comm == "PETROLEUM" else f_st if comm == "FOOD_STAPLES" else True
            src = p_src if comm == "PETROLEUM" else f_src if comm == "FOOD_STAPLES" else "DEFAULT"
            
            vt = dv_f if comm == "PETROLEUM" else dv_e if comm == "FOOD_STAPLES" else "DEFAULT"
            if not vt: vt = "DEFAULT"
            
            out = {
                "commodity": comm,
                "baseline_daily_demand": float(self.bl[comm]),
                "demand_unit": self.u[comm],
                "demand_elasticity": float(self.el[comm]),
                "import_dependence_pct": float(self.dep[comm]),
                "demand_surge_factors": self.surge[comm],
                "data_vintage_date": vt,
                "data_source": src,
                "stale_flag": st,
                "timestamp": ts_now.isoformat().replace("+00:00", "Z")
            }
            self.publish("econ.demand_profile", out)

if __name__ == "__main__":
    mod = EconDemand()
    mod.start()
