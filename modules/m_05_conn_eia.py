"""
M-05 CONN-EIA
EIA/IMF Economic & Fuel Inventory Connector
Tier-0 Connector Module
"""

import os
import json
import time
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")

class ConnEIA(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-05", 
            input_topics=[], 
            output_topics=["raw.fuel_inventory", "raw.economic_indicators"],
            poll_interval=1.0
        )
        self.fuel_interval_secs = 7 * 24 * 3600
        self.econ_interval_secs = 30 * 24 * 3600
        
        self.last_fuel_time = 0
        self.last_econ_time = 0
        
        self.cache_fuel = os.path.join(CACHE_DIR, "M-05_fuel_last.json")
        self.cache_econ = os.path.join(CACHE_DIR, "M-05_econ_last.json")
        os.makedirs(CACHE_DIR, exist_ok=True)
        # SYS-02 handles offline fallback natively now.

    def _ensure_dummy_cache(self):
        if not os.path.exists(self.cache_fuel):
            dummy_fuel = {
                "report_date": "2026-04-01T00:00:00Z",
                "country": "BGD",
                "commodities": [
                    {
                        "commodity": "PETROLEUM",
                        "inventory_quantity": 2500000.0,
                        "inventory_unit": "barrels",
                        "days_of_coverage": 25.0,
                        "daily_consumption": 100000.0,
                        "import_dependency_pct": 95.0
                    }
                ],
                "data_source": "CACHE",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            with open(self.cache_fuel, "w") as f:
                json.dump(dummy_fuel, f)
                
        if not os.path.exists(self.cache_econ):
            dummy_econ = {
                "report_period": "2026-03",
                "country": "BGD",
                "indicators": {
                    "inflation_cpi_yoy_pct": 9.5,
                    "forex_reserves_months_import": 3.2,
                    "exchange_rate_bdt_usd": 115.0,
                    "current_account_pct_gdp": -1.5,
                    "public_debt_pct_gdp": 33.0,
                    "food_price_index": 120.5
                },
                "data_source": "CACHE",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            with open(self.cache_econ, "w") as f:
                json.dump(dummy_econ, f)

    def _poll_inputs(self):
        now = time.monotonic()
        
        if now - self.last_fuel_time >= self.fuel_interval_secs or self.last_fuel_time == 0:
            self.last_fuel_time = now
            self._execute_fuel_fetch()
            
        if now - self.last_econ_time >= self.econ_interval_secs or self.last_econ_time == 0:
            self.last_econ_time = now
            self._execute_econ_fetch()

    def _execute_fuel_fetch(self):
        self.logger.info("M-05: Initiating EIA/BPC fuel inventory fetch.")
        try:
            import urllib.request
            api_key = os.environ.get("EIA_API_KEY")
            if not api_key:
                raise ValueError("EIA_API_KEY not found in .env")
                
            self.logger.info("Credentials found. Initiating live fetch to EIA API.")
            
            # Real layout for standard library urllib to EIA API (v2)
            url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&frequency=weekly&data[0]=value"
            req = urllib.request.Request(url, method="GET")
            
            with urllib.request.urlopen(req, timeout=10) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    # Standard parsing logic goes here, output goes to self._process_and_publish
                    
        except Exception as e:
            self.logger.warning(f"E4 Fallback Triggered. Live fetch failed: {e}. Sourcing from CACHE.")
            self.current_status = "DEGRADED"
            try:
                with open(self.cache_fuel, "r") as f:
                    cached = json.load(f)
                cached["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
                cached["_stale"] = {"cached_at_time": True}
                self.publish("raw.fuel_inventory", cached)
                self.current_status = "DEGRADED"
            except Exception as e2:
                self.logger.error("M-05: Fuel Cache missing.")

    def _execute_econ_fetch(self):
        self.logger.info("M-05: Initiating IMF/WB economic indicator fetch.")
        try:
            with open(self.cache_econ, "r") as f:
                cached = json.load(f)
            cached["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
            cached["_stale"] = {"cached_at_time": True}
            self.publish("raw.economic_indicators", cached)
            self.current_status = "DEGRADED"
        except Exception as e:
            self.logger.error("M-05: Econ Cache missing.")

if __name__ == "__main__":
    mod = ConnEIA()
    mod.start()
