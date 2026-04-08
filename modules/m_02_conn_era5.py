"""
M-02 CONN-ERA5
ERA5-Land Weather Field Connector
Tier-0 Connector Module — Regime 3.0 Sterilized
"""

import os
import json
import time
import math
from datetime import datetime, timezone
import logging

from modules.base_module import AlphaBaseModule

class ConnERA5(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-02", 
            input_topics=[], 
            output_topics=["raw.weather_fields"],
            poll_interval=1.0
        )
        self.fetch_interval_seconds = 21600 # 6 hours
        self.last_fetch_time = 0

    def _poll_inputs(self):
        """Regime 3.0 Feed Consumer."""
        now = time.monotonic()
        if now - self.last_fetch_time >= 600: # Check every 10 mins
            self.last_fetch_time = now
            self._consume_feed()

    def _consume_feed(self):
        feed_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "active_feed", f"{self.module_id}_payload.json")
        try:
            if not os.path.exists(feed_path):
                self.logger.warning(f"Feed file {feed_path} not found.")
                return

            with open(feed_path, "r") as f:
                feed = json.load(f)
            
            payload = feed.get("payload", {})
            # Alignment: The daemon now provides a 'mapped_grid' key
            cells = payload.get("mapped_grid", [])
            
            if not cells and isinstance(payload, list):
                cells = payload

            if not cells:
                self.logger.warning("No weather cells found in feed.")
                return

            for cell in cells:
                self._publish_cell(cell)
            
            self.current_status = "HEALTHY"
            self.logger.info(f"Published {len(cells)} weather grid cells from live feed.")

        except Exception as e:
            self.logger.error(f"ERA5 Consumption error: {e}")
            self.current_status = "DEGRADED"

    def _parse_cds_dict(self, raw):
        """Fall-through parser for raw CDS JSON dict if daemon didn't flatten it."""
        # Simple placeholder: in a real system we'd use xarray logic here
        # but the daemon is supposed to provide a 'mapped_list'.
        return []

    def _publish_cell(self, cell):
        """Follows MDD Section 5.3 strictly."""
        lat = cell.get("lat")
        lon = cell.get("lon")
        if lat is None or lon is None: return

        # MDD 5.4.1: Compute Relative Humidity if missing
        t = cell.get("temperature_2m_K")
        td = cell.get("dewpoint_2m_K")
        rh = cell.get("relative_humidity_pct")
        if rh is None and t is not None and td is not None:
            # T_c = T_k - 273.15
            tc = t - 273.15
            tdc = td - 273.15
            try:
                rh = 100 * math.exp((17.625 * tdc) / (243.04 + tdc)) / math.exp((17.625 * tc) / (243.04 + tc))
                rh = max(0.0, min(100.0, rh))
            except:
                rh = None

        msg = {
            "grid_id": cell.get("grid_id", f"{lat:.1f}_{lon:.1f}"),
            "lat": float(lat),
            "lon": float(lon),
            "temperature_2m_K": t,
            "dewpoint_2m_K": td,
            "total_precipitation_m": cell.get("total_precipitation_m"),
            "surface_pressure_hPa": cell.get("surface_pressure_hPa"),
            "u_wind_10m_ms": cell.get("u_wind_10m_ms"),
            "v_wind_10m_ms": cell.get("v_wind_10m_ms"),
            "soil_moisture_level1_m3m3": cell.get("soil_moisture_level1_m3m3"),
            "soil_moisture_level2_m3m3": cell.get("soil_moisture_level2_m3m3"),
            "relative_humidity_pct": rh,
            "data_source": "ERA5_API",
            "valid_time": cell.get("valid_time"),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        self.publish("raw.weather_fields", msg)

if __name__ == "__main__":
    mod = ConnERA5()
    mod.start()
