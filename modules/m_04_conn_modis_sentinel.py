"""
M-04 CONN-MODIS-SENTINEL
MODIS NDVI + Sentinel-1 SAR Displacement Connector
Tier-0 Connector Module
"""

import os
import json
import time
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")

class ConnModisSentinel(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-04", 
            input_topics=[], 
            output_topics=["raw.ndvi_grid", "raw.sar_displacement"],
            poll_interval=1.0
        )
        self.ndvi_interval_seconds = 8 * 24 * 3600
        self.sar_interval_seconds  = 6 * 24 * 3600
        
        self.last_ndvi_time = 0
        self.last_sar_time = 0
        
        self.cache_ndvi = os.path.join(CACHE_DIR, "M-04_ndvi_last.json")
        self.cache_sar = os.path.join(CACHE_DIR, "M-04_sar_last.json")
        os.makedirs(CACHE_DIR, exist_ok=True)
        # SYS-02 handles offline fallback natively now.

    def _ensure_dummy_cache(self):
        if not os.path.exists(self.cache_ndvi):
            dummy_ndvi = {
                "product": "MOD13Q1",
                "composite_start_date": "2026-04-01T00:00:00Z",
                "composite_end_date": "2026-04-08T00:00:00Z",
                "grid_cells": [
                    {"grid_id": "23.5_90.0", "lat": 23.5, "lon": 90.0, "district": "Dhaka", "ndvi": 0.65, "pixel_reliability": 0, "evi": 0.50}
                ],
                "total_cells": 1,
                "cloud_cover_pct": 0.0,
                "data_source": "CACHE",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            with open(self.cache_ndvi, "w") as f:
                json.dump(dummy_ndvi, f)
                
        if not os.path.exists(self.cache_sar):
            dummy_sar = {
                "product": "S1A",
                "acquisition_date": "2026-04-05T00:00:00Z",
                "reference_date": "2026-03-30T00:00:00Z",
                "segments": [
                    {"segment_id": "SEG-001", "lat_start": 24.0, "lon_start": 89.5, "lat_end": 24.1, "lon_end": 89.6, "district": "Pabna", "displacement_mm": -15.5, "coherence": 0.85, "look_angle_deg": 35.0}
                ],
                "total_segments": 1,
                "data_source": "CACHE",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            with open(self.cache_sar, "w") as f:
                json.dump(dummy_sar, f)

    def _poll_inputs(self):
        now = time.monotonic()
        
        # NDVI Pass
        if now - self.last_ndvi_time >= self.ndvi_interval_seconds or self.last_ndvi_time == 0:
            self.last_ndvi_time = now
            self._execute_ndvi_fetch()
            
        # SAR Pass
        if now - self.last_sar_time >= self.sar_interval_seconds or self.last_sar_time == 0:
            self.last_sar_time = now
            self._execute_sar_fetch()

    def _execute_ndvi_fetch(self):
        self.logger.info("M-04: Checking for live NDVI feed from CDSE (Normalized).")
        feed_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "active_feed", "M-04-NDVI_payload.json")
        try:
            if not os.path.exists(feed_path):
                self.logger.warning("NDVI Feed not found. Falling back to cache.")
                with open(self.cache_ndvi, "r") as f:
                    cached = json.load(f)
                cached["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
                cached["_stale"] = {"reason": "FEED_MISSING"}
                self.publish("raw.ndvi_grid", cached)
                return

            with open(feed_path, "r") as f:
                feed = json.load(f)
            
            raw_grid = feed.get("payload", {}).get("ndvi_grid_raw", [])
            
            grid_cells = []
            for i, raw_val in enumerate(raw_grid[:100]):
                # ALGORITHM RESTORATION (MDD §5.5.1): ndvi = raw_ndvi_int / 10000.0
                ndvi_scaled = None
                if raw_val is not None:
                    ndvi_scaled = float(raw_val) / 10000.0
                    # Quality check: valid -1.0 to 1.0
                    ndvi_scaled = max(-1.0, min(1.0, ndvi_scaled))

                grid_cells.append({
                    "grid_id": f"pixel_{i}",
                    "lat": 23.5, "lon": 90.0,
                    "district": "Dhaka",
                    "ndvi": ndvi_scaled,
                    "pixel_reliability": 0, # Assuming good pixels from Process API
                    "evi": None
                })

            out = {
                "product": "SENTINEL-2_NDVI", 
                "composite_start_date": datetime.now(timezone.utc).isoformat() + "Z",
                "composite_end_date": datetime.now(timezone.utc).isoformat() + "Z",
                "grid_cells": grid_cells,
                "total_cells": len(grid_cells),
                "cloud_cover_pct": 0.0,
                "data_source": "CDSE_API",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            self.publish("raw.ndvi_grid", out)
            self.current_status = "HEALTHY"
            
        except Exception as e:
            self.logger.error(f"M-04: NDVI Feed error: {e}")

    def _execute_sar_fetch(self):
        self.logger.info("M-04: Checking for live SAR Displacement/Flood feed from CDSE.")
        feed_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "active_feed", "M-04-SAR_payload.json")
        try:
            if not os.path.exists(feed_path):
                self.logger.warning("SAR Feed not found. Falling back to cache.")
                with open(self.cache_sar, "r") as f:
                    cached = json.load(f)
                cached["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
                cached["_stale"] = {"reason": "FEED_MISSING"}
                self.publish("raw.sar_displacement", cached)
                return

            with open(feed_path, "r") as f:
                feed = json.load(f)
            
            payload = feed.get("payload", {})
            raw_flood = payload.get("flood_grid", [])
            coh_val = payload.get("coherence", 1.0)
            disp_mm = payload.get("displacement_mm", 0.0)
            
            # ALGORITHM RESTORATION (MDD §5.5.4): Discard segment if coherence < 0.30
            if coh_val < 0.30:
                self.logger.warning(f"SAR Data discarded due to low coherence: {coh_val}")
                return

            segments = []
            for i, is_water in enumerate(raw_flood[:50]):
                if is_water == 1:
                    segments.append({
                        "segment_id": f"SAR_FLOOD_{i}",
                        "lat_start": 24.0, "lon_start": 89.5,
                        "lat_end": 24.1, "lon_end": 89.6,
                        "district": "Unknown",
                        "displacement_mm": disp_mm,
                        "coherence": coh_val, 
                        "look_angle_deg": 35.0,
                        "_meta": {"is_flood": True}
                    })

            out = {
                "product": "S1A_GRD_AUTO",
                "acquisition_date": datetime.now(timezone.utc).isoformat() + "Z",
                "reference_date": None,
                "segments": segments,
                "total_segments": len(segments),
                "data_source": "CDSE_API",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }
            self.publish("raw.sar_displacement", out)
            self.current_status = "HEALTHY"

        except Exception as e:
            self.logger.error(f"M-04: SAR Feed error: {e}")

if __name__ == "__main__":
    mod = ConnModisSentinel()
    mod.start()
