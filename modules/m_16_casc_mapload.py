"""
M-16 CASC-MAPLOAD
Cascade Transmission Map Loader
"""

import os
import json
import sys
import time
from datetime import datetime, timezone
import threading

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    HAS_WATCHDOG = True
except ImportError:
    HAS_WATCHDOG = False

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
MAP_FILE = os.path.join(DATA_DIR, "cascade_transmission_map.json")

class MapChangeHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback
    def on_modified(self, event):
        if event.src_path == MAP_FILE:
            self.callback()

class CascMapload(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-16",
            input_topics=[],
            output_topics=["casc.transmission_map"],
            poll_interval=5.0
        )
        self.last_mtime = 0

    def _start_custom(self):
        """Called automatically after base startup."""
        self._load_and_publish()
        
        if HAS_WATCHDOG:
            self.logger.info("Watchdog library active. Starting OS file event listener.")
            self.observer = Observer()
            handler = MapChangeHandler(self._load_and_publish)
            self.observer.schedule(handler, DATA_DIR, recursive=False)
            self.observer.start()
        else:
            self.logger.info("Watchdog missing. Falling back to OS poll loop.")

    def _poll_inputs(self):
        if not HAS_WATCHDOG:
            # Polling fallback
            if os.path.exists(MAP_FILE):
                mtime = os.path.getmtime(MAP_FILE)
                if mtime > self.last_mtime + 2.0:
                    self._load_and_publish()

    def _load_and_publish(self):
        if not os.path.exists(MAP_FILE):
            self.logger.critical("MAP_FILE_MISSING")
            sys.exit(1)
            
        try:
            with open(MAP_FILE, "r") as f:
                data = json.load(f)
        except Exception:
            self.logger.critical("MAP_FILE_MALFORMED")
            sys.exit(1)

        pairs = data.get("pairs", [])
        valid_pairs = []
        domains = {"HYDRO", "AQUA", "GEO", "AGRI", "ATMO", "ECON"}
        
        for p in pairs:
            if p.get("source_hazard") in domains and p.get("target_hazard") in domains:
                prob = float(p.get("base_transmission_probability", -1))
                if 0.0 <= prob <= 1.0 and p.get("lag_mean_days", 0) > 0 and p.get("lag_std_days", -1) >= 0:
                    valid_pairs.append(p)
                    
        if len(valid_pairs) < 10:
            self.logger.critical("INSUFFICIENT_PAIRS")
            sys.exit(1)
            
        load_status = "FULL" if len(valid_pairs) == 14 else "PARTIAL"
        if load_status == "PARTIAL":
            self.logger.warning("PARTIAL_MAP_LOADED")
            
        out = {
            "version": data.get("version", "1.0"),
            "source": data.get("source", "USS"),
            "pairs_count": len(valid_pairs),
            "pairs": valid_pairs,
            "load_status": load_status,
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
        }
        
        self.last_mtime = os.path.getmtime(MAP_FILE)
        self.publish("casc.transmission_map", out)

if __name__ == "__main__":
    mod = CascMapload()
    mod.start()
