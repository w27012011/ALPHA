"""
M-03 CONN-USGS
USGS Earthquake Event Connector
Tier-0 Connector Module
"""

import os
import json
import time
import math
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

from modules.base_module import AlphaBaseModule

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")

def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on the earth."""
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371 # Radius of earth in kilometers
    return c * r

class ConnUSGS(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-03", 
            input_topics=[], 
            output_topics=["raw.earthquake_events"],
            poll_interval=1.0
        )
        self.poll_interval_seconds = 300 # 5 minutes
        self.last_fetch_time = 0
        self.cache_file = os.path.join(CACHE_DIR, "M-03_last_events.json")
        os.makedirs(CACHE_DIR, exist_ok=True)
        
        # In-memory dedup cache
        self.dedup_cache = {}
        self.dedup_window_hours = 24

    def _poll_inputs(self):
        now = time.monotonic()
        if now - self.last_fetch_time >= self.poll_interval_seconds:
            self.last_fetch_time = now
            self._execute_fetch()

    def _execute_fetch(self):
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=30)
        
        # API params
        params = {
            "format": "geojson",
            "starttime": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "endtime": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
            "minmagnitude": 3.5,
            "latitude": 23.7,
            "longitude": 90.4,
            "maxradiuskm": 1000
        }
        query_string = urllib.parse.urlencode(params)
        url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?{query_string}"
        
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=15) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode('utf-8'))
                    self._process_events(data)
                    self.current_status = "HEALTHY"
        except Exception as e:
            self.logger.warning(f"USGS API fetch failed: {e}")
            self.current_status = "DEGRADED"

    def _process_events(self, data):
        features = data.get("features", [])
        new_events_found = False
        
        for feature in features:
            props = feature.get("properties", {})
            geom = feature.get("geometry", {})
            
            event_id = feature.get("id")
            if not event_id or event_id in self.dedup_cache:
                continue
                
            self.dedup_cache[event_id] = datetime.utcnow()
            new_events_found = True
            
            coords = geom.get("coordinates", [])
            lat, lon, depth = None, None, None
            if len(coords) >= 3:
                lon, lat, depth = coords[0], coords[1], coords[2]

            if lat is None or lon is None:
                continue

            mag = props.get("mag")
            if mag is None or mag < 0 or mag > 10:
                continue

            # Calculate distance to Bangladesh Box
            # Bbox: lat [20.74°N, 26.63°N], lon [88.01°E, 92.67°E]
            lat2 = max(20.74, min(lat, 26.63))
            lon2 = max(88.01, min(lon, 92.67))
            dist = haversine(lat, lon, lat2, lon2)
            
            felt = False
            if dist < 500 or mag > 6.0:
                felt = True

            # Calculate PGA
            pga = None
            if dist <= 1000:
                dhaka_dist = haversine(lat, lon, 23.81, 90.41)
                r_hypo = math.sqrt(dhaka_dist**2 + (depth or 10)**2)
                # Boore-Atkinson
                c1, c2, c3, c4 = -2.991, 1.414, -1.693, -0.00607
                ln_pga = c1 + c2*mag + c3*math.log(r_hypo) + c4*r_hypo
                pga = math.exp(ln_pga)

            msg = {
                "event_id": str(event_id),
                "magnitude": float(mag),
                "depth_km": float(depth) if depth is not None else None,
                "lat": float(lat),
                "lon": float(lon),
                "place_description": props.get("place"),
                "event_time": datetime.utcfromtimestamp(props.get("time", 0)/1000.0).isoformat() + "Z" if props.get("time") else datetime.now(timezone.utc).isoformat() + "Z",
                "distance_to_bangladesh_km": float(dist),
                "felt_in_bangladesh": felt,
                "pga_estimate_g": float(pga) if pga is not None else None,
                "data_source": "USGS_API",
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z"
            }

            self.publish("raw.earthquake_events", msg)

        # Prune dedup cache
        now = datetime.utcnow()
        cutoff = now - timedelta(hours=self.dedup_window_hours)
        self.dedup_cache = {k: v for k, v in self.dedup_cache.items() if v > cutoff}

if __name__ == "__main__":
    mod = ConnUSGS()
    mod.start()
