"""
m_01_conn_ffwc.py - PROJECT ALPHA
HYBRID LIVE/OFFLINE CONNECTOR: FFWC Flood Intelligence
"""

import requests
import sqlite3
import json
import time
import os
from bs4 import BeautifulSoup
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUS_PATH = os.path.join(BASE_DIR, 'alpha_bus.sqlite')
FALLBACK_PATH = os.path.join(BASE_DIR, 'data', 'ffwc_high_fidelity_baseline.json')

def scrape_ffwc():
    """Scrapes today's water levels from the official FFWC portal."""
    url = "http://ffwc.gov.bd/ffwc_charts/waterlevel.php"
    try:
        print("FFWC Connector: Attempting live telemetry sync...")
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200: raise Exception("Server error")
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.find_all('tr')
        
        stations = {}
        for row in rows[2:]:
            cols = row.find_all('td')
            if len(cols) >= 5:
                s_name = cols[1].text.strip()
                river = cols[0].text.strip()
                level = cols[4].text.strip()
                try: 
                    val = float(level)
                    stations[s_name] = {
                        "river": river,
                        "water_level": val,
                        "status": "LIVE",
                        "timestamp": time.time()
                    }
                except: continue
        
        if not stations: raise Exception("No data found")
        return stations, "LIVE"
    
    except Exception as e:
        print(f"FFWC: Live Sync Failed ({e}). Falling back to High-Fidelity Archive...")
        try:
            with open(FALLBACK_PATH, 'r') as f:
                data = json.load(f)
                return data, "OFFLINE_ARCHIVE"
        except:
            print("FFWC: Critical Error - Fallback archive missing.")
            return None, None

def publish_to_bus(data, source):
    """Publishes the telemetry to the SQLite Bus with source tagging."""
    if not data: return
    
    conn = sqlite3.connect(BUS_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS pubsub 
                      (id INTEGER PRIMARY KEY AUTOINCREMENT, channel TEXT, message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    
    # Wrap message with metadata
    envelope = {
        "module_id": "M-01",
        "source": source,
        "count": len(data),
        "data": data,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    msg = json.dumps(envelope)
    cursor.execute("INSERT INTO pubsub (channel, message) VALUES (?, ?)", ('raw.ffwc', msg))
    
    # Send Heartbeat (Satisfies Watchdog Lock 5)
    hb = {"module_id": "M-01", "status": "OK", "timestamp": datetime.now(timezone.utc).isoformat()}
    cursor.execute("INSERT INTO pubsub (channel, message) VALUES (?, ?)", ("system.heartbeat", json.dumps(hb)))
    
    conn.commit()
    conn.close()
    print(f"FFWC: Published via [{source}]")

if __name__ == "__main__":
    while True:
        telemetry, source = scrape_ffwc()
        if telemetry:
            publish_to_bus(telemetry, source)
        
        time.sleep(300) # Sync every 5 mins
