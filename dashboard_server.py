import os
import json
import sqlite3
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from datetime import datetime, timezone

DB_PATH = "alpha_bus.sqlite"
PUBLIC_DIR = "public"
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8080

class DashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # M-89: Suppress default logs to focus on tactical telemetry
        pass

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        path = parsed_path.path

        if path == "/":
            self.send_response(302)
            self.send_header("Location", "/strat_master.html")
            self.end_headers()
            return

        if path.startswith("/api/bus"):
            self.handle_api_bus()
        elif path.startswith("/api/heartbeats"):
            self.handle_api_heartbeats()
        elif path.startswith("/api/alerts"):
            self.handle_api_alerts()
        elif path.startswith("/data/"):
            rest = path.replace("/data/", "").lstrip("/")
            self.handle_static_file("data", rest)
        else:
            self.handle_static_file("public", path.lstrip("/"))

    def handle_static_file(self, base_dir, relative_path):
        full_path = os.path.normpath(os.path.join(ROOT_DIR, base_dir, relative_path))
        
        # Security/Consistency Check
        if not full_path.startswith(ROOT_DIR):
            self.send_error(403, "Access Denied")
            return

        print(f"[*] ALPHABUS-FS: Serving {full_path}") # M-89: Verified Telemetry path
        
        if not os.path.exists(full_path) or os.path.isdir(full_path):
            print(f"[!] ALPHABUS-FS: 404 NOT FOUND - {full_path}")
            self.send_error(404, f"File not found: {relative_path}")
            return

        self.send_response(200)
        ext = os.path.splitext(full_path)[1].lower()
        content_type = {
            ".html": "text/html", ".css": "text/css", ".js": "application/javascript",
            ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg",
            ".geojson": "application/json", ".webp": "image/webp"
        }.get(ext, "application/octet-stream")
        
        self.send_header("Content-type", content_type)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            with open(full_path, "rb") as f:
                self.wfile.write(f.read())
        except Exception as e:
            print(f"[!] ALPHABUS-FS: Read Error - {str(e)}")

    def handle_api_bus(self):
        # M-89: Thread-safe DB access (read-only)
        self._send_db_resp("SELECT id, channel, message, timestamp FROM pubsub ORDER BY id DESC LIMIT 100")

    def handle_api_heartbeats(self):
        self._send_db_resp("SELECT message, timestamp FROM pubsub WHERE channel = 'system.heartbeat' ORDER BY id DESC LIMIT 500", is_hb=True)

    def handle_api_alerts(self):
        self._send_db_resp("SELECT id, channel, message, timestamp FROM pubsub WHERE channel IN ('system.alert', 'system.dlq') ORDER BY id DESC LIMIT 20")

    def _send_db_resp(self, query, is_hb=False):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        try:
            # Use a fresh connection per thread for safety
            with sqlite3.connect(DB_PATH, timeout=5) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(query)
                rows = cur.fetchall()
                data = [dict(row) for row in rows]
                for r in data:
                    try: r["message"] = json.loads(r["message"])
                    except: pass
                
                if is_hb:
                    hb_map = {}
                    for r in data:
                        mid = r["message"].get("module_id")
                        if mid and mid not in hb_map:
                            hb_map[mid] = {"status": r["message"].get("status"), "timestamp": r["timestamp"]}
                    self.wfile.write(json.dumps(hb_map).encode('utf-8'))
                else:
                    self.wfile.write(json.dumps(data).encode('utf-8'))
        except Exception as e:
            self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread for concurrent map loads."""
    daemon_threads = True

def main():
    server_address = ('', 8080)
    httpd = ThreadedHTTPServer(server_address, DashboardHandler)
    print(f"Alpha THREADED Tactical Server running on http://localhost:8080")
    print(f"ROOT: {ROOT_DIR}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server.")

if __name__ == '__main__':
    main()
