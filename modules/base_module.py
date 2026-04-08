"""
modules/base_module.py — The Enterprise Compiler
Serves as the rigid framework that forces all 51 domain models to comply 
with the Error Protocol, Concurrency Protocols, and Schema Guardian constants.
"""

import os
import json
import time
import threading
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

# Must import from core.database directly
import sys
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# PORTABLE LIBRARY INJECTION
LIB_DIR = os.path.join(ROOT_DIR, "libs")
if LIB_DIR not in sys.path:
    sys.path.insert(0, LIB_DIR)

from core.database import AlphaDatabase

class TimeoutException(Exception):
    pass

class NullFlaggedException(Exception):
    """Raised natively when a module decides to explicitly fail with an E_XX code."""
    def __init__(self, error_code: str, message: str, recoverable: bool = True):
        self.code = error_code
        self.message = message
        self.recoverable = recoverable
        super().__init__(self.message)


class AlphaBaseModule:
    """
    All 51 modules MUST inherit from this class. 
    It enforces timeouts, error structures, and database locking.
    """
    
    def __init__(self, module_id: str, input_topics: List[str], output_topics: List[str], poll_interval: float = 3.0):
        self.module_id = module_id
        self.input_topics = input_topics
        self.output_topics = output_topics
        self.poll_interval = poll_interval
        
        self.logger = logging.getLogger(f"ALPHA.{self.module_id}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
            self.logger.addHandler(ch)

        # Load environment variables natively (Zero-Dependency rule)
        self._load_env()
        
        # Pendrive Constraints: Initialize SQLite
        self.db = AlphaDatabase(check_same_thread=False)
        self.last_seen_message_ids: Dict[str, int] = {topic: 0 for topic in input_topics}
        self.is_running = False
        
        # Performance Tracking
        self.messages_processed = 0
        self.start_time = time.monotonic()
        self.error_count = 0
        self.current_status = "HEALTHY"
        self.consecutive_errors = 0

    def _load_env(self):
        """Zero-dependency parser for d:\ALPHA\.env to load API keys securely."""
        env_path = os.path.join(ROOT_DIR, ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            key_val = line.split("=", 1)
                            if len(key_val) == 2:
                                os.environ[key_val[0].strip()] = key_val[1].strip()
            except Exception as e:
                self.logger.warning(f"Failed to read .env file: {e}")

    def start(self):
        """Starts the main polling thread and heartbeat thread."""
        self.is_running = True
        
        # Start Heartbeat thread
        hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True, name=f"{self.module_id}-hb")
        hb_thread.start()

        # Main Loop (Replacing Redis Subscribe blocking)
        self.logger.info(f"{self.module_id} started. Polling SQLite every {self.poll_interval}s.")
        try:
            while self.is_running:
                self._poll_inputs()
                time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.logger.info(f"{self.module_id} shutting down manually.")
            self.stop()
        except Exception as e:
            self.logger.critical(f"Fatal module crash: {e}")
            self.stop()

    def stop(self):
        self.is_running = False
        self.db.close()

    def _heartbeat_loop(self):
        """UPSERTs status into module_state every 15s to keep Orchestrator happy."""
        hb_interval = 15.0
        while self.is_running:
            uptime = time.monotonic() - self.start_time
            self.db.update_heartbeat(
                module_id=self.module_id,
                status=self.current_status,
                last_hb=time.monotonic(),
                uptime=uptime,
                err_count=self.error_count
            )
            time.sleep(hb_interval)

    def _poll_inputs(self):
        """Queries the DB for new messages sequentially."""
        for topic in self.input_topics:
            last_id = self.last_seen_message_ids[topic]
            new_msgs = self.db.fetch_new_messages(topic, last_id)
            
            for msg_id, payload in new_msgs:
                self.logger.debug(f"Received msg #{msg_id} on {topic}")
                self._handle_message(topic, payload)
                self.last_seen_message_ids[topic] = msg_id

    def _handle_message(self, topic: str, data: Dict):
        """Handles incoming messages and strictly checks for upstream Errors."""
        
        # Error Protocol Rule 6.1: Check upstream _error blocks.
        if "_error" in data:
            self.logger.warning(f"Received propagated upstream error from {data['_error']['source_module']}")
            propagated = self._build_null_flagged_output(
                error_code=data["_error"]["code"],
                message=f"Propagated from {data['_error']['source_module']} via {self.module_id}: {data['_error']['message'][:300]}"
            )
            propagated["_error"]["source_module"] = data["_error"]["source_module"]
            self.publish_all(propagated)
            return

        # Regular Processing with Timeout Catch
        try:
            output = self._process_with_timeout(topic, data, timeout_seconds=300)
            if output:
                self.publish_all(output)
            
            # Reset health
            self.current_status = "HEALTHY"
            self.consecutive_errors = 0
            self.messages_processed += 1

        except TimeoutException as e:
            self.logger.error(f"E6: RESOURCE_EXHAUSTION (Timeout). {e}")
            self.error_count += 1
            self.current_status = "DEGRADED"
            err_out = self._build_null_flagged_output("E6", f"process() exceeded 300s timeout.", recoverable=True)
            self.publish_all(err_out)

        except NullFlaggedException as nfe:
            self.logger.error(f"{nfe.code}: {nfe.message}")
            self.error_count += 1
            err_out = self._build_null_flagged_output(nfe.code, nfe.message, nfe.recoverable)
            self.publish_all(err_out)

        except Exception as e:
            # Catching E3: Computation Failure
            self.logger.error(f"E3: COMPUTATION_FAILURE. {traceback.format_exc()}")
            self.error_count += 1
            self.current_status = "DEGRADED"
            self.consecutive_errors += 1
            if self.consecutive_errors > 5:
                self.current_status = "ERROR"

            err_msg = f"process() raised {type(e).__name__}: {str(e)[:400]}"
            err_out = self._build_null_flagged_output("E3", err_msg, recoverable=True)
            self.publish_all(err_out)

    def _process_with_timeout(self, topic: str, data: Dict, timeout_seconds: int = 300) -> Optional[Dict]:
        """Runs Windows-Safe Threading loop to prevent E6 Freezes."""
        result = [None]
        exception = [None]
        
        def target():
            try:
                result[0] = self.process(topic, data)
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            raise TimeoutException(f"Execution exceeded {timeout_seconds}s limit.")
        if exception[0]:
            raise exception[0]
        return result[0]

    def _build_null_flagged_output(self, error_code: str, message: str, recoverable: bool = True) -> Dict:
        """Constructs an empty/null dictionary compliant with contracts."""
        empty_payload = {}  # In a highly strict typed system we'd fill all contract keys as `null` here.
        empty_payload["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
        empty_payload["_error"] = {
            "code": error_code,
            "source_module": self.module_id,
            "message": message,
            "recoverable": recoverable,
            "timestamp": empty_payload["timestamp"]
        }
        return empty_payload

    def publish(self, topic: str, data: Dict):
        """Pushes to SQLite."""
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(timezone.utc).isoformat() + "Z"
        self.db.publish_message(topic, self.module_id, data)

    def publish_all(self, data: Dict):
        """Publishes the same payload to all output topics."""
        for t in self.output_topics:
            self.publish(t, data)

    def get_fallback_demo_data(self) -> Optional[Dict]:
        """
        Vulnerability Defense Rule 1: Offshore/Demo API Fail.
        Loads data/archive/M_XX_demo.json 
        """
        archive_path = os.path.join(ROOT_DIR, "data", "archive", f"{self.module_id}_demo.json")
        try:
            with open(archive_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Could not load fallback offline data from {archive_path}: {e}")
            return None

    # =========================================================================
    # CORE METHOD TO BE OVERRIDDEN BY THE ACTUAL MODULE
    # =========================================================================
    def process(self, topic: str, data: Dict) -> Optional[Dict]:
        """
        MUST BE OVERWRITTEN BY SUBCLASSES.
        Processes incoming payload and returns a dictionary to publish.
        """
        raise NotImplementedError("Modules must implement the process() function.")
