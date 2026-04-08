"""
M-24 GEO-INSAR
Sentinel-1 InSAR Displacement Processor
"""

import time
from datetime import datetime, timezone
import dateutil.parser

from modules.base_module import AlphaBaseModule

class GeoInsar(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-24",
            input_topics=["raw.sar_displacement"],
            output_topics=["geo.displacement_processed"]
        )
        self.prev_acq = {}

    def process(self, topic, message):
        if topic == "raw.sar_displacement":
            self._process_sar(message)

    def _process_sar(self, sar_msg):
        acq_str = sar_msg.get("acquisition_date")
        if not acq_str: return
        
        try:
            acq_date = dateutil.parser.isoparse(acq_str)
        except Exception:
            return

        orb = sar_msg.get("orbit_direction", "DESCENDING")
        now_ts_iso = datetime.now(timezone.utc).isoformat() + "Z"
        
        # Clean stale history
        stale_thresh = datetime.now(timezone.utc).timestamp() - (60 * 24 * 3600)
        self.prev_acq = {k: v for k, v in self.prev_acq.items() if v["ts"] > stale_thresh}

        for seg in sar_msg.get("segments", []):
            sid = seg.get("segment_id")
            coh = seg.get("coherence_score", 0.0)
            disp = seg.get("displacement_mm")
            
            coh = max(0.0, min(1.0, float(coh)))
            
            flag = "HIGH_COHERENCE"
            if coh < 0.30:
                flag = "UNUSABLE"
                disp = None
            elif coh < 0.60:
                flag = "LOW_COHERENCE"
                
            rate = None
            days_bw = None
            prev_str = None
            
            if flag != "UNUSABLE" and disp is not None:
                if abs(disp) > 500.0:
                    disp = None
                    flag = "UNUSABLE"
                    self.logger.warning("DISPLACEMENT_PHYSICALLY_IMPOSSIBLE")
                else:
                    if sid not in self.prev_acq:
                        flag = "SINGLE_ACQUISITION"
                    else:
                        prev = self.prev_acq[sid]
                        prev_date = dateutil.parser.isoparse(prev["date"])
                        days_bw = (acq_date - prev_date).total_seconds() / 86400.0
                        prev_str = prev["date"]
                        
                        if days_bw > 0:
                            rate = disp / days_bw
                            
            out = {
                "segment_id": sid,
                "district_code": seg.get("district_code"),
                "river_name": seg.get("river_name"),
                "site_lat": float(seg.get("site_lat", 0.0)),
                "site_lon": float(seg.get("site_lon", 0.0)),
                "displacement_mm": float(disp) if disp is not None else None,
                "displacement_rate_mm_per_day": float(rate) if rate is not None else None,
                "coherence_score": coh,
                "quality_flag": flag,
                "acquisition_date": acq_str,
                "previous_acquisition_date": prev_str,
                "days_between_acquisitions": float(days_bw) if days_bw is not None else None,
                "orbit_direction": orb,
                "timestamp": now_ts_iso
            }
            
            self.publish("geo.displacement_processed", out)
            
            # update hist
            if flag != "UNUSABLE" and disp is not None:
                self.prev_acq[sid] = {"date": acq_str, "ts": acq_date.timestamp(), "disp": disp}

if __name__ == "__main__":
    mod = GeoInsar()
    mod.start()
