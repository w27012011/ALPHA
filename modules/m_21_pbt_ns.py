"""
M-21 PBT-NS
Novelty Scorer
"""

import os
from collections import deque
import math
from datetime import datetime, timezone

try:
    import numpy as np
    from scipy.stats import gaussian_kde
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

from modules.base_module import AlphaBaseModule

class PbtNs(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-21",
            input_topics=[
                "pbt.hps_raw", "flood_predictions", "arsenic_alerts",
                "erosion_alerts", "seismic_events", "crop_stress",
                "lightning_alerts", "storm_forecast"
            ],
            output_topics=["pbt.ns_score"]
        )
        self.latest = {}
        self.history = {}

    def process(self, topic, message):
        self.latest[topic] = message
        if topic == "pbt.hps_raw":
            self._compute_novelty(message)

    def _compute_novelty(self, hps_msg):
        dist = hps_msg.get("district_code")
        if not dist:
            return

        lt = self.latest
        # Assemble vector
        v = [
            float(lt.get("flood_predictions", {}).get("flood_probability", 0)),
            0.0, # arsenic ratio
            0.0, # erosion
            float(lt.get("crop_stress", {}).get("stress_index", 0)),
            float(lt.get("lightning_alerts", {}).get("lightning_density_strikes_km2", 0)),
            0.0 # cyclone max
        ]
        
        # Pull Aqua
        aq = lt.get("arsenic_alerts", {})
        if aq:
            wa = max(1, aq.get("wells_assessed", 1))
            v[1] = float(aq.get("wells_flagged", 0)) / wa
            
        # Pull Erosion
        ero = lt.get("erosion_alerts", {})
        if ero:
            prs = [s.get("erosion_probability", 0) for s in ero.get("high_risk_segments", [])]
            if prs: v[2] = max(prs)
            
        # Pull Storm
        st = lt.get("storm_forecast", {})
        if st:
            drs = [d.get("impact_probability", 0) for d in st.get("districts_at_risk", [])]
            if drs: v[5] = max(drs)
            
        # Clamp bounds
        v = [max(0.0, min(1.0, float(x))) for x in v]
        
        if dist not in self.history:
            self.history[dist] = deque(maxlen=17520)
            
        self.history[dist].append(v)
        hx = list(self.history[dist])
        
        # Build Active Str
        ahs = []
        if v[0] > 0.1: ahs.append("flood")
        if v[1] > 0.1: ahs.append("arsenic")
        if v[2] > 0.1: ahs.append("erosion")
        if v[3] > 0.1: ahs.append("crop stress")
        if v[4] > 0.1: ahs.append("lightning")
        if v[5] > 0.1: ahs.append("cyclone")
        c_desc = " + ".join(ahs) if ahs else "no active hazard"

        ns = None
        prank = None
        sb_flag = False
        n_analogues = None
        kde_d = None

        if len(hx) >= 365:
            if HAS_SCIPY:
                try:
                    H = np.array(hx).T # shape 6 x N
                    kde = gaussian_kde(H, bw_method='scott')
                    x_c = np.array(v)
                    kde_d = float(kde.evaluate(x_c)[0])
                    
                    scores = kde.evaluate(H)
                    max_d = max(float(np.max(scores)), 1e-12)
                    prank = float(np.sum(scores <= kde_d)) / len(hx) * 100.0
                    
                    ns = max(0.0, min(1.0, 1.0 - (kde_d / max_d)))
                    sb_flag = (prank > 99.0)
                    
                    stds = np.std(H, axis=1)
                    sig_cmb = float(np.mean(stds))
                    
                    diffs = H.T - x_c
                    dists = np.sqrt(np.sum(diffs**2, axis=1))
                    n_analogues = int(np.sum(dists <= sig_cmb))
                    
                except Exception as e:
                    self.logger.warning(f"KDE Failed: {e}")
            else:
                # Euclidean Fallback
                # Find std of each dim to use as weight
                avgs = [sum([r[i] for r in hx])/len(hx) for i in range(6)]
                stds = []
                for i in range(6):
                    vr = sum([(r[i]-avgs[i])**2 for r in hx])/len(hx)
                    stds.append(math.sqrt(vr))
                    
                sig_cmb = sum(stds)/6.0 if sum(stds)>0 else 0.01
                
                # compute distances from c
                dists = []
                for r in hx:
                    d = math.sqrt(sum(((r[i]-v[i])**2 for i in range(6))))
                    dists.append(d)
                
                curr_dist = 0.0 # distance to itself is zero, but distance to mean? 
                # Let's say Novelty is relative to the mean distance of all points to their center
                mean_dist = sum(dists)/len(dists) if dists else 1.0
                
                ns = min(1.0, mean_dist / (sig_cmb*5)) # rough approx
                prank = sum(1 for d in dists if d > mean_dist) / len(hx) * 100.0
                sb_flag = (ns > 0.8) # rough fallback rule
                n_analogues = sum(1 for d in dists if d <= (sig_cmb))
                kde_d = 0.0 # not computed
                
        out = {
            "district_code": dist,
            "district_name": hps_msg.get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "novelty_score": float(ns) if ns is not None else None,
            "percentile_rank": float(prank) if prank is not None else None,
            "structural_break_flag": sb_flag,
            "historical_analogues_found": n_analogues,
            "combination_description": c_desc,
            "kde_density": kde_d,
            "history_length_days": len(hx)
        }
        
        self.publish("pbt.ns_score", out)

if __name__ == "__main__":
    mod = PbtNs()
    mod.start()
