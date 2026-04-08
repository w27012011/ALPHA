"""
M-46 UPDATE-PF
Particle Filter
"""

import math
import random
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

def pdf_norm(x, mu, sigma):
    if sigma == 0: return 1.0 if x == mu else 0.0
    return math.exp(-0.5 * ((x - mu) / sigma)**2) / (sigma * math.sqrt(2 * math.pi))

class UpdatePf(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-46",
            input_topics=["nowcast_state", "pbt.ns_score", "seismic_events"],
            output_topics=["belief_updates"]
        )
        self.ns = {}
        self.seis = {}
        self.n_particles = 1000
        self.particles = {}
        self.weights = {}

    def process(self, topic, message):
        if topic == "pbt.ns_score":
            self.ns = message
        elif topic == "seismic_events":
            self.seis = message
        elif topic == "nowcast_state":
            self._process_pf(message)

    def _should_activate(self):
        # M4.5+ EQ
        mag = self.seis.get("magnitude", 0.0)
        dist = self.seis.get("distance_to_epicenter_km", 999.0)
        if mag >= 4.5 and dist <= 200.0:
            return True
            
        # NS struct break
        b_flag = self.ns.get("structural_break_flag", False)
        # Without state tracking, we just check if it's currently breaking
        if b_flag:
            return True
            
        return False

    def _init_particles(self, eng, prior_mean, prior_std):
        self.particles[eng] = [random.gauss(prior_mean, prior_std) for _ in range(self.n_particles)]
        self.weights[eng] = [1.0 / self.n_particles] * self.n_particles

    def _resample(self, eng):
        pts = self.particles[eng]
        wts = self.weights[eng]
        
        # Systematic Resampling
        new_pts = []
        c = [0.0] * self.n_particles
        c[0] = wts[0]
        for i in range(1, self.n_particles):
            c[i] = c[i-1] + wts[i]
            
        u1 = random.uniform(0, 1.0 / self.n_particles)
        i = 0
        for j in range(self.n_particles):
            uj = u1 + float(j) / self.n_particles
            while uj > c[i]:
                i += 1
                if i >= self.n_particles: i = self.n_particles - 1
            new_pts.append(pts[i])
            
        self.particles[eng] = new_pts
        self.weights[eng] = [1.0 / self.n_particles] * self.n_particles

    def _process_pf(self, nowcast_msg):
        if not self._should_activate():
            return
            
        engines = nowcast_msg.get("per_engine_nowcast", {})
        
        for eng, data in engines.items():
            z_t = data.get("state_estimate", 0.0)
            conf = data.get("confidence", 0.5)
            
            R = max(0.01, 1.0 - conf)
            
            # Init if needed
            if eng not in self.particles:
                self._init_particles(eng, z_t, math.sqrt(R))
                
            pts = self.particles[eng]
            wts = self.weights[eng]
            
            # Predict & Update Weights
            sum_w = 0.0
            for i in range(self.n_particles):
                # Transition (identity + noise)
                pts[i] = max(0.0, min(1.0, pts[i] + random.gauss(0, 0.05)))
                
                # Likelihood
                wts[i] *= pdf_norm(z_t, pts[i], math.sqrt(R))
                sum_w += wts[i]
                
            hlth = "OPTIMAL"
            # Normalize
            if sum_w < 1e-10:
                self.logger.critical("PARTICLE_DEPLETION")
                self._init_particles(eng, z_t, math.sqrt(R))
                pts = self.particles[eng]
                wts = self.weights[eng]
                hlth = "PARTICLE_DEPLETION"
            else:
                for i in range(self.n_particles):
                    wts[i] /= sum_w
                    
            # Estimate
            state_est = sum(pts[i] * wts[i] for i in range(self.n_particles))
            state_var = sum(wts[i] * (pts[i] - state_est)**2 for i in range(self.n_particles))
            
            # ESS
            sq_w = sum(w**2 for w in wts)
            ess = 1.0 / sq_w if sq_w > 0 else 0
            
            resamp = False
            if ess < self.n_particles / 2.0:
                self._resample(eng)
                resamp = True
                
            out = {
                "engine_name": eng,
                "filter_type": "PARTICLE",
                "state_estimate": float(state_est),
                "state_variance": float(state_var),
                "n_particles": self.n_particles,
                "effective_sample_size": float(ess),
                "resampling_triggered": resamp,
                "filter_health": hlth,
                "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            }
            self.publish("belief_updates", out)

if __name__ == "__main__":
    mod = UpdatePf()
    mod.start()
