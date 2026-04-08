"""
M-42 ECON-CRISIS
Economic Crisis Dashboard Monitor
"""

import math
from datetime import datetime, timezone
from modules.base_module import AlphaBaseModule

def invert_matrix(m):
    # Pure python implementation of Matrix Inversion using Gaussian elimination 
    # Because numpy/scipy might not be on the pendrive!
    n = len(m)
    mat = [row[:] for row in m]
    inv = [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]
    
    for i in range(n):
        pivot = mat[i][i]
        if pivot == 0:
            for k in range(i+1, n):
                if mat[k][i] != 0:
                    mat[i], mat[k] = mat[k], mat[i]
                    inv[i], inv[k] = inv[k], inv[i]
                    pivot = mat[i][i]
                    break
            if pivot == 0:
                raise ValueError("Singular matrix")
                
        for j in range(n):
            mat[i][j] /= pivot
            inv[i][j] /= pivot
            
        for k in range(n):
            if k == i: continue
            factor = mat[k][i]
            for j in range(n):
                mat[k][j] -= factor * mat[i][j]
                inv[k][j] -= factor * inv[i][j]
                
    return inv

def mat_mul(a, b):
    # Vector-matrix (1xN * NxN) returning 1xN
    n = len(a)
    res = [0.0]*n
    for j in range(n):
        for k in range(n):
            res[j] += a[k] * b[k][j]
    return res

def dot_prod(a, b):
    return sum(a[i]*b[i] for i in range(len(a)))

class EconCrisis(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-42",
            input_topics=["econ.domestic_output", "econ.global_output", "econ.reserve_status", "raw.economic_indicators"],
            output_topics=["economic_pressure"]
        )
        self.dom = {}
        self.glo = {}
        self.res = {}
        self.econ = {}
        
        # Means (mu)
        self.mu = [5.5, 6.0, 3.5, 5.0, 5.0, 15.0]
        # Standard Devs (sigma)
        self.sigma = [2.0, 1.5, 1.5, 1.0, 5.0, 5.0]
        
        # Covariance (Identity for uncorrelated variables fallback)
        self.cov = [
            [1.0, -0.2, 0.3, 0.4, 0.1, 0.2],
            [-0.2, 1.0, -0.6, -0.1, -0.3, -0.4],
            [0.3, -0.6, 1.0, 0.5, 0.4, 0.5],
            [0.4, -0.1, 0.5, 1.0, 0.2, 0.3],
            [0.1, -0.3, 0.4, 0.2, 1.0, 0.1],
            [0.2, -0.4, 0.5, 0.3, 0.1, 1.0]
        ]

    def process(self, topic, message):
        if topic == "econ.domestic_output": self.dom = message
        elif topic == "econ.global_output": self.glo = message
        elif topic == "econ.reserve_status": self.res = message
        elif topic == "raw.economic_indicators": self.econ = message
        self._process_crisis()

    def _process_crisis(self):
        # Base indicators
        base_inf = self.econ.get("inflation_cpi_yoy_pct", 6.0)
        base_fx = self.econ.get("forex_reserves", 5.0)
        base_ca = self.econ.get("current_account_deficit_pct_gdp", 4.0)
        base_fs = self.econ.get("fiscal_deficit_pct_gdp", 5.0)
        base_rer = self.econ.get("real_exchange_rate_deviation_pct", 2.0)
        base_dsr = self.econ.get("debt_service_ratio_pct_exports", 15.0)
        
        # Adjustments
        inf = float(base_inf) + float(self.dom.get("inflation_acceleration_pct", 0.0))
        fx_p = float(self.dom.get("forex_pressure_score", 0.0))
        fx = float(base_fx) - (fx_p * 0.5)
        
        x = [inf, fx, float(base_ca), float(base_fs), float(base_rer), float(base_dsr)]
        
        # Normalize
        x_norm = [(x[i] - self.mu[i]) / self.sigma[i] for i in range(6)]
        
        # Mahalanobis
        ehps = None
        try:
            cov_inv = invert_matrix(self.cov)
            temp = mat_mul(x_norm, cov_inv)
            ehps = math.sqrt(dot_prod(temp, x_norm))
        except Exception:
            # Fallback Euclidean
            self.logger.warning("MAHALANOBIS_FAILED_USING_EUCLIDEAN")
            ehps = math.sqrt(sum(val**2 for val in x_norm))
            
        # Zones
        cz = 0
        sz = 0
        wz = 0
        
        c_t = [15.0, 2.0, 8.0, 8.0, 35.0, 35.0]
        s_t = [10.0, 3.5, 6.0, 6.5, 25.0, 25.0]
        w_t = [7.5, 4.5, 4.5, 5.5, 15.0, 20.0]
        
        # Check crisis thresholds. Note fx is reversed (lower is worse)
        # We handle this manually:
        if x[0] > c_t[0]: cz += 1
        elif x[0] > s_t[0]: sz += 1
        elif x[0] > w_t[0]: wz += 1
        
        if x[1] < c_t[1]: cz += 1
        elif x[1] < s_t[1]: sz += 1
        elif x[1] < w_t[1]: wz += 1
        
        for i in range(2, 6):
            if x[i] > c_t[i]: cz += 1
            elif x[i] > s_t[i]: sz += 1
            elif x[i] > w_t[i]: wz += 1
            
        al = False
        st = "NORMAL"
        
        if ehps is not None:
            if ehps > 3.5: st = "CRISIS"
            elif ehps > 2.5: st = "ALERT"
            elif ehps > 1.5: st = "ELEVATED"
            
        if ehps is not None and ehps > 2.5 or cz > 0:
            al = True
            
        rec = []
        if al:
            rec.append("ACTIVATE_STRATEGIC_RESERVES")
            rec.append("RESTRICT_NON_ESSENTIAL_IMPORTS")
            
        out = {
            "economic_hps": float(ehps) if ehps is not None else None,
            "current_account_deficit_pct_gdp": float(x[2]),
            "forex_reserves_months_import": float(x[1]),
            "inflation_rate_cpi_pct": float(x[0]),
            "fiscal_deficit_pct_gdp": float(x[3]),
            "real_exchange_rate_deviation_pct": float(x[4]),
            "debt_service_ratio_pct_exports": float(x[5]),
            "indicators_in_watch_zone": wz,
            "indicators_in_stress_zone": sz,
            "indicators_in_crisis_zone": cz,
            "ehps_status": st,
            "alert_triggered": al,
            "intervention_recommendations": rec,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        }
        self.publish("economic_pressure", out)

if __name__ == "__main__":
    mod = EconCrisis()
    mod.start()
