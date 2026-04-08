"""
M-19 CASC-TREE
Cascade Probability Tree Builder
"""

import time
import json
import os
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

class CascTree(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-19",
            input_topics=["casc.active_transmissions", "pbt.hps_raw"],
            output_topics=["casc.tree_output"]
        )
        self.latest = {}
        self.pop_data = {}
        try:
            with open(os.path.join(DATA_DIR, "district_population.json"), "r") as f:
                self.pop_data = json.load(f)
        except Exception:
            pass

        self.costs = {
            "HYDRO": 12000, "AQUA": 8500, "GEO": 25000,
            "AGRI": 6000, "ATMO": 4000, "ECON": 3500
        }
        self.node_id_counter = 1

    def process(self, topic, message):
        if topic == "pbt.hps_raw":
            dist = message.get("district_code")
            if dist:
                if "HPS" not in self.latest: self.latest["HPS"] = {}
                self.latest["HPS"][dist] = message
            return
            
        if topic == "casc.active_transmissions":
            self._build_tree(message)

    def _get_id(self):
        ident = f"N{self.node_id_counter:03d}"
        self.node_id_counter += 1
        return ident

    def _build_tree(self, msg):
        dist = msg.get("district_code")
        if dist:
            hps_msg = self.latest.get("HPS", {}).get(dist)
            if not hps_msg:
                # E1: HPS not available
                return
                
            hps_val = hps_msg.get("hps_value")
            if hps_val is None or hps_val < 2.5:
                # Eligibility fail
                return
                
            self.node_id_counter = 1
            act_trans = msg.get("active_transmissions", [])
            
            # Root finding
            roots = set([x.get("source_hazard") for x in act_trans])
            if msg.get("active_count", 0) == 0:
                pass
                
            nodes = []
            branches = []
            paths_pjoint = []
            
            pop = self.pop_data.get(dist, {}).get("population")
            
            def recurse(current_hazard, current_node_id, current_prob, accum_hours, path_domains, depth):
                # Search for children matching source == current_hazard
                children = [x for x in act_trans if x.get("source_hazard") == current_hazard]
                is_leaf = True
                
                for c in children:
                    th = c.get("target_hazard")
                    if th in path_domains: continue # Cycle Guard
                    if depth >= 4: continue # Max Depth Guard
                    
                    p_branch = float(c.get("transmission_probability", 0))
                    p_cum = current_prob * p_branch
                    if p_cum < 0.05: continue # Prune Guard
                    
                    is_leaf = False
                    nid = self._get_id()
                    l_hrs = float(c.get("lag_peak_days", 0)) * 24.0
                    acc = accum_hours + l_hrs
                    
                    c_cost = (pop * self.costs.get(th, 0)) if pop else None
                    
                    nodes.append({
                        "node_id": nid, "hazard_domain": th, "probability": p_cum,
                        "time_to_realisation_hours": acc, "affected_population": pop,
                        "cost_estimate_bdt": c_cost, "child_node_ids": []
                    })
                    
                    branches.append({
                        "from_node_id": current_node_id, "to_node_id": nid,
                        "transmission_probability": p_branch, "lag_hours": l_hrs
                    })
                    
                    # Add child link to parent
                    for n in nodes:
                        if n["node_id"] == current_node_id:
                            n["child_node_ids"].append(nid)
                            
                    n_path = set(path_domains)
                    n_path.add(th)
                    recurse(th, nid, p_cum, acc, n_path, depth+1)
                    
                if is_leaf:
                    paths_pjoint.append((current_node_id, current_prob, accum_hours))

            # Trigger root recursions
            for r in roots:
                nid = self._get_id()
                nodes.append({
                    "node_id": nid, "hazard_domain": r, "probability": 1.0,
                    "time_to_realisation_hours": 0.0, "affected_population": pop,
                    "cost_estimate_bdt": (pop*self.costs.get(r,0)) if pop else None,
                    "child_node_ids": []
                })
                recurse(r, nid, 1.0, 0.0, set([r]), 1)
                
            wc_horizon = max([x[2] for x in paths_pjoint]) if paths_pjoint else 0.0
            jp_full = max([x[1] for x in paths_pjoint]) if paths_pjoint else 0.0

            out = {
                "district_code": dist,
                "district_name": msg.get("district_name", dist),
                "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
                "hps_value": float(hps_val),
                "worst_case_horizon_hours": float(wc_horizon),
                "joint_probability_full_cascade": float(jp_full),
                "tree_nodes": nodes,
                "branches": branches
            }
            self.publish("casc.tree_output", out)

if __name__ == "__main__":
    mod = CascTree()
    mod.start()
