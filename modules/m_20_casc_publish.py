"""
M-20 CASC-PUBLISH
Cascade Publisher
"""

import time
from datetime import datetime, timezone

from modules.base_module import AlphaBaseModule

class CascPublish(AlphaBaseModule):
    def __init__(self):
        super().__init__(
            module_id="M-20",
            input_topics=["casc.tree_output"],
            output_topics=["cascade_events"]
        )
        self.latest_trees = {}

    def process(self, topic, message):
        dist = message.get("district_code")
        if dist:
            self.latest_trees[dist] = message
            self._publish_cascade(message)

    def _publish_cascade(self, tree_msg):
        dist = tree_msg.get("district_code")
        jp = tree_msg.get("joint_probability_full_cascade", 0.0)
        
        # Alert Level
        al = "LOW"
        if jp >= 0.60: al = "CRITICAL"
        elif jp >= 0.40: al = "HIGH"
        elif jp >= 0.20: al = "MODERATE"
        
        nodes = tree_msg.get("tree_nodes", [])
        branches = tree_msg.get("branches", [])
        
        # Depth
        depth = 0
        if branches:
            # simple longest path
            depth_map = {}
            # find roots
            targets = set([b["to_node_id"] for b in branches])
            roots = [n["node_id"] for n in nodes if n["node_id"] not in targets]
            
            def get_d(n_id):
                ch = [b["to_node_id"] for b in branches if b["from_node_id"] == n_id]
                if not ch: return 1
                return 1 + max([get_d(c) for c in ch])
                
            for r in roots:
                d = get_d(r)
                if d > depth: depth = d
        
        # Path string "HYDRO -> GEO -> AQUA"
        summary = "No active cascade transmissions"
        if nodes and branches:
            # traverse highest prob branch at each point
            targets = set([b["to_node_id"] for b in branches])
            roots = [n for n in nodes if n["node_id"] not in targets]
            if roots:
                curr = roots[0]
                path_str = curr["hazard_domain"]
                
                while True:
                    ch = [b for b in branches if b["from_node_id"] == curr["node_id"]]
                    if not ch: break
                    nxt_b = max(ch, key=lambda x: x["transmission_probability"])
                    nxt_n = next((n for n in nodes if n["node_id"] == nxt_b["to_node_id"]), None)
                    if not nxt_n: break
                    path_str += f" -> {nxt_n['hazard_domain']}"
                    curr = nxt_n
                    
                hz = tree_msg.get("worst_case_horizon_hours", 0)
                summary = f"{path_str} (P={jp:.2f}, horizon={hz:.0f}h)"
                if len(summary) > 200:
                    summary = summary[:197] + "..."

        active_count = sum(1 for t in self.latest_trees.values() if t.get("joint_probability_full_cascade", 0.0) > 0.0)
        
        out = {
            "district_code": dist,
            "district_name": tree_msg.get("district_name", dist),
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",
            "hps_value": tree_msg.get("hps_value"),
            "worst_case_horizon_hours": tree_msg.get("worst_case_horizon_hours"),
            "joint_probability_full_cascade": jp,
            "alert_level": al,
            "summary_worst_cascade_path": summary,
            "district_count_in_cascade_risk": active_count,
            "max_cascade_depth": depth,
            "tree_nodes": nodes,
            "branches": branches
        }
        self.publish("cascade_events", out)

if __name__ == "__main__":
    mod = CascPublish()
    mod.start()
