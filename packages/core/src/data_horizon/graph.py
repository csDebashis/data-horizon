"""Directed schedule connectivity and bounded read-only scenario estimates."""

from __future__ import annotations

import heapq
import math
from collections import defaultdict


def canonical_edges(edges):
    result = {}
    for e in edges:
        cost = float(e.get("travel_seconds", e.get("transfer_time", 180)))
        if not math.isfinite(cost) or cost < 0:
            raise ValueError("Edge costs must be finite and non-negative")
        source, target = e["source"], e["target"]
        if len(source.split("|")) != 3 or len(target.split("|")) != 3:
            raise ValueError("Graph node must be route|direction|stop")
        item = {**e, "travel_seconds": cost, "weight": float(e.get("weight", 1))}
        key = (source, target, e.get("edge_type", "travel"))
        if key not in result or cost < result[key]["travel_seconds"]:
            result[key] = item
    return [result[k] for k in sorted(result)]


def what_if(edges, route_id, stop_id, delay_seconds, horizon_minutes=30, limit=20):
    if not 0 <= delay_seconds <= 7200 or not 1 <= horizon_minutes <= 60 or not 1 <= limit <= 100:
        raise ValueError("Scenario input outside supported limits")
    adjacency = defaultdict(list)
    nodes = set()
    for e in canonical_edges(edges):
        adjacency[e["source"]].append((e["target"], e["travel_seconds"]))
        nodes.update((e["source"], e["target"]))
    origins = sorted(n for n in nodes if n.split("|")[0] == route_id and n.split("|")[2] == stop_id)
    if not origins:
        raise LookupError("Origin is not present in the configured graph")
    output = {
        "origin": {"route_id": route_id, "stop_id": stop_id},
        "horizon_minutes": horizon_minutes,
        "delay_seconds": delay_seconds,
        "affected_stations": [],
        "method": "directed scheduled-travel reachability with exponential impact decay",
        "limitations": [
            "Scenario estimate; not a causal guarantee",
            "Connectivity does not establish shared track or vehicle circulation",
        ],
    }
    if delay_seconds == 0:
        return output
    frontier = [(0, n, [n]) for n in origins]
    heapq.heapify(frontier)
    best = {n: 0 for n in origins}
    found = {}
    while frontier:
        seconds, node, path = heapq.heappop(frontier)
        if seconds > best.get(node, float("inf")):
            continue
        if node not in origins:
            route, direction, stop = node.split("|")
            found[node] = {
                "node_id": node,
                "route_id": route,
                "direction_id": direction,
                "stop_id": stop,
                "travel_seconds": seconds,
                "estimated_delay_seconds": round(
                    delay_seconds * math.exp(-seconds / (horizon_minutes * 60)), 2
                ),
                "path": path,
            }
        for target, cost in adjacency[node]:
            new = seconds + cost
            if new <= horizon_minutes * 60 and new < best.get(target, float("inf")):
                best[target] = new
                heapq.heappush(frontier, (new, target, path + [target]))
    output["affected_stations"] = sorted(
        found.values(), key=lambda x: (-x["estimated_delay_seconds"], x["node_id"])
    )[:limit]
    return output


def haversine_metres(lat1, lon1, lat2, lon2):
    for lat, lon in ((lat1, lon1), (lat2, lon2)):
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise ValueError("Invalid WGS84 coordinates")
    a, b, c, d = map(math.radians, (lat1, lon1, lat2, lon2))
    h = math.sin((c - a) / 2) ** 2 + math.cos(a) * math.cos(c) * math.sin((d - b) / 2) ** 2
    return 6371008.8 * 2 * math.asin(min(1, math.sqrt(h)))
