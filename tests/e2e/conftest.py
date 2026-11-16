"""Browser fixtures for the small, account-free dashboard."""
from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {**browser_context_args, "timezone_id": "Asia/Kolkata", "viewport": {"width": 1440, "height": 1000}}


@pytest.fixture
def dashboard_url():
    return os.getenv("DASHBOARD_URL", "http://127.0.0.1:8501").rstrip("/")


@pytest.fixture(autouse=True)
def no_browser_errors(page):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    yield
    assert not errors, f"Uncaught browser errors: {errors}"


@pytest.fixture
def screenshot_dir():
    folder = Path(os.getenv("BROWSER_SCREENSHOT_DIR", "reports/project-baseline/browser"))
    folder.mkdir(parents=True, exist_ok=True)
    return folder


@pytest.fixture
def sample_api(page):
    """Controlled synthetic data verifies UI behaviour; it is not live integration evidence."""
    state = {"calls": [], "fail": set(), "mode": "synthetic", "hold_status": False, "held_status": [],
             "unmatched_schedule": False}
    instant = "2026-11-19T06:30:00Z"
    health_clock = {"health_source_as_of": instant, "health_age_seconds": 2,
                    "health_freshness_seconds": 120, "health_retention_seconds": 300,
                    "health_retention_eligible": True}
    routes = [{"route_id": "1", "route_long_name": "Broadway local", "health_score": 85, "health_level": "Good", **health_clock},
              {"route_id": "2", "route_long_name": "Seventh Avenue express", "health_score": 70, "health_level": "Watch", **health_clock}]
    stops = [{"stop_id": "101N", "stop_name": "First Station", "stop_lat": 40.7, "stop_lon": -73.9},
             {"stop_id": "102N", "stop_name": "Second Station", "stop_lat": 40.72, "stop_lon": -73.92},
             {"stop_id": "103N", "stop_name": "Old Station", "stop_lat": 40.73, "stop_lon": -73.93},
             {"stop_id": "104N", "stop_name": "Quiet Station", "stop_lat": 40.74, "stop_lon": -73.94},
             {"stop_id": "105N", "stop_name": "Quiet Terminal", "stop_lat": 40.75, "stop_lon": -73.95}]

    def serve(route):
        request = route.request
        url = urlsplit(request.url)
        path = url.path.split("/bff/api/v1/", 1)[1]
        params = parse_qs(url.query)
        state["calls"].append((request.method, path, params))
        if path == "status" and state["hold_status"]:
            state["held_status"].append(route)
            return
        if path in state["fail"]:
            route.fulfill(status=503, json={"detail": "Synthetic test: API unavailable"})
            return
        meta = {"mode": state["mode"], "as_of": instant, "freshness_seconds": 2, "stale": False,
                "stale_after_seconds": 120, "time_basis": "predicted"}
        if path == "status":
            data = {**meta, "city": "nyc", "routes": ["1", "2"]}
        elif path == "routes":
            data = routes
        elif path == "stops":
            data = stops
        elif path == "network/health":
            data = state.get("health") or {"health_score": 80, "anomaly_count": 1, "routes": routes, **health_clock}
        elif path == "alerts":
            data = [{"route_ids": ["1"], "header": "Synthetic service notice", "description": "Testing a delay notice.",
                     "start_time": instant, "end_time": None}]
        elif path == "anomalies":
            data = [{"route_id": "2", "stop_id": "102N", "delay_mean_seconds": 180, "health_level": "Watch"}]
        elif path.startswith("routes/") and path.endswith("/coverage"):
            route_id = path.split("/")[1]
            platforms = []
            for index, stop in enumerate(stops):
                status = "fresh" if index < 2 else "stale" if index == 2 else "no_reading"
                fresh = status == "fresh"
                last = instant if fresh else "2026-11-18T06:30:00Z" if status == "stale" else None
                platforms.append({**stop, "route_id": route_id, "parent_station": stop["stop_id"][:-1],
                    "direction_id": 0, "reading_status": status, "last_reading_at": last,
                    "source_as_of": last, "window_start": last, "freshness_seconds": 2 if fresh else 86400 if last else None,
                    "delay_mean_seconds": 60 if fresh and not state["unmatched_schedule"] else None,
                    "headway_seconds": 180 if fresh else None,
                    "scheduled_headway_seconds": 120 if fresh and not state["unmatched_schedule"] else None,
                    "health_score": 80 if fresh and not state["unmatched_schedule"] else None,
                    "health_level": "good" if fresh and not state["unmatched_schedule"] else "unknown",
                    "time_basis": "observed" if index == 0 else "predicted" if last else "unavailable"})
            platforms.sort(key=lambda item: (item["stop_name"], item["stop_id"]))
            data = {"route_id": route_id, "catalogue_as_of": instant, "stops": platforms, "counts": {"catalogue": 5, "fresh": 2, "stale": 1,
                    "no_reading": 2, "exact_delay_available": 0 if state["unmatched_schedule"] else 2},
                    "returned": 5, "limit": 2000, "truncated": False, "stale_after_seconds": 120}
        elif path.startswith("routes/") and path.endswith("/metrics"):
            route_id = path.split("/")[1]
            selected = params.get("stop_id", [""])[0]
            data = [{"route_id": route_id, "stop_id": selected or "101N", "direction_id": 0,
                     "window_start": instant, "delay_mean_seconds": 60, "headway_seconds": 180,
                     "scheduled_headway_seconds": 120, "health_score": 80, "health_level": "Good",
                     "time_basis": "predicted", "event_count": 3},
                    {"route_id": route_id, "stop_id": selected or "101N", "direction_id": 0,
                     "window_start": "2026-11-19T06:31:00Z", "delay_mean_seconds": 80,
                     "headway_seconds": 200, "scheduled_headway_seconds": 120, "health_score": 75,
                     "health_level": "Watch", "time_basis": "predicted", "event_count": 4}]
        elif path == "pipeline/status":
            data = {"engine": "postgresql", "counts": {"events": 100, "metrics": 20, "quarantine": 2, "batches": 4},
                    "streaming": {"input_rows_per_second": 3.5, "processed_rows_per_second": 6.1,
                                  "kafka_lag": 2, "batch_duration_ms": 700, "last_progress_at": instant,
                                  "query_active": True},
                    "sources": {"trip_updates": {"status": "ok", "last_success": instant}}}
        elif path == "what-if" and request.method == "POST":
            body = json.loads(request.post_data)
            data = {"origin": {"route_id": body["route_id"], "stop_id": body["stop_id"]},
                    "horizon_minutes": body["horizon_minutes"], "delay_seconds": body["delay_seconds"], "affected_stations": [
                        {"route_id": body["route_id"], "stop_id": "102N", "travel_seconds": 120,
                         "estimated_delay_seconds": body["delay_seconds"],
                         "path": [f'{body["route_id"]}|0|{body["stop_id"]}', f'{body["route_id"]}|0|102N']}],
                    "limitations": ["Synthetic graph result for the browser test."]}
        else:
            route.fulfill(status=404, json={"detail": "No synthetic fixture for this endpoint"})
            return
        if path.startswith("routes/") and path.endswith("/metrics") and params.get("stop_id", [""])[0] in {"104N", "105N"}:
            data = []
        if state["unmatched_schedule"] and path.startswith("routes/") and path.endswith("/metrics"):
            for row in data:
                row.update(delay_mean_seconds=None, scheduled_headway_seconds=None,
                           health_score=None, health_level="unknown")
        route.fulfill(json={"data": data, "meta": meta})

    def release_status():
        state["hold_status"] = False
        held, state["held_status"] = state["held_status"], []
        for request in held:
            serve(request)

    state["release_status"] = release_status
    page.route("**/bff/api/v1/**", serve)
    return state
