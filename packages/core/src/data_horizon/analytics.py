"""Deterministic reference transformations shared with streaming sinks."""

from __future__ import annotations

from collections import defaultdict
from datetime import timedelta
from statistics import mean, median, pstdev

from .contracts import TransitEvent, timestamp, utcnow
from .alert_scope import alert_matches, entity_alert_contexts


def delay_seconds(event: dict) -> float | None:
    # A guessed timetable match cannot support either actual or forecast delay.
    if (event.get("cancelled") or event.get("match_confidence") != "exact"
            or not event.get("scheduled_time")):
        return None
    basis = event.get("time_basis")
    value = (
        event.get("observed_time")
        if basis == "observed"
        else event.get("predicted_time")
        if basis == "predicted"
        else None
    )
    return (timestamp(value) - timestamp(event["scheduled_time"])).total_seconds() if value else None


def health_score(delay, headway_deviation, alert_severity):
    if any(v is None for v in (delay, headway_deviation, alert_severity)):
        return {
            "health_score": None,
            "health_level": "unknown",
            "reason": "Incomplete required inputs",
            "components": {},
        }
    parts = {
        "delay": 0.6 * min(max(delay, 0) / 600, 1),
        "headway": 0.3 * min(abs(headway_deviation), 1),
        "alerts": 0.1 * min(max(alert_severity, 0), 1),
    }
    score = round(max(0, min(100, 100 * (1 - sum(parts.values())))), 2)
    return {
        "health_score": score,
        "health_level": "good" if score >= 80 else "watch" if score >= 50 else "disrupted",
        "components": parts,
    }


def weather_as_of(weather, now):
    """Availability matters as well as observation time for historical features."""
    weather = weather or {}
    for key in ("observed_at", "retrieved_at", "available_at"):
        if weather.get(key) and timestamp(weather[key]) > now:
            return {}
    return weather


def apply_trip_cancellations(events):
    """Apply native no-stop controls without inventing a station called __trip__."""
    rows = [e.model_dump(mode="json") if isinstance(e, TransitEvent) else dict(e) for e in events]
    controls = {}
    for e in rows:
        if e.get("stop_id") == "__trip__" and e.get("cancelled"):
            key = (e["route_id"], e["trip_id"], e.get("service_date"))
            value = timestamp(e["event_time"])
            if key not in controls or value > controls[key]:
                controls[key] = value
    output = []
    for e in rows:
        if e.get("stop_id") == "__trip__":
            continue
        # Platform/status timestamps are not arrival predictions and must not
        # replace a trip's valid stop-time update in an arrival sequence.
        if e.get("source") == "mta-vehicle-status" and not (
            e.get("observed_time") or e.get("predicted_time")
        ):
            continue
        boundary = controls.get((e["route_id"], e["trip_id"], e.get("service_date")))
        if boundary is not None and timestamp(e["event_time"]) <= boundary:
            e["cancelled"] = True
        output.append(e)
    return output


def latest_event_revisions(events, *, as_of=None):
    """Keep canonical trip-stop revisions before expensive downstream processing."""
    now = timestamp(as_of) if as_of is not None else utcnow()
    earliest = now - timedelta(minutes=30)
    latest = {}
    for event in apply_trip_cancellations(events):
        instant = timestamp(event['event_time'])
        if not earliest < instant <= now:
            continue
        key = (event['route_id'], str(event.get('direction_id', '0')), event['stop_id'], event['trip_id'])
        rank = (instant, event.get('event_id', ''))
        if key not in latest or rank > latest[key][0]:
            latest[key] = (rank, event)
    # Latest cancellations remain present so an unknown snapshot replaces old health.
    return [latest[key][1] for key in sorted(latest)]


def alert_active(alert, now):
    now = timestamp(now)
    if alert.get("available_at") and timestamp(alert["available_at"]) > now:
        return False
    periods = alert.get("active_periods") or [alert]
    return any(
        (not p.get("start_time") or timestamp(p["start_time"]) <= now)
        and (not p.get("end_time") or timestamp(p["end_time"]) > now)
        for p in periods
    )


def aggregate(events: list[dict], *, as_of=None, alerts=None, weather=None, route_context=None) -> list[dict]:
    """One-minute snapshots, rolling (t-window,t] history; no future snapshots."""
    now = timestamp(as_of) if as_of else utcnow()
    groups = defaultdict(list)
    for event in apply_trip_cancellations(events):
        e = event.model_dump(mode="json") if isinstance(event, TransitEvent) else event
        if now - timedelta(minutes=30) < timestamp(e["event_time"]) <= now:
            groups[(e["route_id"], str(e.get("direction_id", "0")), e["stop_id"])].append(e)
    output = []
    for (route, direction, stop), rows in sorted(groups.items()):
        # Only the latest revision of a trip-stop represents its current prediction.
        by_trip = {}
        for e in sorted(rows, key=lambda e: (e["event_time"], e["event_id"])):
            by_trip[e["trip_id"]] = e
        current = [e for e in by_trip.values() if not e.get("cancelled")]
        delays = [d for e in current if (d := delay_seconds(e)) is not None]
        arrivals = sorted(
            timestamp(e.get("observed_time") or e.get("predicted_time"))
            for e in current
            if e.get("observed_time") or e.get("predicted_time")
        )
        gaps = [(b - a).total_seconds() for a, b in zip(arrivals, arrivals[1:]) if b > a]
        # Arrival gaps remain useful even when timetable matching is uncertain.
        # Only exact matches may supply either kind of scheduled comparison.
        matched = [e for e in current if e.get("match_confidence") == "exact"]
        schedules = sorted(timestamp(e["scheduled_time"]) for e in matched if e.get("scheduled_time"))
        scheduled_gaps = [(b - a).total_seconds() for a, b in zip(schedules, schedules[1:]) if b > a]
        explicit = [
            e["scheduled_headway_seconds"] for e in matched if (e.get("scheduled_headway_seconds") or 0) > 0
        ]
        scheduled = median(explicit) if explicit else median(scheduled_gaps) if scheduled_gaps else None
        headway = median(gaps) if gaps else None
        deviation = (headway - scheduled) / scheduled if headway is not None and scheduled else None
        contexts = entity_alert_contexts(current, route_context=route_context) or [
            {
                **(route_context or {}).get(route, {}),
                "route_id": route,
                "stop_id": stop,
                "direction_id": direction,
            }
        ]
        applicable = [
            a
            for a in (alerts or [])
            if alert_active(a, now) and any(alert_matches(a, context) for context in contexts)
        ]
        severity = max((a.get("severity", 0.5) for a in applicable), default=0.0)
        delay = mean(delays) if delays else None
        basis = (
            "observed"
            if current and all(e["time_basis"] == "observed" for e in current)
            else "predicted"
            if any(e["time_basis"] == "predicted" for e in current)
            else "unavailable"
        )
        row = {
            "route_id": route,
            "direction_id": direction,
            "stop_id": stop,
            "window_start": now.replace(second=0, microsecond=0).isoformat(),
            "as_of": now.isoformat(),
            "source_as_of": max(timestamp(e["event_time"]) for e in rows).isoformat(),
            "delay_mean_seconds": delay,
            "delay_stddev_seconds": pstdev(delays) if delays else None,
            "headway_seconds": headway,
            "scheduled_headway_seconds": scheduled,
            "headway_deviation": deviation,
            "alert_count": len(applicable),
            "event_count": len(current),
            "time_basis": basis,
            "weather_temperature_c": weather_as_of(weather, now).get("temperature_c"),
            "feature_version": "v1",
        }
        row.update(health_score(delay, deviation, severity))
        row["anomaly"] = (
            "high_delay"
            if delay is not None and delay >= 300
            else "irregular_headway"
            if deviation is not None and abs(deviation) >= 0.5
            else None
        )
        for minutes in (5, 15, 30):
            # Keep one revision per trip in each history window as well.
            candidates = {}
            for e in sorted(rows, key=lambda x: (x["event_time"], x["event_id"])):
                if now - timedelta(minutes=minutes) < timestamp(e["event_time"]) <= now:
                    candidates[e["trip_id"]] = e
            values = [d for e in candidates.values() if (d := delay_seconds(e)) is not None]
            row[f"rolling_{minutes}m_mean"] = mean(values) if values else None
            row[f"rolling_{minutes}m_stddev"] = pstdev(values) if values else None
        output.append(row)
    return output
