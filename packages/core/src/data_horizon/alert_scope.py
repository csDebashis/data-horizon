"""Shared bounded GTFS selector matching and source-specific alert availability."""

from __future__ import annotations

from .contracts import timestamp, utcnow

MAX_SELECTORS = 200
ALERTS_LAST_KNOWN = "Current service-alert information is unavailable. Retained notices are last-known."
ALERTS_EXPIRED = "Service-alert information is stale. Retained notices are last-known."


def _equal(value, expected):
    return value is not None and value != "" and str(value) == str(expected)


def _selector_matches(selector, context, allow_partial):
    if not isinstance(selector, dict) or not selector:
        return False
    supported = {"route_id", "stop_id", "agency_id", "route_type", "direction_id", "trip"}
    if set(selector) - supported:
        return False
    for name, expected in selector.items():
        if name == "trip":
            if not isinstance(expected, dict) or not expected:
                return False
            fields = {
                "trip_id": "trip_id",
                "route_id": "route_id",
                "direction_id": "direction_id",
                "start_date": "service_date",
                "start_time": "trip_start_time",
                "schedule_relationship": "schedule_relationship",
            }
            if set(expected) - set(fields):
                return False
            for field, value in expected.items():
                actual = context.get(fields[field])
                if field == "start_date" and actual:
                    actual = str(actual).replace("-", "")
                if not _equal(actual, value):
                    return False
        elif allow_partial and name in {"route_id", "stop_id"} and not context.get(name):
            # A route-wide list includes scoped notices without claiming every
            # station is affected. Entity scoring always uses strict matching.
            if expected is None or expected == "":
                return False
        elif not _equal(context.get(name), expected):
            return False
    return True


def alert_matches(alert, context=None, *, allow_partial=False):
    context = context or {}
    if "informed_entities" in alert:
        selectors = alert["informed_entities"]
        return (
            isinstance(selectors, list)
            and 0 < len(selectors) <= MAX_SELECTORS
            and any(_selector_matches(item, context, allow_partial) for item in selectors)
        )
    # Existing recorded/fixture objects retain route AND station semantics.
    for field, key in (("route_ids", "route_id"), ("stop_ids", "stop_id")):
        values = alert.get(field)
        if field == "route_ids" and not values and alert.get("route_id"):
            values = [alert["route_id"]]
        if values:
            if not isinstance(values, (list, tuple)):
                return False
            actual = context.get(key)
            if not actual and allow_partial:
                continue
            if actual not in values:
                return False
    return True


def entity_alert_contexts(events, *, route_context=None):
    """Caller supplies current canonical trip rows, plus optional static route facts."""
    return [{**(route_context or {}).get(event["route_id"], {}), **event} for event in events]


def selection_contexts(routes, stops, *, route_id=None, stop_id=None, observations=()):
    """Use explicit catalogue membership, never guessed station suffixes."""
    selected = [row for row in routes if route_id is None or row["route_id"] == route_id]
    contexts = []
    for route in selected:
        if stop_id is None:
            contexts.append(dict(route))
        for stop in stops:
            membership = stop.get("route_ids", [])
            if (stop_id is None or stop["stop_id"] == stop_id) and route["route_id"] in membership:
                contexts.append({**route, "stop_id": stop["stop_id"]})
        for row in observations:
            if row.get("route_id") == route["route_id"] and (
                stop_id is None or row.get("stop_id") == stop_id
            ):
                contexts.append({**route, **row})
    return contexts


def fresh_observations(rows, now, stale_after_seconds):
    result = []
    for row in rows:
        source = row.get("source_as_of") or row.get("event_time") or row.get("as_of")
        try:
            age = (now - timestamp(source)).total_seconds() if source else None
        except (ValueError, TypeError, AttributeError, OverflowError):
            continue
        if row.get("stale") is not True and age is not None and 0 <= age <= stale_after_seconds:
            result.append(row)
    return result


def latest_entity_contexts(events, now, *, route_context=None):
    from datetime import timedelta

    latest = {}
    for event in events:
        instant = timestamp(event["event_time"])
        if not now - timedelta(minutes=30) < instant <= now:
            continue
        key = (event["route_id"], str(event.get("direction_id", "0")), event["stop_id"], event["trip_id"])
        rank = (instant, event.get("event_id", ""))
        if key not in latest or rank > latest[key][0]:
            latest[key] = (rank, event)
    grouped = {}
    for key, (_, event) in latest.items():
        if not event.get("cancelled"):
            grouped.setdefault(key[:3], []).extend(
                entity_alert_contexts([event], route_context=route_context)
            )
    return grouped


def alert_source_status(mode, sources, *, stale_after_seconds, fixture_as_of=None, now=None):
    now = timestamp(now) if now is not None else utcnow()
    source = (sources or {}).get("alerts", {}) if isinstance(sources, dict) else {}
    source = source if isinstance(source, dict) else {}
    as_of = source.get("last_success")
    # A fixture's explicit decision time is useful context, never a live-source receipt.
    if not source and mode in {"synthetic", "recorded"}:
        as_of = fixture_as_of
    age = None
    try:
        instant = timestamp(as_of) if as_of else None
        age = (now - instant).total_seconds() if instant is not None else None
        as_of = instant.isoformat() if instant is not None else None
    except (ValueError, TypeError, AttributeError, OverflowError):
        as_of = None
    invalid = age is None or age < 0
    failed = source.get("status") in {"degraded", "error", "unavailable"}
    stale = invalid or failed or age > stale_after_seconds
    return {
        "as_of": as_of,
        "freshness_seconds": age if not invalid else None,
        "stale": stale,
        "stale_after_seconds": stale_after_seconds,
        "reason": ALERTS_LAST_KNOWN if invalid or failed else ALERTS_EXPIRED if stale else None,
    }


def retained_alert(alert, status):
    return {**alert, "stale": True, "last_known": True} if status["stale"] else dict(alert)
