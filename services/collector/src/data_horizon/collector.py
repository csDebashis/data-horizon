"""Bounded official-source collection, immutable archives and reproducible replay."""

from __future__ import annotations

import csv
import hashlib
import fcntl
import math
import io
import json
import os
import random
import re
import shutil
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from collections import defaultdict
from datetime import timedelta
from pathlib import Path, PurePosixPath
from statistics import median
from zoneinfo import ZoneInfo

from .config import Settings, trusted_url
from .contracts import TransitEvent, timestamp, utcnow
from .graph import canonical_edges
from .schedule import (
    LOOKUP_SCHEMA_VERSION, ScheduleMatcher, lookup_artifacts, parse_calendar, source_profile, trip_direction,
)
from .store import Store, dumps


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("Source redirects are not permitted")


def fetch(url, *, attempts=3, timeout=15, max_bytes=32 * 1024 * 1024, opener=None, sleep=time.sleep):
    trusted_url(url)
    opener = opener or urllib.request.build_opener(NoRedirect()).open
    request = urllib.request.Request(
        url, headers={"User-Agent": "DataHorizon/0.1 (transit analytics)", "Accept": "*/*"}
    )
    for attempt in range(attempts):
        try:
            with opener(request, timeout=timeout) as response:
                body = response.read(max_bytes + 1)
                if len(body) > max_bytes:
                    raise ValueError("Source response exceeds byte limit")
                return body
        except (TimeoutError, urllib.error.URLError, OSError) as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code not in {429, 500, 502, 503, 504}:
                raise
            if attempt + 1 == attempts:
                raise
            sleep(min(2**attempt, 8))
    raise RuntimeError("No collection attempts configured")


def atomic_write(path: Path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temp.open("xb") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def same_file_content(path, content):
    """Compare an immutable artifact without copying its full saved contents."""
    view = memoryview(content)
    with path.open("rb") as stream:
        for start in range(0, len(view), 1024 * 1024):
            if stream.read(1024 * 1024) != view[start:start + 1024 * 1024]:
                return False
        return stream.read(1) == b""


def archive_response(settings, name, content, *, source_url=None):
    if not re.fullmatch(r"[a-z0-9_-]{1,40}", name):
        raise ValueError("Invalid source name")
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    minimum = float(os.getenv("MIN_FREE_GB", "2")) * 1024**3
    maximum = float(os.getenv("MAX_RAW_GB", "15")) * 1024**3
    if not math.isfinite(minimum) or minimum < 0 or not math.isfinite(maximum) or maximum <= 0:
        raise ValueError("Archive limits must be finite; MIN_FREE_GB>=0 and MAX_RAW_GB>0")
    if shutil.disk_usage(settings.data_dir).free < minimum + len(content):
        raise OSError("DATA_DISK_LOW: archive publication paused; existing data preserved")
    digest = hashlib.sha256(content).hexdigest()
    folder = settings.data_dir / "raw" / settings.mode / name / utcnow().date().isoformat()
    path = folder / (digest + ".bin")
    manifest = {
        "source": name,
        "source_url": source_url,
        "retrieved_at": utcnow().isoformat(),
        "sha256": digest,
        "size_bytes": len(content),
        "path": str(path),
        "mode": settings.mode,
        "status": "archived",
    }
    record = folder / (digest + ".manifest.json")
    encoded = dumps(manifest).encode()
    with (settings.data_dir / ".archive.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        raw = settings.data_dir / "raw"
        usage = sum(p.stat().st_size for p in raw.rglob("*") if p.is_file()) if raw.exists() else 0
        addition = (
            (0 if path.exists() else len(content))
            + len(encoded)
            - (record.stat().st_size if record.exists() else 0)
        )
        if usage + addition > maximum:
            raise OSError("RAW_ARCHIVE_LIMIT: export/approved retention required; existing data preserved")
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError("Existing archive checksum mismatch; restore the original object")
        else:
            atomic_write(path, content)
        atomic_write(record, encoded)
    return manifest


def validate_gtfs(content):
    required = {"routes.txt", "stops.txt", "trips.txt", "stop_times.txt"}
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
        total = 0
        for member in z.infolist():
            path = PurePosixPath(member.filename)
            if path.is_absolute() or ".." in path.parts or "\\" in member.filename:
                raise ValueError("Unsafe ZIP member path")
            total += member.file_size
            if total > 512 * 1024 * 1024:
                raise ValueError("GTFS expanded size exceeds limit")
        names = set(z.namelist())
        missing = required - names
        if missing:
            raise ValueError("Missing GTFS files: " + ",".join(sorted(missing)))
        if not {"calendar.txt", "calendar_dates.txt"} & names:
            raise ValueError("Missing GTFS calendar.txt or calendar_dates.txt")
        if z.testzip():
            raise ValueError("Corrupt GTFS member")
        return z
    except zipfile.BadZipFile as exc:
        raise ValueError("Corrupt GTFS ZIP") from exc


def csv_rows(z, name):
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig", newline=""))


def ingest_static(content, store, settings=None):
    settings = settings or store.settings
    manifest = archive_response(settings, "gtfs-static", content, source_url=settings.static_url)
    z = validate_gtfs(content)
    version = manifest["sha256"]
    routes = [r for r in csv_rows(z, "routes.txt") if r["route_id"] in settings.routes]
    trips = {}
    for row in csv_rows(z, "trips.txt"):
        if row["route_id"] in settings.routes:
            if row["trip_id"] in trips:
                raise ValueError("Duplicate GTFS trip ID")
            trips[row["trip_id"]] = row
    calendar = parse_calendar(z, csv_rows, {trip.get("service_id") for trip in trips.values()})
    all_stops = {r["stop_id"]: r for r in csv_rows(z, "stops.txt")}
    stop_routes = defaultdict(set)
    trip_stops = defaultdict(list)
    # Keep compact tuples rather than one full dict per schedule row.
    for row in csv_rows(z, "stop_times.txt"):
        trip = trips.get(row["trip_id"])
        if not trip:
            continue
        stop = row["stop_id"]
        stop_routes[stop].add(trip["route_id"])
        trip_stops[row["trip_id"]].append(
            (int(row["stop_sequence"]), stop, row.get("arrival_time") or row.get("departure_time"))
        )
    edges = defaultdict(list)
    stop_nodes = defaultdict(set)
    for trip_id, rows in trip_stops.items():
        trip = trips[trip_id]
        route = trip["route_id"]
        direction = str(trip.get("direction_id", "0"))
        rows.sort()
        for _, stop, _ in rows:
            stop_nodes[stop].add(f"{route}|{direction}|{stop}")
        for a, b in zip(rows, rows[1:]):

            def seconds(v):
                h, m, s = map(int, v.split(":"))
                return h * 3600 + m * 60 + s

            if not a[2] or not b[2]:
                continue
            cost = seconds(b[2]) - seconds(a[2])
            if 0 <= cost <= 7200:
                edges[(f"{route}|{direction}|{a[1]}", f"{route}|{direction}|{b[1]}", "travel")].append(cost)
    if "transfers.txt" in z.namelist():
        for transfer in csv_rows(z, "transfers.txt"):
            if transfer.get("transfer_type") == "3":
                continue
            a = transfer["from_stop_id"]
            b = transfer["to_stop_id"]
            from_stops = [s for s in stop_nodes if s == a or all_stops[s].get("parent_station") == a]
            to_stops = [s for s in stop_nodes if s == b or all_stops[s].get("parent_station") == b]
            cost = max(0, float(transfer.get("min_transfer_time") or 180))
            for start in from_stops:
                for end in to_stops:
                    for source in stop_nodes[start]:
                        for target in stop_nodes[end]:
                            if source != target:
                                edges[(source, target, "transfer")].append(cost)
    result_edges = canonical_edges(
        [
            {
                "source": a,
                "target": b,
                "edge_type": kind,
                "travel_seconds": median(costs),
                "static_version": version,
                "cost_basis": "GTFS median" if kind == "travel" else "declared transfer (180s default)",
            }
            for (a, b, kind), costs in edges.items()
        ]
    )
    stops = []
    for stop, route_ids in sorted(stop_routes.items()):
        row = all_stops[stop]
        lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
        if not -90 <= lat <= 90 or not -180 <= lon <= 180:
            raise ValueError("GTFS stop coordinate out of range")
        stops.append({**row, "stop_lat": lat, "stop_lon": lon, "route_ids": sorted(route_ids)})
    # Both immutable lookup files bind the same archive, source profile and contents.
    records = [
        {"trip_id": trip, "route_id": trips[trip]["route_id"],
         "direction_id": trips[trip].get("direction_id", "0"),
         "service_id": trips[trip].get("service_id"), "stops": rows}
        for trip, rows in sorted(trip_stops.items())
    ]
    stem, schedule_body, calendar_body, calendar_sha = lookup_artifacts(
        version, settings.static_url, records, calendar,
    )
    schedule_path = settings.data_dir / "static" / (stem + ".schedule-v2.jsonl")
    calendar_path = settings.data_dir / "static" / (stem + ".calendar-v2.json")
    for path, body in ((calendar_path, calendar_body), (schedule_path, schedule_body)):
        if path.exists():
            if not same_file_content(path, body):
                raise ValueError("Existing immutable timetable lookup differs")
        else:
            atomic_write(path, body)
    schedule_sha = hashlib.sha256(schedule_body).hexdigest()
    feed_validity = calendar["feed_validity"]
    # Keep only the small published assets before validating another matcher.
    # The caller retains its current matcher until the replacement succeeds.
    z.close()
    del z, content, records, trip_stops, trips, all_stops, edges, stop_nodes, stop_routes
    del schedule_body, calendar_body, body, calendar
    # A failure leaves the old graph and metadata active; no reader adopts half a lookup.
    ScheduleMatcher(schedule_path)
    report = {
        **manifest,
        "static_version": version,
        "route_count": len(routes),
        "stop_count": len(stops),
        "edge_count": len(result_edges),
        "schedule_path": str(schedule_path),
        "calendar_path": str(calendar_path),
        "lookup_schema_version": LOOKUP_SCHEMA_VERSION,
        "source_profile": source_profile(settings.static_url),
        "schedule_sha256": schedule_sha,
        "calendar_sha256": calendar_sha,
        "feed_validity": feed_validity,
    }
    store.replace_static(routes, stops, result_edges, metadata=report)
    return report


def static_refresh_intervals():
    """Validate operator-controlled intervals without echoing setting values."""
    values = []
    for name, default in (("STATIC_REFRESH_SECONDS", "3600"), ("STATIC_RETRY_SECONDS", "300")):
        try:
            value = float(os.getenv(name, default))
        except (TypeError, ValueError):
            raise ValueError(f"{name} must be finite seconds between 60 and 604800") from None
        if not math.isfinite(value) or not 60 <= value <= 604800:
            raise ValueError(f"{name} must be finite seconds between 60 and 604800")
        values.append(value)
    return tuple(values)


def refresh_static(store, settings, matcher=None, *, now=None, producer=None):
    """Refresh an expired live schedule, retaining the last usable lookup on failure."""
    refresh_seconds, retry_seconds = static_refresh_intervals()
    now = timestamp(now) if now is not None else utcnow()
    static = store.get_metadata("static", {}) or {}
    sources = store.get_metadata("sources", {}) or {}
    previous = sources.get("static", {})
    schedule_path = static.get("schedule_path")
    cached = bool(schedule_path and Path(schedule_path).is_file())
    if cached and matcher is None:
        try:
            matcher = ScheduleMatcher(schedule_path)
        except (OSError, ValueError, KeyError, TypeError):
            cached = False

    def age(value):
        try:
            return (now - timestamp(value)).total_seconds() if value else None
        except (ValueError, TypeError, AttributeError, OverflowError):
            return None

    cache_age = age(static.get("retrieved_at"))
    current_source = (
        static.get("source_url") == settings.static_url
        and static.get("lookup_schema_version") == LOOKUP_SCHEMA_VERSION
        and static.get("source_profile") == source_profile(settings.static_url)
    )
    if cached and current_source and cache_age is not None and 0 <= cache_age < refresh_seconds:
        if not previous:
            sources["static"] = {
                "status": "ok",
                "last_success": static["retrieved_at"],
                "source_url": settings.static_url,
                "mode": settings.mode,
                "static_version": static.get("static_version"),
            }
            store.put_metadata("sources", sources)
        return matcher
    attempt_age = age(previous.get("last_attempt"))
    same_request = (
        previous.get("requested_source_url", previous.get("source_url")) == settings.static_url
        and previous.get("requested_lookup_schema_version") == LOOKUP_SCHEMA_VERSION
    )
    if same_request and previous.get("status") == "degraded" and attempt_age is not None and 0 <= attempt_age < retry_seconds:
        if cached:
            return matcher
        raise RuntimeError("Static schedule unavailable; refresh retry is pending")
    attempted = now.isoformat()
    status = {
        **previous,
        "last_attempt": attempted,
        "source_url": settings.static_url,
        "requested_source_url": settings.static_url,
        "requested_lookup_schema_version": LOOKUP_SCHEMA_VERSION,
        "mode": settings.mode,
    }
    if static.get("retrieved_at"):
        status.setdefault("last_success", static["retrieved_at"])
    try:
        report = ingest_static(fetch(settings.static_url, max_bytes=64 * 1024 * 1024), store, settings)
        replacement = ScheduleMatcher(report["schedule_path"])
    except (OSError, ValueError, KeyError, TypeError) as exc:
        sources["static"] = {
            **status,
            "status": "degraded",
            "last_failure": attempted,
            "reason": type(exc).__name__,
            "using_cached_schedule": cached,
            "source_url": static.get("source_url") if cached else settings.static_url,
            "source_profile": static.get("source_profile") if cached else None,
            "static_version": static.get("static_version"),
        }
        store.put_metadata("sources", sources)
        if not cached:
            raise
        return matcher
    sources["static"] = {
        **status,
        "status": "ok",
        "last_success": report["retrieved_at"],
        "source_profile": report["source_profile"],
        "static_version": report["static_version"],
        "using_cached_schedule": False,
    }
    for key in ("reason", "last_failure"):
        sources["static"].pop(key, None)
    store.put_metadata("sources", sources)
    if report["static_version"] != static.get("static_version"):
        descriptor = {key: report[key] for key in (
            "static_version", "sha256", "route_count", "stop_count", "edge_count"
        )}
        publish_mirror(store, settings, "static-snapshots", descriptor, source="mta-gtfs-static",
                       retrieved_at=report["retrieved_at"], producer=producer)
    return replacement


def decode_feed(content, settings, *, matcher=None, static_version=None, source_url=None):
    from google.transit import gtfs_realtime_pb2
    from google.protobuf.message import DecodeError
    from .alert_scope import MAX_SELECTORS
    from .mta_alerts import alert_selector, official_subway_alerts_source

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        feed.ParseFromString(content)
    except DecodeError as exc:
        raise ValueError("Invalid GTFS-Realtime protobuf response") from exc
    if not feed.IsInitialized():
        raise ValueError("Incomplete GTFS-Realtime header")
    feed_time = timestamp(feed.header.timestamp) if feed.header.timestamp else None
    event_time = feed_time or utcnow()
    events = []
    alerts = []
    rejected = 0
    alerts_rejected = 0
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            update = entity.trip_update
            trip = update.trip
            if trip.route_id not in settings.routes:
                continue
            local = event_time.astimezone(ZoneInfo(settings.timezone))
            service_date = (
                f"{trip.start_date[:4]}-{trip.start_date[4:6]}-{trip.start_date[6:]}"
                if trip.start_date
                else local.date().isoformat()
            )
            source_service_date = service_date if trip.start_date else None
            reference_time = feed_time
            if update.timestamp and feed_time is not None:
                reference_time = timestamp(update.timestamp)
                source_age = (feed_time - reference_time).total_seconds()
                if not -30 <= source_age <= 6 * 60 * 60:
                    # A contradictory trip clock cannot authorise a date shift.
                    reference_time = None
            if matcher:
                effective_day = matcher.effective_service_date(
                    trip.trip_id, source_service_date, reference_time=reference_time,
                )
                if effective_day is not None:
                    service_date = effective_day
            if trip.schedule_relationship == 3 and not update.stop_time_update:
                events.append(
                    TransitEvent(
                        source="mta-subway",
                        route_id=trip.route_id,
                        direction_id=str(trip.direction_id) if trip.HasField("direction_id") else "0",
                        trip_id=trip.trip_id,
                        stop_id="__trip__",
                        event_time=timestamp(update.timestamp) if update.timestamp else event_time,
                        time_basis="unavailable",
                        cancelled=True,
                        service_date=service_date,
                        static_version=static_version,
                    ).model_dump(mode="json")
                )
            for stop in update.stop_time_update:
                predicted = stop.arrival.time or stop.departure.time
                if not stop.stop_id or not predicted:
                    rejected += 1
                    continue
                scheduled, confidence = (
                    matcher.match(
                        trip.trip_id, stop.stop_id, source_service_date,
                        route_id=trip.route_id,
                        direction_id=str(trip.direction_id) if trip.HasField("direction_id")
                        else trip_direction(trip.trip_id),
                        remaining_stop_ids=[item.stop_id for item in update.stop_time_update],
                        reference_time=reference_time,
                    )
                    if matcher
                    else (None, "unmatched")
                )
                try:
                    events.append(
                        TransitEvent(
                            source="mta-subway",
                            route_id=trip.route_id,
                            direction_id=str(trip.direction_id)
                            if trip.HasField("direction_id")
                            else ("1" if stop.stop_id.endswith("S") else "0"),
                            trip_id=trip.trip_id,
                            stop_id=stop.stop_id,
                            event_time=timestamp(update.timestamp) if update.timestamp else event_time,
                            predicted_time=timestamp(predicted),
                            scheduled_time=scheduled,
                            time_basis="predicted",
                            static_version=static_version,
                            match_confidence=confidence,
                            service_date=service_date,
                            cancelled=trip.schedule_relationship == 3 or stop.schedule_relationship == 1,
                        ).model_dump(mode="json")
                    )
                except ValueError:
                    rejected += 1
        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            trip = vehicle.trip
            if trip.route_id in settings.routes and trip.trip_id and vehicle.stop_id:
                events.append(
                    TransitEvent(
                        source="mta-vehicle-status",
                        route_id=trip.route_id,
                        direction_id=str(trip.direction_id)
                        if trip.HasField("direction_id")
                        else ("1" if vehicle.stop_id.endswith("S") else "0"),
                        trip_id=trip.trip_id,
                        stop_id=vehicle.stop_id,
                        event_time=timestamp(vehicle.timestamp) if vehicle.timestamp else event_time,
                        time_basis="inferred",
                        latitude=vehicle.position.latitude if vehicle.HasField("position") else None,
                        longitude=vehicle.position.longitude if vehicle.HasField("position") else None,
                        static_version=static_version,
                    ).model_dump(mode="json")
                )
        if entity.HasField("alert"):
            alert = entity.alert
            if not 0 < len(alert.informed_entity) <= MAX_SELECTORS:
                alerts_rejected += 1
                continue
            selectors = []
            selector_metadata = []
            official_source = official_subway_alerts_source(source_url, settings.alerts_url)
            for index, selector in enumerate(alert.informed_entity):
                item, metadata = alert_selector(selector, official_source=official_source)
                if len(dumps(item).encode()) > 16384:
                    item = {"unsupported_restriction": "oversized-selector"}
                selectors.append(item)
                if metadata:
                    selector_metadata.append({"selector_index": index, **metadata})
            routes = sorted({i.route_id for i in alert.informed_entity if i.route_id in settings.routes})
            if not routes and all(item.get("route_id") for item in selectors):
                continue
            periods = list(alert.active_period)
            starts = [p.start for p in periods if p.start]
            ends = [p.end for p in periods if p.end]

            def text(t):
                return t.translation[0].text if t.translation else ""

            alerts.append(
                {
                    "id": entity.id,
                    "route_ids": routes,
                    "informed_entities": selectors,
                    "selector_metadata": selector_metadata,
                    "severity": 1.0 if alert.effect in {1, 2, 3} else 0.5,
                    "description": text(alert.header_text),
                    "details": text(alert.description_text),
                    "start_time": timestamp(min(starts)).isoformat() if starts else None,
                    "end_time": timestamp(max(ends)).isoformat() if ends else None,
                    "effect": int(alert.effect),
                    "severity_basis": "standard-effect heuristic"
                    if alert.effect in {1, 2, 3}
                    else "default heuristic; authoritative effect unavailable",
                    "active_periods": [
                        {
                            "start_time": timestamp(p.start).isoformat() if p.start else None,
                            "end_time": timestamp(p.end).isoformat() if p.end else None,
                        }
                        for p in periods
                    ],
                }
            )
    return (
        events,
        alerts,
        {
            "feed_timestamp": event_time.isoformat(),
            "accepted": len(events),
            "rejected": rejected,
            "alerts_rejected": alerts_rejected,
            "entities": len(feed.entity),
            "matched": sum(e["match_confidence"] == "exact" for e in events),
            "tentative": sum(e["match_confidence"] == "tentative" for e in events),
            "unmatched": sum(e["match_confidence"] in {"unmatched", "ambiguous"} for e in events),
        },
    )


def decode_tracking_snapshot(content, settings, *, retrieved_at=None):
    """Capture full-feed station status without modifying canonical event payloads."""
    from datetime import datetime
    from google.protobuf.message import DecodeError
    from google.protobuf.unknown_fields import UnknownFieldSet
    from google.transit import gtfs_realtime_pb2 as gtfs
    from .vehicle_source import MAX_TRACKED_VEHICLES, SOURCE, public_trip_id, valid_identifier

    if not isinstance(content, bytes) or not content or len(content) > 32 * 1024 * 1024:
        raise ValueError("Invalid or oversized tracking source")
    feed = gtfs.FeedMessage()
    try:
        feed.ParseFromString(content)
    except DecodeError as exc:
        raise ValueError("Invalid GTFS-Realtime tracking response") from exc
    if (not feed.IsInitialized() or len(feed.entity) > 100000
            or feed.header.gtfs_realtime_version not in {"1.0", "2.0"}):
        raise ValueError("Incomplete or oversized tracking feed")
    incrementality_field = feed.header.DESCRIPTOR.fields_by_name["incrementality"].number
    if (feed.header.incrementality != gtfs.FeedHeader.FULL_DATASET
            or any(field.field_number == incrementality_field for field in UnknownFieldSet(feed.header))):
        raise ValueError("Tracking requires a full source snapshot")
    try:
        feed_time = timestamp(feed.header.timestamp).isoformat() if feed.header.HasField("timestamp") else None
        retrieval = timestamp(retrieved_at if retrieved_at is not None else utcnow()).isoformat()
    except (ValueError, TypeError, AttributeError, OverflowError, OSError) as exc:
        raise ValueError("Invalid tracking source clock") from exc
    rows = {}
    quality = {"entities": len(feed.entity), "vehicle_entities": 0, "duplicates": 0,
               "excluded_route": 0, "rejected": 0, "deleted": 0, "cancelled": 0}
    cancelled = set()
    cancelled_relationships = {gtfs.TripDescriptor.CANCELED, getattr(gtfs.TripDescriptor, "DELETED", 7)}
    for entity in feed.entity:
        if entity.HasField("trip_update") and entity.trip_update.trip.schedule_relationship in cancelled_relationships:
            trip = entity.trip_update.trip
            cancelled.add((trip.route_id, trip.start_date, trip.trip_id))
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        quality["vehicle_entities"] += 1
        vehicle, trip = entity.vehicle, entity.vehicle.trip
        if entity.is_deleted:
            quality["deleted"] += 1
            continue
        if trip.route_id not in settings.routes:
            quality["excluded_route"] += 1
            continue
        if trip.schedule_relationship in cancelled_relationships or (trip.route_id, trip.start_date, trip.trip_id) in cancelled:
            quality["cancelled"] += 1
            continue
        try:
            status_field = vehicle.DESCRIPTOR.fields_by_name["current_status"].number
            if any(field.field_number == status_field for field in UnknownFieldSet(vehicle)):
                raise ValueError("Unsupported tracking vehicle status")
            if not re.fullmatch(r"[0-9]{8}", trip.start_date):
                raise ValueError("Explicit vehicle service date required")
            day = datetime.strptime(trip.start_date, "%Y%m%d").date().isoformat()
            identity = public_trip_id(trip.route_id, day, trip.trip_id)
            if not valid_identifier(vehicle.stop_id, 100):
                raise ValueError("Invalid tracking station scope")
            sequence = vehicle.current_stop_sequence if vehicle.HasField("current_stop_sequence") else None
            explicit = vehicle.HasField("current_status")
            status = gtfs.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status) if explicit else "UNSPECIFIED"
            if status not in {"INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO", "UNSPECIFIED"}:
                raise ValueError("Unsupported tracking vehicle status")
            if explicit and sequence is None:
                status = "UNKNOWN"
            direction = str(trip.direction_id) if trip.HasField("direction_id") else None
            if direction not in {None, "0", "1"}:
                raise ValueError("Invalid tracking direction")
            movement = timestamp(vehicle.timestamp).isoformat() if vehicle.HasField("timestamp") else None
            row = {"id": identity, "route_id": trip.route_id, "trip_id": trip.trip_id, "service_date": day,
                   "direction_id": direction, "stop_id": vehicle.stop_id, "current_stop_sequence": sequence,
                   "status": status, "status_explicit": explicit, "last_movement_at": movement}
        except (ValueError, TypeError, AttributeError, OverflowError, OSError):
            quality["rejected"] += 1
            continue
        if identity in rows:
            quality["duplicates"] += 1
            # Duplicate entity ordering cannot change which public state wins.
            if (row["last_movement_at"] or "", dumps(row)) <= (rows[identity]["last_movement_at"] or "", dumps(rows[identity])):
                continue
        rows[identity] = row
    by_route = defaultdict(int)
    for row in rows.values():
        by_route[row["route_id"]] += 1
    ordered = [rows[key] for key in sorted(rows)]
    quality.update(reported_by_route=dict(sorted(by_route.items())),
                   retained=min(len(ordered), MAX_TRACKED_VEHICLES),
                   omitted_by_bound=max(0, len(ordered) - MAX_TRACKED_VEHICLES))
    return {"schema_version": 1, "snapshot_id": hashlib.sha256(content).hexdigest(), "mode": settings.mode,
            "source": SOURCE, "feed_timestamp": feed_time, "retrieved_at": retrieval,
            "collection_status": "ok", "vehicles": ordered[:MAX_TRACKED_VEHICLES], "quality": quality,
            "truncated": len(ordered) > MAX_TRACKED_VEHICLES}


def tracking_collection_failed(store):
    """Retain complete last-good membership and times, with a safe failure state."""
    previous = store.get_metadata("tracking_snapshot")
    if isinstance(previous, dict):
        store.put_metadata("tracking_snapshot", {
            **previous, "collection_status": "degraded",
            "reason": "Train station-status collection is temporarily unavailable.",
        })


def write_events(settings, events, name="events"):
    if not events:
        return None
    import pyarrow as pa
    import pyarrow.parquet as pq

    content = "\n".join(dumps(e) for e in events).encode()
    manifest = archive_response(settings, name, content)
    folder = settings.data_dir / "parquet" / settings.mode / "raw" / utcnow().date().isoformat()
    path = folder / (manifest["sha256"] + ".parquet")
    if not path.exists():
        folder.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        pq.write_table(pa.Table.from_pylist(events), temp, compression="zstd")
        temp.replace(path)
    return manifest


def ensure_event_topics(settings, admin=None):
    """Create the closed five-topic namespace; never depend on auto-creation."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    owned = admin is None
    client = admin or KafkaAdminClient(
        bootstrap_servers=settings.kafka_servers.split(","),
        request_timeout_ms=10000, api_version_auto_timeout_ms=3000,
    )
    try:
        missing = set(settings.event_topics.values()) - set(client.list_topics())
        if missing:
            topics = [NewTopic(name=name, num_partitions=3, replication_factor=1)
                      for name in sorted(missing)]
            try:
                client.create_topics(topics, timeout_ms=10000)
            except TopicAlreadyExistsError:
                # Another startup may have created the same fixed topics.
                if set(settings.event_topics.values()) - set(client.list_topics()):
                    raise
        return settings.event_topics
    finally:
        if owned:
            client.close()


def producer_for(settings):
    if not settings.kafka_servers:
        return None
    from kafka import KafkaProducer
    from .event_topics import canonical_bytes

    ensure_event_topics(settings)
    return KafkaProducer(
        bootstrap_servers=settings.kafka_servers.split(","),
        value_serializer=canonical_bytes,
        key_serializer=lambda x: x.encode(),
        acks="all",
        retries=5,
        max_block_ms=15000,
        request_timeout_ms=10000,
    )


def outbox_identity(settings, event_type, archive_sha256):
    from .event_topics import canonical_bytes
    context = {"mode": settings.mode, "event_type": event_type,
               "topic": settings.topic_for(event_type), "archive_sha256": archive_sha256}
    return hashlib.sha256(canonical_bytes(context)).hexdigest()


def enqueue_archive(settings, event_type, manifest):
    identity = outbox_identity(settings, event_type, manifest["sha256"])
    folder = settings.data_dir / "outbox" / settings.mode
    pending, ack = folder / "pending" / (identity + ".json"), folder / "published" / (identity + ".json")
    if not pending.exists() and not ack.exists():
        entry = {"schema_version": 1, "mode": settings.mode, "event_type": event_type,
                 "topic": settings.topic_for(event_type), "receipt_id": identity,
                 "manifest": manifest, "created_at": manifest["retrieved_at"]}
        atomic_write(pending, dumps(entry).encode())
    return identity


def publish(settings, events, producer=None):
    # Transit keeps its event contract and archive/Parquet path. Other domains
    # cannot enter this topic or the Spark TransitEvent parser.
    events = [TransitEvent.model_validate(event).model_dump(mode="json") for event in events]
    manifest = write_events(settings, events)
    if manifest and (producer or settings.kafka_servers):
        enqueue_archive(settings, "transit-events", manifest)
        if producer:
            drain_outbox(settings, producer, event_types={"transit-events"})
        elif settings.kafka_servers:
            raise ConnectionError("Kafka unavailable; source batch remains in durable outbox")
    return manifest


def publish_auxiliary_snapshot(settings, event_type, payload, *, source, retrieved_at,
                               source_time=None, producer=None):
    """Publish a public-source inspection mirror, preserving complete snapshots."""
    from .event_topics import build_snapshot_events, canonical_bytes

    records = build_snapshot_events(event_type, settings.mode, payload, source=source,
                                    retrieved_at=retrieved_at, source_time=source_time)
    content = b"\n".join(canonical_bytes(record) for record in records)
    manifest = archive_response(settings, "kafka-" + event_type, content)
    if producer or settings.kafka_servers:
        enqueue_archive(settings, event_type, manifest)
        if producer:
            result = drain_outbox(settings, producer, event_types={event_type})
            if result.get("mirror_failures"):
                raise ConnectionError("Auxiliary mirror remains in durable outbox")
        elif settings.kafka_servers:
            raise ConnectionError("Auxiliary mirror remains in durable outbox")
    return manifest


def publish_mirror(store, settings, event_type, payload, *, source, retrieved_at,
                   source_time=None, producer=None):
    """An optional inspection mirror must not fail ordinary source serving."""
    if not (settings.kafka_servers or producer):
        return None
    statuses = store.get_metadata("event_mirrors", {}) or {}
    if not isinstance(statuses, dict):
        statuses = {}
    try:
        manifest = publish_auxiliary_snapshot(settings, event_type, payload, source=source,
                                              retrieved_at=retrieved_at, source_time=source_time,
                                              producer=producer)
    except Exception as exc:
        statuses[event_type] = {**statuses.get(event_type, {}), "status": "degraded",
                                "last_failure": utcnow().isoformat(), "reason": type(exc).__name__}
        result = None
    else:
        statuses[event_type] = {"status": "ok", "last_success": timestamp(retrieved_at).isoformat(),
                                "archive_sha256": manifest["sha256"]}
        result = manifest
    store.put_metadata("event_mirrors", statuses)
    return result


def validated_outbox_records(settings, path, entry):
    from .event_topics import SnapshotEvent, canonical_bytes, reassemble_snapshot_events

    event_type = entry.get("event_type")
    topic = settings.topic_for(event_type)
    manifest = entry["manifest"]
    expected = outbox_identity(settings, event_type, manifest["sha256"])
    if (entry.get("schema_version") != 1 or entry.get("mode") != settings.mode
            or entry.get("topic") != topic or entry.get("receipt_id") != expected
            or path.name != expected + ".json" or manifest.get("mode") != settings.mode):
        raise ValueError("Outbox type/topic/namespace/identity mismatch")
    raw_path = Path(manifest["path"]).resolve()
    if not raw_path.is_relative_to((settings.data_dir / "raw" / settings.mode).resolve()):
        raise ValueError("Invalid outbox archive path")
    if raw_path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("Outbox archive exceeds byte bound")
    content = raw_path.read_bytes()
    if hashlib.sha256(content).hexdigest() != manifest["sha256"]:
        raise ValueError("Outbox archive checksum mismatch")
    records = [json.loads(line) for line in content.splitlines() if line]
    if not records:
        raise ValueError("Empty outbox batch")
    if event_type == "transit-events":
        return [TransitEvent.model_validate(record).model_dump(mode="json") for record in records]
    checked = [SnapshotEvent.model_validate(record).model_dump(mode="json") for record in records]
    if any(record["event_type"] != event_type or record["mode"] != settings.mode for record in checked):
        raise ValueError("Auxiliary record type or namespace mismatch")
    # A partial archived snapshot must never acquire a successful receipt.
    reassemble_snapshot_events(checked)
    if any(len(canonical_bytes(record)) > 256 * 1024 for record in checked):
        raise ValueError("Auxiliary record exceeds byte bound")
    return checked


def drain_outbox(settings, producer, max_batches=10, *, event_types=None):
    from .event_topics import EVENT_TYPES

    allowed = set(EVENT_TYPES) if event_types is None else set(event_types)
    if not allowed <= set(EVENT_TYPES):
        raise ValueError("Unknown outbox event type")
    folder = settings.data_dir / "outbox" / settings.mode / "pending"
    candidates, mirror_failures = [], []
    for path in folder.glob("*.json"):
        try:
            if path.stat().st_size > 64 * 1024:
                raise ValueError("Outbox receipt exceeds byte bound")
            entry = json.loads(path.read_text())
            if not isinstance(entry, dict):
                raise ValueError("Outbox receipt must be an object")
            event_type = entry.get("event_type")
            if event_type not in EVENT_TYPES:
                raise ValueError("Unknown outbox event type")
            if not isinstance(entry.get("created_at"), str):
                raise ValueError("Outbox creation clock must be timezone-aware text")
            created_at = timestamp(entry["created_at"]).timestamp()
            if event_type in allowed:
                candidates.append((event_type != "transit-events", created_at, path.name, path, entry))
        except (ValueError, KeyError, TypeError, OSError) as exc:
            mirror_failures.append({"event_type": "unknown", "reason": type(exc).__name__})
    sent = 0
    # Transit first: corrupt or unavailable optional mirrors cannot starve it.
    for _, _, _, path, entry in sorted(candidates)[:max_batches]:
        event_type = entry["event_type"]
        try:
            records = validated_outbox_records(settings, path, entry)
            pending = []
            for record in records:
                key = (f"{record['route_id']}|{record['direction_id']}|{record['stop_id']}"
                       if event_type == "transit-events" else record["snapshot_id"])
                pending.append(producer.send(settings.topic_for(event_type), key=key, value=record))
            for future in pending:
                future.get(timeout=20 if event_type == "transit-events" else 3)
            producer.flush(timeout=20 if event_type == "transit-events" else 3)
            ack = folder.parent / "published" / path.name
            atomic_write(ack, dumps({**entry, "acknowledged_at": utcnow().isoformat(),
                                     "events": len(records)}).encode())
            path.unlink()
            sent += len(records)
        except Exception as exc:
            if event_type == "transit-events":
                raise
            mirror_failures.append({"event_type": event_type, "reason": type(exc).__name__})
    result = {"published_events": sent, "pending_batches": sum(1 for _ in folder.glob("*.json"))}
    if mirror_failures:
        result["mirror_failures"] = mirror_failures[:10]
    return result


def synthetic_static(store):
    names = [
        ("101N", "Van Cortlandt Park", 40.889, -73.898),
        ("120N", "96 Street", 40.793, -73.972),
        ("127N", "Times Sq–42 Street", 40.755, -73.987),
        ("635N", "14 Street–Union Square", 40.735, -73.990),
        ("142N", "South Ferry", 40.702, -74.013),
    ]
    routes = [
        {
            "route_id": str(r),
            "route_short_name": str(r),
            "route_long_name": f"Subway line {r} (synthetic fixture)",
            "route_color": "E53935" if r < 4 else "00897B",
        }
        for r in range(1, 7)
    ]
    stops = [
        {
            "stop_id": i,
            "stop_name": n,
            "stop_lat": lat,
            "stop_lon": lon,
            "parent_station": i[:-1],
            "route_ids": [str(r) for r in range(1, 7)],
        }
        for i, n, lat, lon in names
    ]
    edges = []
    for route in routes:
        r = route["route_id"]
        for a, b in zip(stops, stops[1:]):
            edges.append(
                {
                    "source": f"{r}|0|{a['stop_id']}",
                    "target": f"{r}|0|{b['stop_id']}",
                    "edge_type": "travel",
                    "travel_seconds": 240,
                    "weight": 1,
                    "static_version": "synthetic-v1",
                }
            )
        if r != "6":
            edges.append(
                {
                    "source": f"{r}|0|127N",
                    "target": f"{int(r) + 1}|0|127N",
                    "edge_type": "transfer",
                    "travel_seconds": 180,
                    "weight": 1,
                    "static_version": "synthetic-v1",
                }
            )
    from .event_topics import canonical_bytes
    checksum = hashlib.sha256(canonical_bytes({"routes": routes, "stops": stops, "edges": edges})).hexdigest()
    previous = store.get_metadata("static", {}) or {}
    activated = (previous.get("retrieved_at") if previous.get("sha256") == checksum else None) or utcnow().isoformat()
    store.replace_static(routes, stops, edges, metadata={
        "source": "synthetic", "time_basis": "synthetic", "static_version": "synthetic-v1", "sha256": checksum,
        "retrieved_at": activated, "route_count": len(routes), "stop_count": len(stops), "edge_count": len(edges),
    })
    return routes, stops, edges


def synthetic_events(now=None, *, seed=42):
    now = timestamp(now) if now else utcnow()
    rng = random.Random(seed)
    events = []
    stop_ids = ["101N", "120N", "127N", "635N", "142N"]
    for r in range(1, 7):
        for stop in stop_ids:
            for i in range(6):
                observed = now - timedelta(minutes=25 - 5 * i)
                delay = (r - 1) * 65 + rng.randrange(0, 60)
                if r == 3 and stop in {"127N", "635N"}:
                    delay += 220
                scheduled = observed - timedelta(seconds=delay)
                events.append(
                    TransitEvent(
                        source="synthetic",
                        route_id=str(r),
                        trip_id=f"fixture-{r}-{i}",
                        stop_id=stop,
                        event_time=observed,
                        observed_time=observed,
                        scheduled_time=scheduled,
                        time_basis="observed",
                        match_confidence="exact",
                        scheduled_headway_seconds=300,
                        static_version="synthetic-v1",
                    ).model_dump(mode="json")
                )
    return events


def collect_once(store, settings, *, producer=None, matcher=None):
    mirrors = []
    alerts_complete = False
    sources = store.get_metadata("sources", {})
    static = store.get_metadata("static", {})
    try:
        raw = fetch(settings.realtime_url)
        manifest = archive_response(settings, "gtfs-realtime", raw, source_url=settings.realtime_url)
        events, alerts, quality = decode_feed(
            raw, settings, matcher=matcher, static_version=static.get("static_version"),
            source_url=settings.realtime_url,
        )
    except (OSError, ValueError) as exc:
        tracking_collection_failed(store)
        sources["transit"] = {
            **sources.get("transit", {}),
            "status": "degraded",
            "last_failure": utcnow().isoformat(),
            "reason": type(exc).__name__,
        }
        store.put_metadata("sources", sources)
        raise
    try:
        tracking = decode_tracking_snapshot(raw, settings, retrieved_at=manifest["retrieved_at"])
    except ValueError:
        # A tracking-only validation error must not change canonical publication.
        tracking_collection_failed(store)
    else:
        store.put_metadata("tracking_snapshot", tracking)
        mirrors.append(("vehicle-status", tracking, {"source": tracking["source"],
                        "retrieved_at": tracking["retrieved_at"], "source_time": tracking["feed_timestamp"]}))
    try:
        alert_raw = fetch(settings.alerts_url, attempts=2)
        alert_manifest = archive_response(settings, "service-alerts", alert_raw, source_url=settings.alerts_url)
        _, extra, alert_quality = decode_feed(alert_raw, settings, source_url=settings.alerts_url)
        alerts.extend(extra)
        alerts_complete = True
        sources["alerts"] = {"status": "ok", "last_success": alert_manifest["retrieved_at"]}
    except (OSError, ValueError) as exc:
        sources["alerts"] = {
            **sources.get("alerts", {}),
            "status": "degraded",
            "reason": type(exc).__name__,
            "last_failure": utcnow().isoformat(),
        }
        known = store.alerts()
        ids = {a["id"] for a in alerts}
        alerts.extend(a for a in known if a["id"] not in ids)
    previous_alerts = {a["id"]: a for a in store.assets("alerts")}
    for alert in alerts:
        previous = previous_alerts.get(alert["id"])

        def comparable(item):
            return {k: v for k, v in item.items() if k != "available_at"}

        alert["available_at"] = (
            previous.get("available_at", utcnow().isoformat())
            if previous and comparable(previous) == comparable(alert)
            else utcnow().isoformat()
        )
    store.replace_assets("alerts", list({a["id"]: a for a in alerts}.values()))
    if alerts_complete:
        mirrors.append(("service-alerts", {"alerts": store.assets("alerts"), "authoritative": True,
                                           "quality": alert_quality},
                        {"source": "mta-service-alerts", "retrieved_at": alert_manifest["retrieved_at"],
                         "source_time": alert_quality["feed_timestamp"]}))
    weather = store.get_metadata("weather", {})
    if (
        not weather.get("retrieved_at")
        or (utcnow() - timestamp(weather["retrieved_at"])).total_seconds() > 600
    ):
        try:
            weather_raw = fetch(settings.weather_url, attempts=2)
            archive_response(settings, "weather", weather_raw, source_url=settings.weather_url)
            p = json.loads(weather_raw)["properties"]
            weather = {
                "station": "KNYC",
                "temperature_c": p.get("temperature", {}).get("value"),
                "observed_at": p.get("timestamp"),
                "retrieved_at": utcnow().isoformat(),
                "time_basis": "observed",
                "units": "degC",
            }
            store.put_metadata("weather", weather)
            sources["weather"] = {"status": "ok", "last_success": weather["retrieved_at"]}
            mirrors.append(("weather-observations", weather,
                            {"source": "noaa-knyc", "retrieved_at": weather["retrieved_at"],
                             "source_time": weather["observed_at"]}))
        except (OSError, ValueError, KeyError) as exc:
            sources["weather"] = {"status": "degraded", "reason": type(exc).__name__}
    sources["transit"] = {
        "status": "ok",
        "last_success": utcnow().isoformat(),
        "manifest": manifest,
        "quality": quality,
    }
    store.put_metadata("sources", sources)
    publish(settings, events, producer)
    # Canonical transit delivery happens first; a bad inspection mirror cannot
    # make successfully collected transit unavailable.
    for event_type, payload, clocks in mirrors:
        publish_mirror(store, settings, event_type, payload, producer=producer, **clocks)
    if not settings.kafka_servers:
        store.apply_batch(events, query_id="direct-live-v1", batch_id=manifest["sha256"])
    store.put_metadata("collector_error", None)
    return {"events": len(events), "alerts": len(alerts), "quality": quality, "manifest": manifest}


def run_collector(once=False):
    settings = Settings.load()
    store = Store()
    producer = None
    synthetic_mirrors = []
    if settings.mode == "synthetic":
        synthetic_static(store)
        fixture_time = utcnow().isoformat()
        store.put_metadata("weather", {"temperature_c": 12, "time_basis": "synthetic", "station": "fixture",
                                       "retrieved_at": fixture_time, "observed_at": None})
        store.replace_assets(
            "alerts",
            [
                {
                    "id": "fixture-alert",
                    "route_ids": ["3"],
                    "severity": 1.0,
                    "description": "Synthetic signal disruption near Times Square",
                    "end_time": (utcnow() + timedelta(days=1)).isoformat(),
                }
            ],
        )
        descriptor = store.get_metadata("static")
        synthetic_mirrors = [
            ("static-snapshots", {key: descriptor[key] for key in (
                "static_version", "sha256", "route_count", "stop_count", "edge_count"
            )}, descriptor["retrieved_at"]),
            ("service-alerts", {"alerts": store.assets("alerts"), "authoritative": True,
                                "time_basis": "synthetic"}, fixture_time),
            # No vehicle positions are invented; the Synthetic fixture has none.
            ("vehicle-status", {"vehicles": [], "time_basis": "synthetic", "collection_status": "ok"}, fixture_time),
            ("weather-observations", store.get_metadata("weather"), fixture_time),
        ]
        matcher = None
    elif settings.mode == "recorded":
        raise ValueError("Use replay --input for recorded mode")
    else:
        static_refresh_intervals()
        matcher = None
    try:
        while True:
            try:
                if settings.kafka_servers and producer is None:
                    try:
                        producer = producer_for(settings)
                    except Exception as exc:
                        store.put_metadata("kafka", {"status": "degraded", "reason": type(exc).__name__})
                if producer:
                    try:
                        pending = drain_outbox(settings, producer)
                        store.put_metadata("kafka", {"status": "ok", **pending})
                    except ValueError:
                        raise  # Corrupt local outbox requires intervention, never skip it.
                    except Exception as exc:
                        store.put_metadata("kafka", {"status": "degraded", "reason": type(exc).__name__})
                        try:
                            producer.close(timeout=1)
                        except Exception:
                            pass
                        producer = None
                if settings.mode == "synthetic":
                    events = synthetic_events()
                    manifest = publish(settings, events, producer)
                    for kind, payload, retrieved in synthetic_mirrors:
                        publish_mirror(store, settings, kind, payload, source="synthetic-fixture",
                                       retrieved_at=retrieved, producer=producer)
                    synthetic_mirrors = []
                    if not settings.kafka_servers:
                        store.apply_batch(events, query_id="direct-synthetic-v1", batch_id=manifest["sha256"])
                    result = {"events": len(events), "mode": "synthetic"}
                else:
                    matcher = refresh_static(store, settings, matcher, producer=producer)
                    result = collect_once(store, settings, producer=producer, matcher=matcher)
                print(dumps(result), flush=True)
            except Exception as exc:
                store.put_metadata(
                    "collector_error", {"type": type(exc).__name__, "at": utcnow().isoformat()}
                )
                print(dumps({"error": type(exc).__name__, "service": "collector"}), flush=True)
                if once:
                    raise
            if once:
                break
            time.sleep(settings.poll_seconds)
    finally:
        if producer:
            producer.close(timeout=10)


def replay(input_path, settings, *, speed=1.0, producer=None, sleep=time.sleep):
    if settings.mode != "recorded":
        raise ValueError("Replay requires isolated DATA_MODE=recorded")
    if not 0 < speed <= 1000:
        raise ValueError("Replay speed must be in (0,1000]")
    path = Path(input_path)
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        events = pq.read_table(path).to_pylist()
    else:
        events = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    events.sort(key=lambda e: e["event_time"])
    run = uuid.uuid4().hex
    previous = None
    store = Store()
    if store.mode != settings.mode:
        raise ValueError("Replay Settings and Store namespaces must agree")
    for index, event in enumerate(events):
        t = timestamp(event["event_time"])
        if previous:
            sleep(max(0, (t - previous).total_seconds() / speed))
        item = TransitEvent.model_validate(
            {**event, "replay_run_id": run, "ingested_at": utcnow()}
        ).model_dump(mode="json")
        publish(settings, [item], producer)
        if not settings.kafka_servers:
            store.apply_batch([item], query_id="replay-" + run, batch_id=str(index))
        previous = t
    return {"run_id": run, "events": len(events), "speed": speed, "mode": "recorded"}


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description="Data Horizon source collector service")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    run_collector(once=args.once)


if __name__ == "__main__":
    main()
