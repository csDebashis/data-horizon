"""Join a route's complete stop catalogue to its latest available readings."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import math

from .contracts import timestamp

METRICS = (
    'delay_mean_seconds', 'delay_stddev_seconds', 'headway_seconds',
    'scheduled_headway_seconds', 'headway_deviation', 'health_score', 'event_count',
)


def _instant(value):
    try:
        return timestamp(value).astimezone(timezone.utc) if value is not None else None
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def _number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None


def _rank(reading):
    """Resolve equal snapshot times without depending on database return order."""
    oldest = datetime.min.replace(tzinfo=timezone.utc)
    snapshot = _instant(reading.get('window_start') or reading.get('as_of'))
    source = _instant(reading.get('source_as_of') or reading.get('as_of') or reading.get('window_start'))
    tie = json.dumps([reading.get(field) for field in (*METRICS, 'time_basis', 'health_level', 'anomaly')],
                     default=str, sort_keys=True)
    return snapshot or source or oldest, source or oldest, str(reading.get('direction_id', '')), tie


def route_coverage(route_id, catalogue, readings, *, now, stale_after_seconds, limit=2000, static=None):
    """Keep one row per catalogue platform, including platforms with no readings.

    This reads the latest-entity result, never the bounded historical query.
    Stale values are useful as coverage evidence but are not current metrics.
    """
    platforms = {}
    for stop in sorted(catalogue, key=lambda row: (str(row.get('stop_id', '')), str(row.get('stop_name', '')))):
        if stop.get('stop_id') is not None:
            platforms.setdefault(str(stop['stop_id']), stop)
    latest = {}
    for reading in readings:
        stop_id = str(reading.get('stop_id', ''))
        if str(reading.get('route_id', '')) != route_id or stop_id not in platforms:
            continue
        if stop_id not in latest or _rank(reading) > _rank(latest[stop_id]):
            latest[stop_id] = reading
    counts = {'catalogue': len(platforms), 'fresh': 0, 'stale': 0, 'no_reading': 0, 'exact_delay_available': 0}
    rows = []
    ordered = sorted(platforms.items(), key=lambda item: (str(item[1].get('stop_name') or item[0]).casefold(), item[0]))
    for stop_id, stop in ordered:
        reading = latest.get(stop_id)
        source = _instant((reading.get('source_as_of') or reading.get('as_of') or reading.get('window_start'))
                          if reading else None)
        age = (now - source).total_seconds() if source else None
        fresh = reading is not None and age is not None and 0 <= age <= stale_after_seconds
        status = 'no_reading' if reading is None else 'fresh' if fresh else 'stale'
        counts[status] += 1
        values = {field: _number(reading.get(field)) if fresh else None for field in METRICS}
        if fresh and values['delay_mean_seconds'] is not None:
            counts['exact_delay_available'] += 1
        window = _instant(reading.get('window_start')) if reading else None
        basis = reading.get('time_basis') if reading else 'unavailable'
        rows.append({
            'route_id': route_id, 'stop_id': stop_id, 'stop_name': stop.get('stop_name') or stop_id,
            'parent_station': stop.get('parent_station'),
            'direction_id': reading.get('direction_id') if reading else None,
            'reading_status': status,
            'last_reading_at': source.isoformat() if source else None,
            'source_as_of': source.isoformat() if source else None,
            'freshness_seconds': age if age is not None and age >= 0 else None,
            'window_start': window.isoformat() if window else None,
            'time_basis': basis if basis in {'observed', 'predicted', 'mixed', 'synthetic'} else 'unavailable',
            **values,
            'health_level': (reading.get('health_level') or 'unknown') if values['health_score'] is not None else 'unknown',
            'anomaly': reading.get('anomaly') if fresh else None,
        })
    static = static if isinstance(static, dict) else {}
    catalogue_time = _instant(static.get('retrieved_at'))
    version = static.get('static_version')
    return {
        'route_id': route_id, 'stops': rows[:limit], 'counts': counts,
        'returned': min(len(rows), limit), 'limit': limit, 'truncated': len(rows) > limit,
        'stale_after_seconds': stale_after_seconds,
        'catalogue_as_of': catalogue_time.isoformat() if catalogue_time else None,
        'catalogue_version': version if isinstance(version, str) and len(version) <= 128 else None,
    }
