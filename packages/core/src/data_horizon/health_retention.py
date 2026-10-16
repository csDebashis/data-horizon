"""Bounded last-valid health presentation, separate from current calculations."""
from __future__ import annotations

from datetime import datetime
import math
from statistics import mean

from .contracts import timestamp
from .analytics import latest_event_revisions

RETENTION_SECONDS = 300
HISTORY_LIMIT = 10000
FEATURE_VERSION = 'v1'
SCORING_VERSION = 'health-v2'


def instant(value):
    if not isinstance(value, (str, datetime)):
        return None
    try:
        return timestamp(value)
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def entity_key(row):
    values = tuple(row.get(key) for key in ('route_id', 'direction_id', 'stop_id'))
    return values if all(isinstance(value, str) and value for value in values) else None


def eligible_entities(events, now, static_version):
    """Verify the timetable of the same latest uncancelled trip revisions used by health."""
    if not isinstance(static_version, str) or not static_version or len(static_version) > 200:
        return set()
    versions = {}
    for event in latest_event_revisions(events, as_of=now):
        entity = entity_key(event)
        if entity is not None and not event.get('cancelled'):
            versions.setdefault(entity, set()).add(event.get('static_version'))
    return {key for key, values in versions.items() if values == {static_version}}


def scope_for(settings, mode, static, sources, row):
    """Bind a calculated reading to the source and scoring rules active now."""
    static = static if isinstance(static, dict) else {}
    version = static.get('static_version')
    if not isinstance(version, str) or not version or len(version) > 200:
        return None
    basis = row.get('time_basis')
    if basis not in {'predicted', 'observed', 'mixed', 'synthetic'} or row.get('feature_version') != FEATURE_VERSION:
        return None
    transit = sources.get('transit', {}) if isinstance(sources, dict) else {}
    manifest = transit.get('manifest', {}) if isinstance(transit, dict) else {}
    manifest = manifest if isinstance(manifest, dict) else {}
    return {
        'mode': mode, 'static_version': version, 'source_profile': static.get('source_profile'),
        'static_source': static.get('source_url') or settings.static_url,
        'realtime_source': manifest.get('source_url') or settings.realtime_url,
        'configured_static_source': settings.static_url,
        'configured_realtime_source': settings.realtime_url,
        'configured_alerts_source': settings.alerts_url,
        'feature_version': FEATURE_VERSION, 'scoring_version': SCORING_VERSION, 'time_basis': basis,
    }


def level(score):
    return 'good' if score >= 80 else 'watch' if score >= 50 else 'disrupted'


def retained_reading(row, expected_scope, now, fresh_seconds, reason=None):
    """A fetch never renews a reading's original contributing source clock."""
    if expected_scope is None or row.get('health_scope') != expected_scope or entity_key(row) is None:
        return None
    score = row.get('health_score')
    required = ('delay_mean_seconds', 'headway_seconds', 'scheduled_headway_seconds', 'headway_deviation')
    if not finite(score) or not 0 <= score <= 100 or any(not finite(row.get(key)) for key in required):
        return None
    if row['headway_seconds'] <= 0 or row['scheduled_headway_seconds'] <= 0:
        return None
    source, calculated = instant(row.get('source_as_of')), instant(row.get('as_of'))
    if source is None or calculated is None:
        return None
    age, calculation_age = (now - source).total_seconds(), (now - calculated).total_seconds()
    if not (0 <= age <= RETENTION_SECONDS and 0 <= calculation_age <= RETENTION_SECONDS
            and 0 <= (calculated - source).total_seconds() <= fresh_seconds):
        return None
    return {'score': score, 'level': level(score), 'source_as_of': source.isoformat(),
            'age_seconds': age, 'retention_seconds': RETENTION_SECONDS,
            'reason': reason or 'Waiting for current health inputs.',
            'time_basis': expected_scope['time_basis'], 'entity_count': 1}


def current_provenance(rows, now, fresh_seconds):
    """Use the oldest actual contributor, never the newer global feed clock."""
    times = [instant(row.get('source_as_of')) for row in rows]
    valid_times = bool(times) and all(value is not None and value <= now for value in times)
    oldest = min(times) if valid_times else None
    return {'health_source_as_of': oldest.isoformat() if oldest else None,
            'health_age_seconds': (now - oldest).total_seconds() if oldest else None,
            'health_freshness_seconds': fresh_seconds, 'health_retention_seconds': RETENTION_SECONDS,
            'health_retention_eligible': valid_times and all(row.get('health_retention_eligible') is True for row in rows)}


def retained_summary(readings, now, *, reason, route_count=1, route_weighted=False):
    """Callers provide one latest eligible entity or one summary per route."""
    if not readings:
        return None
    times = [instant(reading.get('source_as_of')) for reading in readings]
    if any(value is None or value > now for value in times):
        return None
    oldest = min(times)
    age = (now - oldest).total_seconds()
    if age > RETENTION_SECONDS or any(not finite(reading.get('score')) or not 0 <= reading['score'] <= 100 for reading in readings):
        return None
    score = round(mean(reading['score'] for reading in readings), 2)
    bases = {reading.get('time_basis') for reading in readings}
    return {'score': score, 'level': level(score), 'source_as_of': oldest.isoformat(),
            'age_seconds': age, 'retention_seconds': RETENTION_SECONDS, 'reason': reason,
            'entity_count': sum(reading.get('entity_count', 1) for reading in readings),
            'route_count': route_count, 'time_basis': next(iter(bases)) if len(bases) == 1 else 'mixed',
            'basis': 'Mean of latest valid route summaries' if route_weighted else 'One latest valid reading per platform and direction'}
