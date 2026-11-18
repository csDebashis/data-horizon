"""Immutable timetable lookups and conservative published-schedule identity checks."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import re

from .contracts import service_time, timestamp
from .store import dumps

SUPPLEMENTED_URL = 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_supplemented.zip'
SOURCE_PROFILE = 'mta-nyct-supplemented-v1'
LOOKUP_SCHEMA_VERSION = 2
MAX_CALENDAR_BYTES = 4 * 1024 * 1024
DAYS = ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
# The complete stopping-path component is required. The origin and direction alone
# cannot identify a published trip, particularly during changed service.
CANONICAL_TRIP = re.compile(r'(?P<origin>[0-9]{6})_(?P<route>[1-6])\.\.(?P<direction>[NS])(?P<path>[A-Za-z0-9]+)')
# NYC realtime trips appear about 30 minutes before origin. A six-hour past
# boundary allows long/late subway trips without accepting another day's train.
ORIGIN_LOOKAHEAD = timedelta(minutes=30)
ORIGIN_LOOKBACK = timedelta(hours=6)


def source_profile(url):
    return SOURCE_PROFILE if url == SUPPLEMENTED_URL else None


def gtfs_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{8}', value):
        raise ValueError('Invalid GTFS calendar date')
    return date.fromisoformat(f'{value[:4]}-{value[4:6]}-{value[6:]}').isoformat()


def _identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 200:
        raise ValueError('Invalid timetable identifier')
    return value


def _iso_date(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError('Invalid lookup calendar date')
    return value


def _validate_calendar(calendar):
    for service, weekly in calendar['services'].items():
        _identifier(service)
        if set(weekly) != {'start_date', 'end_date', 'weekdays'}:
            raise ValueError('Unknown weekly calendar fields')
        start, end = _iso_date(weekly['start_date']), _iso_date(weekly['end_date'])
        days = weekly['weekdays']
        if start > end or len(days) != 7 or any(type(day) is not int or day not in (0, 1) for day in days):
            raise ValueError('Invalid weekly calendar')
    for service, exceptions in calendar['exceptions'].items():
        _identifier(service)
        for day, kind in exceptions.items():
            _iso_date(day)
            if type(kind) is not int or kind not in (1, 2):
                raise ValueError('Invalid calendar exception')
    validity = calendar['feed_validity']
    if validity is not None:
        if set(validity) != {'start_date', 'end_date'}:
            raise ValueError('Unknown feed validity fields')
        if _iso_date(validity['start_date']) > _iso_date(validity['end_date']):
            raise ValueError('Invalid feed validity')


def parse_calendar(z, csv_rows, service_ids):
    """Read weekly service, overriding date exceptions and optional feed validity."""
    services, exceptions = {}, {}
    names = set(z.namelist())
    if 'calendar.txt' in names:
        for row in csv_rows(z, 'calendar.txt'):
            service = _identifier(row.get('service_id'))
            if service in services or any(row.get(day) not in ('0', '1') for day in DAYS):
                raise ValueError('Duplicate service or invalid weekly calendar')
            start, end = gtfs_date(row.get('start_date')), gtfs_date(row.get('end_date'))
            if start > end:
                raise ValueError('Calendar date range is reversed')
            services[service] = {'start_date': start, 'end_date': end,
                                 'weekdays': [int(row[day]) for day in DAYS]}
    if 'calendar_dates.txt' in names:
        for row in csv_rows(z, 'calendar_dates.txt'):
            service, day = _identifier(row.get('service_id')), gtfs_date(row.get('date'))
            if row.get('exception_type') not in ('1', '2') or day in exceptions.get(service, {}):
                raise ValueError('Invalid or conflicting service exception')
            exceptions.setdefault(service, {})[day] = int(row['exception_type'])
    if set(service_ids) - (set(services) | set(exceptions)):
        raise ValueError('Trip service has no calendar')
    validity = None
    if 'feed_info.txt' in names:
        rows = list(csv_rows(z, 'feed_info.txt'))
        if len(rows) > 1:
            raise ValueError('Conflicting feed validity records')
        if rows:
            start, end = rows[0].get('feed_start_date'), rows[0].get('feed_end_date')
            if start or end:
                start, end = gtfs_date(start), gtfs_date(end)
                if start > end:
                    raise ValueError('Feed validity date range is reversed')
                validity = {'start_date': start, 'end_date': end}
    return {'services': services, 'exceptions': exceptions, 'feed_validity': validity}


def lookup_artifacts(version, source_url, records, calendar):
    """Build mutually bound contents; filenames also distinguish source profiles."""
    rows = ('\n'.join(dumps(record) for record in records) + '\n').encode()
    identity = {'lookup_schema_version': LOOKUP_SCHEMA_VERSION, 'archive_sha256': version,
                'source_url': source_url, 'source_profile': source_profile(source_url)}
    calendar_body = dumps({**identity, **calendar,
                           'schedule_records_sha256': hashlib.sha256(rows).hexdigest()}).encode()
    calendar_sha = hashlib.sha256(calendar_body).hexdigest()
    header = dumps({**identity, 'calendar_sha256': calendar_sha}).encode()
    source_key = hashlib.sha256(source_url.encode()).hexdigest()[:16]
    stem = f'{version}.{source_key}'
    return stem, header + b'\n' + rows, calendar_body, calendar_sha


def trip_direction(trip_id):
    parsed = CANONICAL_TRIP.fullmatch(trip_id)
    return ('0' if parsed['direction'] == 'N' else '1') if parsed else None


class ScheduleMatcher:
    """Match a published timetable, never a measured arrival.

    Legacy fixtures retain literal-ID matching. A shortened MTA trip ID requires
    the verified supplemented profile, active calendar and complete trip context.
    No nearest-time or origin-only matching is used.
    """
    def __init__(self, path=None):
        self.exact = {}
        self.suffix = defaultdict(list)
        self.calendar = None
        self.source_profile = None
        self.source_url = None
        self.lookup_schema_version = 1
        if path is None:
            return
        path = Path(path)
        with path.open('rb') as stream:
            first_record = json.loads(stream.readline())
            expected_rows_sha = None
            if first_record.get('lookup_schema_version') == LOOKUP_SCHEMA_VERSION:
                calendar_path = path.with_name(path.name.replace('.schedule-v2.jsonl', '.calendar-v2.json'))
                if calendar_path == path:
                    raise ValueError('Versioned timetable filename required')
                with calendar_path.open('rb') as calendar_stream:
                    calendar_body = calendar_stream.read(MAX_CALENDAR_BYTES + 1)
                if len(calendar_body) > MAX_CALENDAR_BYTES:
                    raise ValueError('Calendar lookup exceeds byte limit')
                if hashlib.sha256(calendar_body).hexdigest() != first_record.get('calendar_sha256'):
                    raise ValueError('Calendar checksum mismatch')
                calendar = json.loads(calendar_body)
                for key in ('lookup_schema_version', 'archive_sha256', 'source_url', 'source_profile'):
                    if first_record.get(key) != calendar.get(key):
                        raise ValueError('Timetable/calendar source identity mismatch')
                if (
                    not re.fullmatch(r'[a-f0-9]{64}', first_record.get('archive_sha256', ''))
                    or not path.name.startswith(first_record['archive_sha256'] + '.')
                    or source_profile(first_record.get('source_url')) != first_record.get('source_profile')
                ):
                    raise ValueError('Timetable source or schedule checksum mismatch')
                _validate_calendar(calendar)
                records_start = stream.tell()
                digest = hashlib.sha256()
                while block := stream.read(1024 * 1024):
                    digest.update(block)
                expected_rows_sha = calendar.get('schedule_records_sha256')
                if digest.hexdigest() != expected_rows_sha:
                    raise ValueError('Timetable source or schedule checksum mismatch')
                self.calendar = calendar
                self.source_url = first_record['source_url']
                self.source_profile = first_record['source_profile']
                self.lookup_schema_version = LOOKUP_SCHEMA_VERSION
                stream.seek(records_start)
            else:
                if path.name.endswith('.schedule-v2.jsonl'):
                    raise ValueError('Versioned timetable header is missing')
                stream.seek(0)
            # Verify first, then parse one row at a time. Neither pass retains
            # the complete serialized schedule alongside its parsed objects.
            parsed_digest = hashlib.sha256() if expected_rows_sha is not None else None
            for line in stream:
                if parsed_digest is not None:
                    parsed_digest.update(line)
                record = json.loads(line)
                trip = _identifier(record['trip_id'])
                if trip in self.exact:
                    raise ValueError('Duplicate timetable trip')
                if self.calendar is not None:
                    self._validate_record(record)
                self.exact[trip] = record
                pieces = trip.split('_')
                if len(pieces) >= 2:
                    self.suffix['_'.join(pieces[-2:])].append(record)
            # Also reject in-place changes between verification and parsing.
            if parsed_digest is not None and parsed_digest.hexdigest() != expected_rows_sha:
                raise ValueError('Timetable changed while loading')

    def _validate_record(self, record):
        _identifier(record['route_id'])
        service = _identifier(record['service_id'])
        if service not in self.calendar['services'] and service not in self.calendar['exceptions']:
            raise ValueError('Unknown timetable service')
        if str(record.get('direction_id')) not in ('0', '1'):
            raise ValueError('Unknown timetable direction')
        seen = set()
        for sequence, stop, value in record['stops']:
            _identifier(stop)
            if not isinstance(sequence, int) or sequence < 0 or sequence in seen:
                raise ValueError('Invalid or duplicate stop sequence')
            seen.add(sequence)
            if value:
                # Date-independent time validation; DST is checked for the source service day.
                if not re.fullmatch(r'[0-9]{1,2}:[0-9]{2}:[0-9]{2}', value):
                    raise ValueError('Invalid schedule time')
                hour, minute, second = map(int, value.split(':'))
                if hour > 71 or minute > 59 or second > 59:
                    raise ValueError('Invalid schedule time')

    def _active(self, record, day):
        if self.calendar is None:
            return True
        parsed = date.fromisoformat(_iso_date(day))
        validity = self.calendar['feed_validity']
        if validity and not validity['start_date'] <= day <= validity['end_date']:
            return False
        service = record.get('service_id')
        exception = self.calendar['exceptions'].get(service, {}).get(day)
        if exception is not None:
            return exception == 1
        weekly = self.calendar['services'].get(service)
        return bool(weekly and weekly['start_date'] <= day <= weekly['end_date']
                    and weekly['weekdays'][parsed.weekday()])

    def effective_service_date(self, trip_id, service_date, *, reference_time=None):
        """Resolve the verified MTA feed's overnight date convention conservatively.

        A shortened ID may contain a 24-plus-hour origin while start_date names
        the local departure day. Only its complete origin ID and source clock
        can establish the GTFS service day. Arrival forecasts never choose it.
        Already correct GTFS dates keep their original extended-hour meaning.
        """
        parsed = CANONICAL_TRIP.fullmatch(trip_id)
        if self.source_profile != SOURCE_PROFILE or trip_id in self.exact or not parsed:
            return service_date
        origin = int(parsed['origin'])
        day_offset = origin // 144000  # Hundredths of minutes in a complete day.
        if not day_offset:
            return service_date
        if reference_time is None or not service_date:
            return None
        try:
            reference = timestamp(reference_time)
            source_day = date.fromisoformat(_iso_date(service_date))
            hours, remainder = divmod(origin * 60 // 100, 3600)
            minutes, seconds = divmod(remainder, 60)
            origin_time = f'{hours:02}:{minutes:02}:{seconds:02}'
            possible = []
            for day in (source_day, source_day - timedelta(days=day_offset)):
                instant = service_time(day, origin_time)
                if reference - ORIGIN_LOOKBACK <= instant <= reference + ORIGIN_LOOKAHEAD:
                    possible.append(day.isoformat())
            return possible[0] if len(possible) == 1 else None
        except (ValueError, TypeError, AttributeError, OverflowError):
            return None

    def match(self, trip_id, stop_id, service_date, *, route_id=None, direction_id=None,
              remaining_stop_ids=None, reference_time=None):
        literal = trip_id in self.exact
        candidates = [self.exact[trip_id]] if literal else self.suffix.get(trip_id, [])
        if not candidates:
            return None, 'unmatched'
        canonical = not literal and self.source_profile == SOURCE_PROFILE
        if canonical:
            parsed = CANONICAL_TRIP.fullmatch(trip_id)
            if (
                not parsed or not service_date or route_id is None or direction_id is None
                or not remaining_stop_ids or parsed['route'] != str(route_id)
                or trip_direction(trip_id) != str(direction_id)
            ):
                return None, 'tentative'
            service_date = self.effective_service_date(trip_id, service_date, reference_time=reference_time)
            if service_date is None:
                return None, 'tentative'
        if self.calendar is not None:
            if not service_date:
                return None, 'tentative'
            try:
                candidates = [record for record in candidates if self._active(record, service_date)]
            except (ValueError, TypeError):
                return None, 'ambiguous'
        if route_id is not None:
            candidates = [r for r in candidates if r.get('route_id') == str(route_id)]
        if direction_id is not None:
            candidates = [r for r in candidates if str(r.get('direction_id')) == str(direction_id)]
        if len(candidates) != 1:
            return None, 'ambiguous' if candidates else 'unmatched'
        record = candidates[0]
        if canonical:
            if int(parsed['origin']) >= 144000:
                try:
                    published_origin = service_time(service_date, record['stops'][0][2])
                    reference = timestamp(reference_time)
                    if not reference - ORIGIN_LOOKBACK <= published_origin <= reference + ORIGIN_LOOKAHEAD:
                        return None, 'tentative'
                except (ValueError, TypeError, AttributeError, IndexError, OverflowError):
                    return None, 'tentative'
            stops = [row[1] for row in record['stops']]
            if len(set(remaining_stop_ids)) != len(remaining_stop_ids):
                return None, 'ambiguous'
            if stop_id not in remaining_stop_ids or any(stops.count(stop) != 1 for stop in remaining_stop_ids):
                return None, 'unmatched'
            positions = [stops.index(stop) for stop in remaining_stop_ids]
            if positions != sorted(positions) or list(remaining_stop_ids) != stops[positions[0]:]:
                return None, 'unmatched'
        matching = [row for row in record['stops'] if row[1] == stop_id]
        if len(matching) != 1 or not matching[0][2]:
            return None, 'ambiguous' if len(matching) > 1 else 'unmatched'
        try:
            value = service_time(service_date, matching[0][2]).isoformat()
        except (ValueError, TypeError, AttributeError):
            return None, 'ambiguous'
        return value, 'exact' if literal or canonical else 'tentative'
