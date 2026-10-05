"""Small validation helpers for the collector's vehicle-status source records."""
from __future__ import annotations

from datetime import date
import hashlib
import json
import re

MAX_TRACKED_VEHICLES = 1000
SOURCE = "mta-vehicle-status"
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.:-]+")


def valid_identifier(value, maximum=200):
    return isinstance(value, str) and 0 < len(value) <= maximum and bool(_IDENTIFIER.fullmatch(value))


def service_date(value):
    """Require the feed's explicit service date rather than guessing from today's date."""
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Invalid vehicle service date")
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("Invalid vehicle service date")
    return value


def public_trip_id(route_id, day, trip_id):
    if not valid_identifier(route_id, 32) or not valid_identifier(trip_id):
        raise ValueError("Invalid vehicle trip scope")
    service_date(day)
    identity = json.dumps([route_id, day, trip_id], separators=(",", ":"))
    return hashlib.sha256(identity.encode()).hexdigest()
