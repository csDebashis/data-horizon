from google.transit import gtfs_realtime_pb2 as gtfs
import pytest

from data_horizon.collector import decode_feed
from data_horizon.config import Settings

pytestmark = [pytest.mark.case("F002-TC08"), pytest.mark.evidence("synthetic", "contract")]


def feed_alert(selectors):
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 1794657600
    entity = feed.entity.add()
    entity.id = "scoped-alert"
    entity.alert.header_text.translation.add(text="Station access disruption")
    for values in selectors:
        selector = entity.alert.informed_entity.add()
        for key, value in values.items():
            if key == "trip":
                for field, item in value.items():
                    setattr(selector.trip, field, item)
            else:
                setattr(selector, key, value)
    return feed.SerializeToString()


def test_raw_route_station_alternatives_are_preserved_without_cross_product():
    _, alerts, _ = decode_feed(
        feed_alert([{"route_id": "1", "stop_id": "A"}, {"route_id": "2", "stop_id": "B"}]),
        Settings(mode="synthetic"),
    )
    assert alerts[0]["informed_entities"] == [
        {"route_id": "1", "stop_id": "A"},
        {"route_id": "2", "stop_id": "B"},
    ]


def test_raw_trip_and_zero_direction_constraints_are_not_discarded():
    selectors = [
        {
            "route_id": "1",
            "direction_id": 0,
            "trip": {"trip_id": "run-A", "start_date": "20261114", "direction_id": 0},
        }
    ]
    _, alerts, _ = decode_feed(feed_alert(selectors), Settings(mode="synthetic"))
    assert alerts[0]["informed_entities"] == selectors


@pytest.mark.parametrize("selector", [{"stop_id": "A"}, {"agency_id": "explicit-agency"}, {"route_type": 1}])
def test_raw_non_route_selectors_are_retained_restrictively(selector):
    _, alerts, _ = decode_feed(feed_alert([selector]), Settings(mode="synthetic"))
    assert alerts[0]["informed_entities"] == [selector]


def test_unknown_native_wire_restriction_is_not_dropped_to_a_route_wildcard():
    feed = gtfs.FeedMessage.FromString(feed_alert([{"route_id": "1"}]))
    selector = feed.entity[0].alert.informed_entity[0]
    # Unknown length-delimited field 123, carrying a future restrictive value.
    selector.MergeFromString(b"\xda\x07\x01x")
    _, alerts, _ = decode_feed(feed.SerializeToString(), Settings(mode="synthetic"))
    from data_horizon.alert_scope import alert_matches

    assert alerts[0]["informed_entities"][0]["unsupported_wire_restriction"] is True
    assert not alert_matches(alerts[0], {"route_id": "1"})


def test_selector_count_and_individual_size_are_bounded():
    _, alerts, rejected = decode_feed(feed_alert([{"route_id": "1"}] * 201), Settings(mode="synthetic"))
    assert alerts == [] and rejected["alerts_rejected"] == 1 and rejected["rejected"] == 0
    _, alerts, _ = decode_feed(feed_alert([{"stop_id": "A" * 17000}]), Settings(mode="synthetic"))
    assert alerts[0]["informed_entities"] == [{"unsupported_restriction": "oversized-selector"}]


def test_mixed_feed_keeps_alert_rejections_separate_from_transit_candidate_quality():
    feed = gtfs.FeedMessage.FromString(feed_alert([{"route_id": "1"}] * 201))
    trip = feed.entity.add(id="mixed-trip").trip_update
    trip.trip.trip_id = "mixed-run"
    trip.trip.route_id = "1"
    trip.stop_time_update.add(stop_id="A").arrival.time = feed.header.timestamp + 300
    trip.stop_time_update.add(stop_id="B")  # Invalid transit candidate: no estimate.
    vehicle = feed.entity.add(id="mixed-vehicle").vehicle
    vehicle.trip.route_id = "2"
    vehicle.trip.trip_id = "vehicle-run"
    vehicle.stop_id = "C"
    valid_alert = feed.entity.add(id="valid-alert").alert
    valid_alert.header_text.translation.add(text="Valid scoped notice")
    valid_alert.informed_entity.add(route_id="1", stop_id="A")
    # Count native transit candidates independently of the decoder's counters.
    candidates = sum(
        len(entity.trip_update.stop_time_update) for entity in feed.entity if entity.HasField("trip_update")
    )
    candidates += sum(1 for entity in feed.entity if entity.HasField("vehicle"))
    events, alerts, quality = decode_feed(feed.SerializeToString(), Settings(mode="synthetic"))
    assert candidates == 3
    assert len(events) == quality["accepted"] == 2
    assert quality["rejected"] == 1
    assert quality["accepted"] + quality["rejected"] == candidates
    assert quality["alerts_rejected"] == 1
    assert [alert["id"] for alert in alerts] == ["valid-alert"]


def _varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    return bytes(result + bytes([value]))


def _wire(number, payload, wire_type=2):
    return _varint((number << 3) | wire_type) + (_varint(len(payload)) + payload if wire_type == 2 else payload)


def _mercury_selector(selector, payload, *, number=1001, wire_type=2):
    # Build the official proto2 extension without registering or weakening the base schema.
    selector.MergeFromString(_wire(number, payload, wire_type))


def _native_alert(selectors=None, metadata=None):
    values = selectors or [{'agency_id': 'MTASBWY', 'route_id': '4'}]
    feed = gtfs.FeedMessage.FromString(feed_alert(values))
    for selector in feed.entity[0].alert.informed_entity:
        payload = metadata if metadata is not None else _wire(1, b'MTASBWY:4:29')
        _mercury_selector(selector, payload)
    return feed


def _decode_official(feed, **settings_changes):
    settings = Settings(mode='synthetic', **settings_changes)
    _, alerts, _ = decode_feed(feed.SerializeToString(), settings, source_url=settings.alerts_url)
    return alerts[0]


@pytest.mark.case('F003-COL01', 'F007-COL02')
def test_official_mercury_priority_and_agency_alias_preserve_a_useful_scoped_notice():
    from data_horizon.alert_scope import alert_matches

    alert = _decode_official(_native_alert())
    assert alert['informed_entities'] == [{'agency_id': 'MTA NYCT', 'route_id': '4'}]
    assert alert['selector_metadata'] == [{
        'selector_index': 0, 'sort_order': 'MTASBWY:4:29',
        'source_agency_id': 'MTASBWY', 'canonical_agency_id': 'MTA NYCT',
    }]
    assert alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4'})
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '6'})


@pytest.mark.case('F003-COL01', 'F007-COL02')
def test_mercury_display_metadata_does_not_remove_route_stop_direction_or_trip_clauses():
    from data_horizon.alert_scope import alert_matches

    selector = {'agency_id': 'MTASBWY', 'route_id': '4', 'stop_id': '619N', 'direction_id': 0,
                'trip': {'trip_id': 'run-A', 'start_date': '20261119', 'direction_id': 0}}
    alert = _decode_official(_native_alert([selector]))
    context = {'agency_id': 'MTA NYCT', 'route_id': '4', 'stop_id': '619N', 'direction_id': 0,
               'trip_id': 'run-A', 'service_date': '2026-11-19'}
    assert alert_matches(alert, context)
    for field, value in [('route_id', '6'), ('stop_id', '619S'), ('direction_id', 1),
                         ('trip_id', 'another-trip'), ('service_date', '2026-11-20')]:
        assert not alert_matches(alert, {**context, field: value})


@pytest.mark.case('F003-COL01')
def test_mercury_selector_alternatives_stay_alternatives_without_a_cross_product():
    from data_horizon.alert_scope import alert_matches

    alert = _decode_official(_native_alert([
        {'agency_id': 'MTASBWY', 'route_id': '4', 'stop_id': 'A'},
        {'agency_id': 'MTASBWY', 'route_id': '6', 'stop_id': 'B'},
    ]))
    assert alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4', 'stop_id': 'A'})
    assert alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '6', 'stop_id': 'B'})
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4', 'stop_id': 'B'})


@pytest.mark.case('F003-COL01')
@pytest.mark.parametrize('payload', [
    b'', b'\x0a\xff', _wire(1, b'\xff'), _wire(1, b'bad\npriority'),
    _wire(1, b'MTASBWY:4:36'), _wire(1, b'X' * 600),
    _wire(1, b'MTASBWY:4:29') + _wire(2, b'future-scope'),
    _wire(1, b'MTASBWY:4:29') + _wire(1, b'MTASBWY:6:29'),
    _wire(1, b'MTASBWY:4:29', wire_type=0),
])
def test_malformed_or_unknown_nested_mercury_metadata_stays_restrictive(payload):
    from data_horizon.alert_scope import alert_matches

    alert = _decode_official(_native_alert(metadata=payload))
    assert alert['informed_entities'][0]['unsupported_wire_restriction'] is True
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4'})


@pytest.mark.case('F003-COL01')
@pytest.mark.parametrize('number,wire_type,payload', [(1002, 2, b'x'), (1001, 0, b'\x01')])
def test_other_unknown_wire_fields_and_wrong_extension_type_stay_restrictive(number, wire_type, payload):
    from data_horizon.alert_scope import alert_matches

    feed = _native_alert()
    _mercury_selector(feed.entity[0].alert.informed_entity[0], payload, number=number, wire_type=wire_type)
    alert = _decode_official(feed)
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4'})


@pytest.mark.case('F003-COL01')
def test_unknown_nested_trip_scope_is_not_hidden_by_valid_mercury_metadata():
    from data_horizon.alert_scope import alert_matches

    feed = _native_alert([{'agency_id': 'MTASBWY', 'route_id': '4', 'trip': {'trip_id': 'run-A'}}])
    feed.entity[0].alert.informed_entity[0].trip.MergeFromString(_wire(123, b'future-trip-constraint'))
    alert = _decode_official(feed)
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4', 'trip_id': 'run-A'})


@pytest.mark.case('F003-COL01')
def test_agency_alias_is_limited_to_explicit_configured_official_subway_alerts_source():
    from data_horizon.alert_scope import alert_matches

    feed = _native_alert()
    settings = Settings(mode='synthetic')
    _, alerts, _ = decode_feed(feed.SerializeToString(), settings)
    assert alerts[0]['informed_entities'][0]['agency_id'] == 'MTASBWY'
    assert not alert_matches(alerts[0], {'agency_id': 'MTA NYCT', 'route_id': '4'})
    for url in (settings.realtime_url, 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Frailroad-alerts'):
        _, alerts, _ = decode_feed(feed.SerializeToString(), settings, source_url=url)
        assert alerts[0]['informed_entities'][0]['agency_id'] == 'MTASBWY'
        assert not alert_matches(alerts[0], {'agency_id': 'MTA NYCT', 'route_id': '4'})
    alert = _decode_official(_native_alert(), alerts_url='https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Frailroad-alerts')
    assert alert['informed_entities'][0]['agency_id'] == 'MTASBWY'


@pytest.mark.case('F003-COL01')
def test_unmapped_agencies_are_never_rewritten_to_subway_scope():
    from data_horizon.alert_scope import alert_matches

    alert = _decode_official(_native_alert([{'agency_id': 'OTHER', 'route_id': '4'}]))
    assert alert['informed_entities'][0]['agency_id'] == 'OTHER'
    assert not alert_matches(alert, {'agency_id': 'MTA NYCT', 'route_id': '4'})


@pytest.mark.case('F003-COL01')
def test_route_only_mercury_extension_is_unknown_outside_official_source():
    from data_horizon.alert_scope import alert_matches

    settings = Settings(mode='synthetic')
    feed = _native_alert([{'route_id': '4'}])
    for url in (None, settings.realtime_url):
        _, alerts, _ = decode_feed(feed.SerializeToString(), settings, source_url=url)
        assert alerts[0]['informed_entities'][0]['unsupported_wire_restriction'] is True
        assert not alert_matches(alerts[0], {'route_id': '4'})
    assert alert_matches(_decode_official(feed), {'route_id': '4'})
