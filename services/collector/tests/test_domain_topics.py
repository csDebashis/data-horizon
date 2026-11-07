"""Domain routing and outbox fault tests use explicit fake brokers, not Live evidence."""
from datetime import timedelta
import json
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs

from data_horizon import collector
from data_horizon.collector import (
    collect_once, drain_outbox, ensure_event_topics, ingest_static, publish,
    publish_auxiliary_snapshot, refresh_static,
)
from data_horizon.config import Settings
from data_horizon.contracts import TransitEvent, utcnow
from data_horizon.event_topics import reassemble_snapshot_events
from data_horizon.store import Store

pytestmark = pytest.mark.feature("F004")


class Future:
    def __init__(self, fail=False):
        self.fail = fail

    def get(self, timeout):
        if self.fail:
            raise TimeoutError("injected unavailable topic")


class Producer:
    def __init__(self, failing=()):
        self.sent = []
        self.failing = set(failing)

    def send(self, topic, *, key, value):
        self.sent.append((topic, key, value))
        return Future(any(f".{kind}.v1" in topic for kind in self.failing))

    def flush(self, timeout):
        pass

    def close(self, timeout):
        pass


def transit(now=None):
    now = now or utcnow()
    return TransitEvent(source="unit-fixture", route_id="1", direction_id="0", stop_id="A", trip_id="T1",
                        event_time=now, predicted_time=now + timedelta(minutes=2),
                        time_basis="predicted").model_dump(mode="json")


def source_feed(now, *, trip=True):
    message = gtfs.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    message.header.timestamp = int(now.timestamp())
    if trip:
        entity = message.entity.add()
        entity.id = "trip"
        entity.trip_update.trip.route_id = "1"
        entity.trip_update.trip.trip_id = "T1"
        entity.trip_update.trip.start_date = now.strftime("%Y%m%d")
        stop = entity.trip_update.stop_time_update.add()
        stop.stop_id = "A"
        stop.arrival.time = int((now + timedelta(minutes=2)).timestamp())
    return message.SerializeToString()


def test_five_types_are_created_explicitly_without_broker_auto_creation():
    settings = Settings(kafka_servers="private-kafka:9092")

    class Admin:
        def list_topics(self):
            return [settings.transit_topic, "unrelated-existing-topic"]

        def create_topics(self, topics, timeout_ms):
            self.created = topics
            assert timeout_ms == 10000
    admin = Admin()
    assert ensure_event_topics(settings, admin) == settings.event_topics
    assert {topic.name for topic in admin.created} == set(settings.event_topics.values()) - {settings.transit_topic}
    assert all(topic.num_partitions == 3 and topic.replication_factor == 1 for topic in admin.created)


def test_all_public_domains_route_to_own_topic_and_stable_keys(isolated_data_dir):
    settings, producer = Settings.load(), Producer()
    event = transit()
    publish(settings, [event], producer)
    for kind in tuple(settings.event_topics)[1:]:
        publish_auxiliary_snapshot(settings, kind, {"items": []}, source="unit-source",
                                   retrieved_at=utcnow(), producer=producer)
    assert {topic for topic, _, _ in producer.sent} == set(settings.event_topics.values())
    assert producer.sent[0][1] == "1|0|A"
    for topic, key, record in producer.sent[1:]:
        assert settings.topic_for(record["event_type"]) == topic
        assert key == record["snapshot_id"] and record["mode"] == "synthetic"
    assert drain_outbox(settings, producer) == {"published_events": 0, "pending_batches": 0}


def test_partial_chunk_publication_retries_complete_identity_before_receipt(isolated_data_dir):
    settings = Settings.load()
    payload = {"alerts": [{"id": "long", "details": "x" * 600000}], "authoritative": True}
    failed = Producer(failing={"service-alerts"})
    with pytest.raises(ConnectionError, match="durable outbox"):
        publish_auxiliary_snapshot(settings, "service-alerts", payload, source="fixture",
                                   retrieved_at=utcnow(), producer=failed)
    assert len(failed.sent) > 1
    assert not list((isolated_data_dir / "outbox/synthetic/published").glob("*.json"))
    healthy = Producer()
    result = drain_outbox(settings, healthy)
    assert result["published_events"] == len(failed.sent) and result["pending_batches"] == 0
    assert [r[2]["event_id"] for r in healthy.sent] == [r[2]["event_id"] for r in failed.sent]
    assert reassemble_snapshot_events([r[2] for r in healthy.sent]) == payload


def test_corrupt_optional_mirror_does_not_starve_pending_transit(isolated_data_dir):
    settings = Settings(kafka_servers="unavailable:9092", data_dir=isolated_data_dir)
    with pytest.raises(ConnectionError):
        publish_auxiliary_snapshot(settings, "vehicle-status", {"vehicles": []},
                                   source="fixture", retrieved_at=utcnow())
    pending = isolated_data_dir / "outbox/synthetic/pending"
    auxiliary = next(pending.glob("*.json"))
    entry = json.loads(auxiliary.read_text())
    Path(entry["manifest"]["path"]).write_text("tampered")
    with pytest.raises(ConnectionError):
        publish(settings, [transit()])
    healthy = Producer()
    result = drain_outbox(settings, healthy)
    assert [r[0] for r in healthy.sent] == [settings.transit_topic]
    assert result["pending_batches"] == 1
    assert result["mirror_failures"] == [{"event_type": "vehicle-status", "reason": "ValueError"}]
    assert auxiliary.exists()


@pytest.mark.parametrize("field,value", [("mode", "live"), ("topic", "private-inbox"),
                                         ("receipt_id", "0" * 64)])
def test_outbox_scope_topic_and_receipt_identity_cannot_be_forged(isolated_data_dir, field, value):
    settings = Settings(kafka_servers="unavailable:9092", data_dir=isolated_data_dir)
    with pytest.raises(ConnectionError):
        publish(settings, [transit()])
    path = next((isolated_data_dir / "outbox/synthetic/pending").glob("*.json"))
    entry = json.loads(path.read_text())
    entry[field] = value
    path.write_text(json.dumps(entry))
    producer = Producer()
    with pytest.raises(ValueError, match="mismatch"):
        drain_outbox(settings, producer)
    assert not producer.sent and path.exists()


def test_live_source_mirrors_empty_clears_and_weather_only_on_retrieval(isolated_data_dir, monkeypatch):
    settings, store, producer, now = Settings.load(), Store(), Producer(), utcnow()
    fetched = []

    def fetch(url, **kwargs):
        fetched.append(url)
        if url == settings.weather_url:
            return json.dumps({"properties": {"timestamp": now.isoformat(), "temperature": {"value": 12}}}).encode()
        return source_feed(now, trip=url == settings.realtime_url)

    monkeypatch.setattr(collector, "fetch", fetch)
    collect_once(store, settings, producer=producer)
    collect_once(store, settings, producer=producer)
    assert fetched.count(settings.weather_url) == 1
    weather = [r for topic, _, r in producer.sent if topic == settings.topic_for("weather-observations")]
    assert len(weather) == 1 and weather[0]["data"]["observed_at"] == now.isoformat()
    alerts = [r for topic, _, r in producer.sent if topic == settings.topic_for("service-alerts")]
    assert len(alerts) == 2 and all(r["data"]["alerts"] == [] for r in alerts)
    vehicles = [r for topic, _, r in producer.sent if topic == settings.topic_for("vehicle-status")]
    assert len(vehicles) == 2
    assert all(r["data"]["vehicles"] == [] for r in vehicles)


def test_retained_alerts_and_failed_tracking_are_not_republished_as_fresh(isolated_data_dir, monkeypatch):
    settings, store, producer, now = Settings.load(), Store(), Producer(), utcnow()
    old = (now - timedelta(minutes=10)).isoformat()
    prior = {"id": "old-notice", "available_at": old, "route_ids": ["1"], "severity": 1}
    store.replace_assets("alerts", [prior])
    store.put_metadata("sources", {"alerts": {"status": "ok", "last_success": old}})
    store.put_metadata("weather", {"retrieved_at": now.isoformat(), "observed_at": old})

    def fetch(url, **kwargs):
        if url == settings.alerts_url:
            raise OSError("source outage")
        return source_feed(now)

    monkeypatch.setattr(collector, "fetch", fetch)
    monkeypatch.setattr(collector, "decode_tracking_snapshot", lambda *a, **k: (_ for _ in ()).throw(ValueError()))
    collect_once(store, settings, producer=producer)
    assert [row[0] for row in producer.sent] == [settings.transit_topic]
    assert store.assets("alerts") == [prior]
    assert store.get_metadata("sources")["alerts"]["last_success"] == old


def test_optional_unavailable_mirror_is_reported_after_successful_transit(isolated_data_dir, monkeypatch):
    settings, store, now = Settings.load(), Store(), utcnow()
    producer = Producer(failing={"vehicle-status"})
    store.put_metadata("weather", {"retrieved_at": now.isoformat()})
    monkeypatch.setattr(collector, "fetch", lambda url, **k: source_feed(now, trip=url == settings.realtime_url))
    result = collect_once(store, settings, producer=producer)
    assert result["events"] == 1 and producer.sent[0][0] == settings.transit_topic
    assert store.get_metadata("event_mirrors")["vehicle-status"]["status"] == "degraded"
    assert store.get_metadata("sources")["transit"]["status"] == "ok"


def test_static_descriptor_only_publishes_after_new_version_activation(isolated_data_dir, monkeypatch):
    from test_collection_recovery import schedule_zip

    settings, store, producer, now = Settings.load(), Store(), Producer(), utcnow()
    first = schedule_zip()
    ingest_static(first, store, settings)
    clock = now + timedelta(days=2)
    monkeypatch.setattr(collector, "utcnow", lambda: clock)
    monkeypatch.setattr(collector, "fetch", lambda *a, **k: first)
    matcher = refresh_static(store, settings, now=clock, producer=producer)
    assert not producer.sent
    clock += timedelta(days=2)
    monkeypatch.setattr(collector, "fetch", lambda *a, **k: schedule_zip(minute=6))
    refresh_static(store, settings, matcher, now=clock, producer=producer)
    assert len(producer.sent) == 1 and producer.sent[0][0] == settings.topic_for("static-snapshots")
    descriptor = producer.sent[0][2]["data"]
    assert descriptor["route_count"] == 1 and descriptor["stop_count"] == 2
    assert not {"schedule_path", "planner_path", "path"} & descriptor.keys()


def test_synthetic_activation_populates_five_domains_without_inventing_vehicle_rows(isolated_data_dir, monkeypatch):
    settings = Settings(kafka_servers="fake-broker:9092", data_dir=isolated_data_dir)
    producer = Producer()
    monkeypatch.setattr(collector.Settings, "load", lambda: settings)
    monkeypatch.setattr(collector, "producer_for", lambda settings: producer)
    collector.run_collector(once=True)
    assert {topic for topic, _, _ in producer.sent} == set(settings.event_topics.values())
    mirrors = [record for topic, _, record in producer.sent if topic != settings.transit_topic]
    assert all(record["mode"] == "synthetic" and record["source"] == "synthetic-fixture" for record in mirrors)
    vehicle = next(record for record in mirrors if record["event_type"] == "vehicle-status")
    assert vehicle["data"]["vehicles"] == [] and vehicle["data"]["time_basis"] == "synthetic"
    static_events = [record for record in mirrors if record["event_type"] == "static-snapshots"]
    # Re-activating an unchanged fixture uses its original version clock and receipt.
    collector.run_collector(once=True)
    after = [record for topic, _, record in producer.sent if topic == settings.topic_for("static-snapshots")]
    assert after == static_events


@pytest.mark.parametrize("bad_receipt", [[], {"event_type": "vehicle-status", "created_at": 5},
                                         {"event_type": "service-alerts", "created_at": "not-a-clock"}])
def test_malformed_optional_receipt_never_breaks_transit_priority(isolated_data_dir, bad_receipt):
    settings = Settings(kafka_servers="unavailable:9092", data_dir=isolated_data_dir)
    with pytest.raises(ConnectionError):
        publish(settings, [transit()])
    folder = isolated_data_dir / "outbox/synthetic/pending"
    broken = folder / "invalid-optional.json"
    broken.write_text(json.dumps(bad_receipt))
    producer = Producer()
    result = drain_outbox(settings, producer)
    assert [topic for topic, _, _ in producer.sent] == [settings.transit_topic]
    assert result["published_events"] == 1 and result["pending_batches"] == 1
    assert result["mirror_failures"] == [{"event_type": "unknown", "reason": "ValueError"}]
    assert broken.exists()


def test_actual_producer_serializer_matches_utf8_record_byte_bound(monkeypatch):
    import kafka
    from data_horizon.event_topics import MAX_AUXILIARY_RECORD_BYTES, build_snapshot_events, canonical_bytes

    monkeypatch.setattr(collector, "ensure_event_topics", lambda settings: settings.event_topics)
    monkeypatch.setattr(kafka, "KafkaProducer", lambda **kwargs: kwargs)
    options = collector.producer_for(Settings(kafka_servers="private-broker:9092"))
    records = build_snapshot_events("service-alerts", "synthetic", {"text": "🚆" * 50000},
                                    source="fixture", retrieved_at=utcnow())
    for record in records:
        wire = options["value_serializer"](record)
        assert wire == canonical_bytes(record) and len(wire) <= MAX_AUXILIARY_RECORD_BYTES
    assert options["acks"] == "all" and options["retries"] == 5
