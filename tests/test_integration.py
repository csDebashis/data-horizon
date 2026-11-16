"""Real service checks. These never write into the live application database."""
from __future__ import annotations

from datetime import timedelta
import json
import os
from pathlib import Path
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
import uuid

import pytest

from data_horizon.contracts import TransitEvent, utcnow
from data_horizon.store import Store

pytestmark = pytest.mark.integration


@pytest.fixture
def postgres_store(tmp_path, monkeypatch):
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("BLOCKED: RUN_INTEGRATION=1 and an isolated PostgreSQL test database are required")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict
    dsn = os.getenv("TEST_DATABASE_URL", "")
    if not dsn and os.getenv("TEST_DATABASE_URL_FILE"):
        dsn = Path(os.environ["TEST_DATABASE_URL_FILE"]).read_text().strip()
    if not dsn:
        pytest.skip("BLOCKED: TEST_DATABASE_URL or TEST_DATABASE_URL_FILE is required")
    if conninfo_to_dict(dsn).get("dbname") != "horizon_test":
        pytest.fail("Integration tests refuse to write outside the dedicated horizon_test database")
    namespace = "test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(namespace)))
    parts = urlsplit(dsn)
    query = dict(parse_qsl(parts.query))
    query["options"] = f"-csearch_path={namespace}"
    isolated_dsn = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    monkeypatch.setenv("DATA_MODE", "synthetic")
    store = Store(data_dir=tmp_path, database_url=isolated_dsn, ensure_schema=True)
    try:
        yield store
    finally:
        with psycopg.connect(dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(namespace)))


def sample_event(index=0):
    now = utcnow().replace(microsecond=0)
    return TransitEvent(source="isolated-integration", route_id="1", trip_id=f"test-trip-{index}",
                        stop_id="101N", event_time=now, scheduled_time=now - timedelta(seconds=120),
                        observed_time=now, time_basis="observed", match_confidence="exact").model_dump(mode="json")


def test_postgres_sink_is_atomic_and_retry_safe(postgres_store):
    events = [sample_event(1), sample_event(2)]
    result = postgres_store.apply_batch(events, query_id="integration", batch_id="1", mode="synthetic")
    assert result["processed"] == 2
    before = postgres_store.metrics()
    replay = postgres_store.apply_batch(events, query_id="integration", batch_id="1", mode="synthetic")
    assert replay["batch_replayed"] is True
    assert postgres_store.metrics() == before
    again = postgres_store.apply_batch(events, query_id="integration", batch_id="2", mode="synthetic")
    assert again["duplicates"] == 2
    assert len(postgres_store.events()) == 2


def test_postgres_failed_aggregate_rolls_back_events_and_batch_marker(postgres_store):
    def broken(*args, **kwargs):
        raise RuntimeError("Injected before transaction commit")
    event = sample_event()
    with pytest.raises(RuntimeError, match="Injected"):
        postgres_store.apply_batch([event], query_id="rollback", batch_id="1", aggregate_fn=broken)
    assert postgres_store.events() == []
    assert postgres_store.apply_batch([event], query_id="rollback", batch_id="1")["processed"] == 1


@pytest.mark.case("F004-TC08")
def test_postgres_synthetic_and_live_namespaces_are_separate(postgres_store, monkeypatch, tmp_path):
    postgres_store.apply_batch([sample_event()], query_id="isolation", batch_id="1")
    monkeypatch.setenv("DATA_MODE", "live")
    # Only the synthetic fixture event is written; live namespace remains untouched.
    other = Store(data_dir=tmp_path, database_url=postgres_store.url, ensure_schema=False)
    assert other.events() == []
    assert len(postgres_store.events()) == 1


@pytest.mark.case("F004-TC01")
def test_kafka_publish_consume_and_postgres_output(postgres_store):
    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
    from kafka.admin import NewTopic
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = "horizon-integration-" + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all",
                             value_serializer=lambda value: json.dumps(value).encode())
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, auto_offset_reset="earliest",
                             group_id=topic, enable_auto_commit=False,
                             value_deserializer=lambda value: json.loads(value.decode()))
    expected = [sample_event(index) for index in range(6)]
    try:
        for event in expected:
            producer.send(topic, key=event["route_id"].encode(), value=event).get(timeout=15)
        producer.flush(timeout=15)
        received = []
        deadline = time.monotonic() + 30
        while len(received) < len(expected) and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                received.extend(record.value for record in records)
        assert {event["event_id"] for event in received} == {event["event_id"] for event in expected}
        result = postgres_store.apply_batch(received, query_id=topic, batch_id="1")
        assert result["processed"] == len(expected)
        assert len(postgres_store.events()) == len(expected)
    finally:
        consumer.close()
        producer.close()
        admin.delete_topics([topic])
        admin.close()


@pytest.mark.case("F012-TC01")
def test_launched_api_reads_fresh_spark_pipeline():
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("BLOCKED: launch the isolated synthetic Docker deployment first")
    import httpx
    base = os.getenv("API_BASE_URL", "http://api:8000")
    token = os.getenv("API_TOKEN", "")
    if not token and os.getenv("API_TOKEN_FILE"):
        token = Path(os.environ["API_TOKEN_FILE"]).read_text().strip()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with httpx.Client(base_url=base, headers=headers, timeout=10) as client:
        assert client.get("/health/live").status_code == 200
        deadline = time.monotonic() + 120
        last = {}
        while time.monotonic() < deadline:
            response = client.get("/api/v1/pipeline/status")
            assert response.status_code == 200
            last = response.json()
            counts = last.get("data", {}).get("counts", {})
            if counts.get("events", 0) > 0 and counts.get("metrics", 0) > 0:
                break
            time.sleep(2)
        assert last["meta"]["mode"] == "synthetic"
        assert counts.get("events", 0) > 0 and counts.get("metrics", 0) > 0
        assert last["data"]["processing"]["query_id"].startswith("spark-")
        assert last["data"]["engine"] == "postgresql"
