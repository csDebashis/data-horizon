"""F010 contract/security cases using independent serving fixtures."""
from __future__ import annotations

import copy
from datetime import timedelta
import secrets

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app


class FixtureStore:
    def routes(self):
        return [{"route_id": "1", "route_long_name": "Broadway"}, {"route_id": "2", "route_long_name": "Seventh Avenue"}]

    def stops(self, route_id=None):
        rows = [{"stop_id": "A", "stop_name": "Alpha", "route_ids": ["1"]},
                {"stop_id": "B", "stop_name": "Beta", "route_ids": ["1", "2"]}]
        return [r for r in rows if not route_id or route_id in r["route_ids"]]

    def metrics(self, route_id=None, limit=300, stop_id=None):
        rows = [{"route_id": "1", "stop_id": "A", "delay_mean_seconds": 120, "health_score": 88, "time_basis": "synthetic"},
                {"route_id": "2", "stop_id": "B", "delay_mean_seconds": 360, "health_score": 60, "time_basis": "synthetic"}]
        return [r for r in rows if (not route_id or r["route_id"] == route_id) and (not stop_id or r["stop_id"] == stop_id)][:limit]

    def network_health(self):
        return {"health_score": 74, "route_count": 2}

    def alerts(self):
        return [{"route_id": "1", "description": "Signal issue"}, {"route_ids": ["2"], "description": "Track work"}]

    def anomalies(self):
        return [{"route_id": "2", "stop_id": "B", "anomaly": "headway"}]

    def graph_edges(self):
        return [{"source": "1|0|A", "target": "1|0|B", "edge_type": "travel", "travel_seconds": 120, "weight": 1}]

    def status_snapshot(self):
        return {"mode": "synthetic", "as_of": "2026-11-14T12:00:00Z", "freshness_seconds": 20,
                "stale": False, "time_basis": "synthetic"}

    def pipeline_status(self):
        return {**self.status_snapshot(), "counts": {"events": 10, "metrics": 2, "quarantine": 1, "batches": 2}}


@pytest.fixture
def store():
    return FixtureStore()


def integration_client(store, monkeypatch):
    monkeypatch.setenv("API_TOKEN", "contract-integration-test-key")
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    return TestClient(create_app(store), headers={"Authorization": "Bearer contract-integration-test-key"})


@pytest.fixture
def client(monkeypatch, store):
    monkeypatch.setenv("API_TOKEN", "contract-integration-test-key")
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    monkeypatch.setenv("APP_ENV", "test")
    return TestClient(create_app(store), headers={"Authorization": "Bearer contract-integration-test-key"})


@pytest.mark.parametrize("path", ["/routes", "/stops", "/network/health", "/routes/1/metrics", "/alerts", "/anomalies", "/pipeline/status"])
@pytest.mark.case("F010-TC01")
@pytest.mark.evidence("synthetic", "contract")
def test_F010_TC01_success_envelope(client, path):
    response = client.get("/api/v1" + path)
    assert response.status_code == 200
    assert set(response.json()) == {"data", "meta"}
    expected_meta = {"mode", "as_of", "freshness_seconds", "stale", "time_basis", "stale_after_seconds"}
    if path == "/alerts":
        expected_meta.add("reason")
    assert set(response.json()["meta"]) == expected_meta
    assert response.json()["meta"]["mode"] == "synthetic"
    assert response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.parametrize("path", [
    "/status", "/routes", "/stops", "/network/health", "/routes/1/metrics",
    "/routes/1/coverage", "/anomalies", "/pipeline/status",
])
@pytest.mark.parametrize("condition", ["fresh", "expired", "missing"])
def test_shared_result_metadata_uses_configured_freshness_budget(
    isolated_data_dir, monkeypatch, path, condition,
):
    from data_horizon.collector import synthetic_static
    from data_horizon.contracts import utcnow
    from data_horizon.store import Store

    monkeypatch.setenv("APP_ENV", "test")
    store = Store()
    store.settings.stale_seconds = 45
    synthetic_static(store)
    now = utcnow()
    monkeypatch.setattr("data_horizon.store.utcnow", lambda: now)
    source_time = None if condition == "missing" else (
        now - timedelta(seconds=20 if condition == "fresh" else 46)
    ).isoformat()
    store.put_metadata("processing", {} if source_time is None else {
        "as_of": source_time, "time_basis": "predicted",
    })
    response = integration_client(store, monkeypatch).get("/api/v1" + path)
    assert response.status_code == 200
    meta = response.json()["meta"]
    assert meta["stale_after_seconds"] == 45
    assert meta["stale"] is (condition != "fresh")
    assert meta["as_of"] == source_time
    assert meta["freshness_seconds"] == (
        None if condition == "missing" else 20 if condition == "fresh" else 46
    )


@pytest.mark.case("F010-TC01")
@pytest.mark.evidence("synthetic", "contract")
def test_F010_TC01_operational_metrics(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "data_horizon_events_total 10" in response.text
    assert "data_horizon_source_stale 0" in response.text


@pytest.mark.case("F010-TC01")
@pytest.mark.evidence("synthetic", "contract")
def test_F010_TC01_streaming_measurements_are_real_numeric_gauges(client, store):
    original = store.pipeline_status
    store.pipeline_status = lambda: {**original(), "streaming": {"input_rows_per_second": 100.5,
        "processed_rows_per_second": 125.75, "batch_duration_ms": 980, "kafka_lag": 12}}
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "data_horizon_input_rows_per_second 100.5" in response.text
    assert "data_horizon_processed_rows_per_second 125.75" in response.text
    assert "data_horizon_batch_duration_ms 980.0" in response.text
    assert "data_horizon_kafka_lag 12.0" in response.text
    assert "# TYPE data_horizon_kafka_lag gauge" in response.text


@pytest.mark.case("F010-TC01")
@pytest.mark.evidence("synthetic", "contract")
@pytest.mark.parametrize("missing", [None, float("nan"), float("inf"), "invalid"])
def test_F010_TC01_unavailable_streaming_measurements_are_omitted(client, store, missing):
    original = store.pipeline_status
    store.pipeline_status = lambda: {**original(), "streaming": {"input_rows_per_second": missing,
        "processed_rows_per_second": missing, "batch_duration_ms": missing, "kafka_lag": missing}}
    response = client.get("/metrics")
    assert response.status_code == 200
    for field in ("input_rows_per_second", "processed_rows_per_second", "batch_duration_ms", "kafka_lag"):
        assert f"data_horizon_{field}" not in response.text


@pytest.mark.case("F010-TC02")
@pytest.mark.evidence("synthetic", "integration")
def test_F010_TC02_filters_and_bounded_pagination(client):
    rows = client.get("/api/v1/routes/1/metrics").json()["data"]
    assert [r["route_id"] for r in rows] == ["1"]
    assert client.get("/api/v1/routes?limit=1&offset=1").json()["data"][0]["route_id"] == "2"
    assert client.get("/api/v1/alerts?route_id=2").json()["data"] == [{"route_ids": ["2"], "description": "Track work", "stale": True, "last_known": True}]
    assert client.get("/api/v1/anomalies?route_id=1").json()["data"] == []


@pytest.mark.case("F010-TC02")
@pytest.mark.evidence("synthetic", "integration")
def test_F010_TC02_stop_filter(client):
    assert client.get("/api/v1/routes/1/metrics?stop_id=A").json()["data"][0]["stop_id"] == "A"
    assert client.get("/api/v1/routes/1/metrics?stop_id=MISSING").status_code == 404


@pytest.mark.parametrize("path", ["/routes?limit=0", "/routes?limit=1001", "/routes?offset=-1", "/stops?route_id=1%27OR%201=1", "/routes/1/metrics?limit=2001", "/alerts?limit=-1"])
@pytest.mark.case("F010-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC03_unsafe_or_unbounded_queries_rejected(client, path):
    assert client.get("/api/v1" + path).status_code == 422


@pytest.mark.parametrize("change", [{"delay_seconds": -1}, {"delay_seconds": 7201}, {"horizon_minutes": 0},
                                     {"horizon_minutes": 61}, {"limit": 101}, {"extra_field": True},
                                     {"route_id": "1' OR 1=1"}])
@pytest.mark.case("F010-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC03_whatif_body_validation(client, change):
    body = {"route_id": "1", "stop_id": "A", "delay_seconds": 600, **change}
    assert client.post("/api/v1/what-if", json=body).status_code == 422


@pytest.mark.case("F010-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC03_large_request_rejected(client):
    response = client.post("/api/v1/what-if", content="x" * 16385, headers={"Content-Type": "application/json"})
    assert response.status_code == 413


@pytest.mark.case("F010-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC04_bearer_authentication_and_redaction(monkeypatch, store, caplog):
    token = secrets.token_urlsafe(32)
    monkeypatch.setenv("API_TOKEN", token)
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    monkeypatch.setenv("APP_ENV", "test")
    client = TestClient(create_app(store))
    for path in ["/api/v1/routes", "/metrics"]:
        assert client.get(path).status_code == 401
        assert client.get(path, headers={"Authorization": "Bearer wrong"}).status_code == 401
        assert client.get(path, headers={"Authorization": f"Bearer {token}"}).status_code == 200
    assert client.get("/health/live").status_code == 200
    assert token not in caplog.text


@pytest.mark.case("F010-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC04_secret_file_precedence(monkeypatch, tmp_path, store):
    token = secrets.token_urlsafe(32)
    filename = tmp_path / "api_token"
    filename.write_text(token + "\n")
    monkeypatch.setenv("API_TOKEN_FILE", str(filename))
    monkeypatch.setenv("API_TOKEN", "ignored")
    monkeypatch.setenv("APP_ENV", "test")
    client = TestClient(create_app(store))
    assert client.get("/api/v1/routes", headers={"Authorization": f"Bearer {token}"}).status_code == 200
    assert client.get("/api/v1/routes", headers={"Authorization": "Bearer ignored"}).status_code == 401


@pytest.mark.case("F010-TC04")
@pytest.mark.evidence("synthetic", "security")
def test_F010_TC04_production_requires_token_and_hides_docs(monkeypatch, store):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    with pytest.raises(RuntimeError, match="requires"):
        create_app(store)
    monkeypatch.setenv("API_TOKEN", "too-short")
    with pytest.raises(RuntimeError, match="32 characters"):
        create_app(store)
    monkeypatch.setenv("API_TOKEN", secrets.token_urlsafe(32))
    monkeypatch.setenv("PUBLIC_API_TOKEN", secrets.token_urlsafe(32))
    monkeypatch.setenv("DASHBOARD_PUBLIC_URL", "https://transit.example.com")
    monkeypatch.setenv("PUBLIC_API_BASE_URL", "https://transit.example.com")
    client = TestClient(create_app(store))
    assert client.get("/docs").status_code == 404
    assert client.get("/openapi.json").status_code == 404


@pytest.mark.case("F010-TC03")
@pytest.mark.evidence("synthetic", "security")
@pytest.mark.parametrize("path", ["/routes?limti=5", "/stops?route=1", "/network/health?unexpected=1", "/routes/1/metrics?stop=A"])
def test_F010_TC03_unknown_query_parameters_rejected(client, path):
    response = client.get("/api/v1" + path)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "UNSUPPORTED_QUERY_PARAMETER"


@pytest.mark.case("F010-TC05")
@pytest.mark.evidence("synthetic", "contract")
def test_F010_TC05_unknown_entities_and_empty_collection(client):
    assert client.get("/api/v1/routes/UNKNOWN/metrics").status_code == 404
    assert client.get("/api/v1/stops?route_id=UNKNOWN").status_code == 404
    assert client.get("/api/v1/anomalies?route_id=1").json()["data"] == []
    assert client.post("/api/v1/what-if", json={"route_id": "1", "stop_id": "MISSING", "delay_seconds": 600}).status_code == 404


@pytest.mark.case("F010-TC06")
@pytest.mark.evidence("synthetic", "recovery")
def test_F010_TC06_dependency_failure_is_safe(monkeypatch, store):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    def unavailable():
        raise ConnectionError("private-database-password")
    store.status_snapshot = unavailable
    client = integration_client(store, monkeypatch)
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 503
    response = client.get("/api/v1/routes")
    assert response.status_code == 503
    assert "private-database-password" not in response.text


@pytest.mark.case("F010-TC01")
@pytest.mark.evidence("synthetic", "contract")
def test_F010_TC01_readonly_whatif_contract(client, store):
    before = copy.deepcopy(store.graph_edges())
    response = client.post("/api/v1/what-if", json={"route_id": "1", "stop_id": "A", "delay_seconds": 600})
    assert response.status_code == 200
    result = response.json()["data"]
    assert result["horizon_minutes"] == 30
    assert result["affected_stations"][0]["stop_id"] == "B"
    assert result["affected_stations"][0]["estimated_delay_seconds"] < 600
    assert store.graph_edges() == before


@pytest.mark.case("F010-TC02")
@pytest.mark.evidence("synthetic", "integration")
def test_F010_TC02_actual_store_integration(monkeypatch, tmp_path):
    """Verify HTTP filtering and values against real SQL persistence, without a mock store."""
    from data_horizon.contracts import TransitEvent, utcnow
    from data_horizon.store import Store
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATA_MODE", "synthetic")
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    monkeypatch.delenv("DATABASE_URL_FILE", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    backend = Store(data_dir=tmp_path, database_url="")
    backend.replace_static([{"route_id": "1"}, {"route_id": "2"}],
                           [{"stop_id": "A", "route_ids": ["1"]}, {"stop_id": "B", "route_ids": ["2"]}], [])
    now = utcnow()
    events = []
    for route, stop in [("1", "A"), ("2", "B")]:
        for index in range(2):
            arrival = now - timedelta(minutes=4 - 2 * index)
            events.append(TransitEvent(source="independent-unit-fixture", route_id=route, stop_id=stop, trip_id=f"{route}-{index}",
                                      event_time=now, scheduled_time=arrival - timedelta(seconds=120), observed_time=arrival,
                                      time_basis="observed", match_confidence="exact", scheduled_headway_seconds=120))
    backend.apply_batch(events, query_id="api-contract", batch_id="0", as_of=now)
    client = integration_client(backend, monkeypatch)
    response = client.get("/api/v1/routes/1/metrics")
    assert response.status_code == 200
    rows = response.json()["data"]
    assert rows and {row["route_id"] for row in rows} == {"1"}
    assert rows[0]["delay_mean_seconds"] == 120
    assert rows[0]["headway_seconds"] == 120
    assert response.json()["meta"]["mode"] == "synthetic"
    assert client.get("/health/ready").status_code == 200


@pytest.mark.case("F010-TC02")
@pytest.mark.evidence("synthetic", "integration")
def test_F010_TC02_quiet_stop_is_not_hidden_by_limit(monkeypatch, tmp_path):
    """CHG-0002: filtering happens before LIMIT, despite a busy neighbouring stop."""
    from data_horizon.contracts import utcnow
    from data_horizon.store import Store, dumps
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("DATA_MODE", "synthetic")
    for name in ("API_TOKEN", "API_TOKEN_FILE", "DATABASE_URL", "DATABASE_URL_FILE"):
        monkeypatch.delenv(name, raising=False)
    backend = Store(data_dir=tmp_path, database_url="")
    backend.replace_static([{"route_id": "1"}], [{"stop_id": "A", "route_ids": ["1"]}, {"stop_id": "B", "route_ids": ["1"]}], [])
    now = utcnow()
    with backend.connect() as connection:
        for index in range(2100):
            row = {"route_id": "1", "direction_id": "0", "stop_id": "A", "window_start": now.isoformat(), "delay_mean_seconds": 1}
            backend.execute(connection, "INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)",
                            ("synthetic", f"busy-{index}", "1", "A", "0", row["window_start"], dumps(row)))
        quiet = {"route_id": "1", "direction_id": "0", "stop_id": "B", "window_start": (now - timedelta(hours=1)).isoformat(), "delay_mean_seconds": 42}
        backend.execute(connection, "INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?)",
                        ("synthetic", "quiet", "1", "B", "0", quiet["window_start"], dumps(quiet)))
    response = integration_client(backend, monkeypatch).get("/api/v1/routes/1/metrics?stop_id=B&limit=1")
    assert response.status_code == 200
    assert response.json()["data"] == [quiet]
