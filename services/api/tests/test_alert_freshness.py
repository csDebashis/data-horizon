from datetime import timedelta

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app
from data_horizon.collector import synthetic_static
from data_horizon.contracts import utcnow
from data_horizon.contracts import TransitEvent
from data_horizon.store import Store

pytestmark = [pytest.mark.case("F010-TC09"), pytest.mark.evidence("synthetic", "contract")]


@pytest.fixture
def alerts_client(isolated_data_dir, monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("API_TOKEN", "source-freshness-fixture-key")
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    store = Store()
    store.mode = store.settings.mode = "live"
    synthetic_static(store)
    store.settings.stale_seconds = 45
    now = utcnow()
    store.put_metadata("processing", {"as_of": now.isoformat(), "time_basis": "predicted"})
    store.replace_assets("alerts", [{"id": "retained", "route_ids": ["1"], "description": "Retained notice"}])
    client = TestClient(create_app(store), headers={"Authorization": "Bearer source-freshness-fixture-key"})
    return store, client, now


@pytest.mark.parametrize("condition", ["fresh", "failed", "missing", "future", "expired"])
def test_alert_source_status_is_independent_of_fresh_transit(alerts_client, condition):
    store, client, now = alerts_client
    source = {"status": "ok", "last_success": now.isoformat()}
    if condition == "failed":
        source.update(status="degraded", reason="private-source-error")
    elif condition == "missing":
        source = {}
    elif condition == "future":
        source["last_success"] = (now + timedelta(seconds=60)).isoformat()
    elif condition == "expired":
        source["last_success"] = (now - timedelta(seconds=46)).isoformat()
    store.put_metadata("sources", {"alerts": source})
    response = client.get("/api/v1/alerts?route_id=1")
    assert response.status_code == 200
    data = response.json()
    assert data["meta"]["stale_after_seconds"] == 45
    assert data["meta"]["stale"] is (condition != "fresh")
    assert "private-source-error" not in response.text
    assert data["data"][0]["id"] == "retained"
    if condition != "fresh":
        assert data["data"][0]["last_known"] is True and data["meta"]["reason"]


def test_native_selectors_retain_route_scope_and_supported_context(alerts_client):
    store, client, now = alerts_client
    store.replace_static(
        [{"route_id": route, "agency_id": "MTA", "route_type": "1"} for route in ("1", "2")],
        [{"stop_id": stop, "route_ids": ["1", "2"]} for stop in ("A", "B")],
        [],
    )
    store.put_metadata("sources", {"alerts": {"status": "ok", "last_success": now.isoformat()}})
    store.apply_batch(
        [
            TransitEvent(
                source="synthetic",
                route_id="1",
                stop_id="A",
                trip_id="run-A",
                direction_id="0",
                event_time=now,
                predicted_time=now + timedelta(minutes=5),
                time_basis="predicted",
            ).model_dump(mode="json")
        ],
        batch_id="context",
    )
    store.replace_assets(
        "alerts",
        [
            {
                "id": "paired",
                "informed_entities": [{"route_id": "1", "stop_id": "A"}, {"route_id": "2", "stop_id": "B"}],
            },
            {"id": "station-A", "informed_entities": [{"stop_id": "A"}]},
            {"id": "agency", "informed_entities": [{"agency_id": "MTA", "route_type": 1}]},
            {"id": "direction", "informed_entities": [{"route_id": "1", "direction_id": 0}]},
            {"id": "trip", "informed_entities": [{"trip": {"trip_id": "run-A"}}]},
            {"id": "unknown", "informed_entities": [{"route_id": "1", "unknown_restriction": "x"}]},
            {"id": "parent-guess", "informed_entities": [{"stop_id": "AN"}]},
        ],
    )
    # A route listing includes native station-scoped notices with their selector
    # intact; direction-scoped notices need an explicit current entity context.
    response = client.get("/api/v1/alerts?route_id=1").json()
    assert {row["id"] for row in response["data"]} == {"paired", "station-A", "agency", "direction"}
    paired = next(row for row in response["data"] if row["id"] == "paired")
    assert paired["informed_entities"] == [
        {"route_id": "1", "stop_id": "A"},
        {"route_id": "2", "stop_id": "B"},
    ]


@pytest.mark.parametrize("mode", ["synthetic", "recorded"])
def test_fixture_context_is_labelled_and_missing_receipt_is_not_invented(alerts_client, mode):
    store, client, now = alerts_client
    store.mode = store.settings.mode = mode
    synthetic_static(store)
    store.put_metadata("processing", {"as_of": now.isoformat(), "time_basis": "predicted"})
    result = client.get("/api/v1/alerts?route_id=1").json()
    assert result["meta"]["mode"] == mode and result["meta"]["stale"] is False
    assert result["meta"]["as_of"] == now.isoformat()
    store.put_metadata("processing", {})
    result = client.get("/api/v1/alerts?route_id=1").json()
    assert result["meta"]["as_of"] is None and result["meta"]["stale"] is True
