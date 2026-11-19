"""Request-local snapshots must be coherent without caching later requests."""

from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
import pytest

from data_horizon.api import create_app

pytestmark = [pytest.mark.case("F010-TC10"), pytest.mark.evidence("synthetic", "contract")]
FIELDS = ("mode", "as_of", "freshness_seconds", "stale", "time_basis")
BASE = datetime(2026, 11, 14, 12, tzinfo=timezone.utc)


class RevisionStore:
    def __init__(self):
        self.reads = 0
        self.fail_next = False

    def status_snapshot(self):
        self.reads += 1
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("private storage diagnostic")
        return {
            "mode": "synthetic",
            "as_of": (BASE + timedelta(seconds=self.reads)).isoformat(),
            "freshness_seconds": self.reads * 10,
            "stale": self.reads % 2 == 0,
            "time_basis": f"revision-{self.reads}",
        }

    def pipeline_status(self):
        return {**self.status_snapshot(), "counts": {"events": self.reads}}

    def routes(self):
        return [{"route_id": "1"}]


@pytest.fixture
def snapshot_client(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("API_TOKEN", "snapshot-test-integration-key")
    monkeypatch.delenv("API_TOKEN_FILE", raising=False)
    store = RevisionStore()
    client = TestClient(create_app(store))
    client.headers["Authorization"] = "Bearer snapshot-test-integration-key"
    return store, client


def assert_revision(body, path, revision, reads):
    snapshot = body["data"]
    expected = {
        "mode": "synthetic",
        "as_of": (BASE + timedelta(seconds=revision)).isoformat(),
        "freshness_seconds": revision * 10,
        "stale": revision % 2 == 0,
        "time_basis": f"revision-{revision}",
    }
    # One comparison exposes both redundant reads and mismatched data/meta.
    assert {
        "reads": reads,
        "data": {key: snapshot[key] for key in FIELDS},
        "meta": body["meta"],
    } == {"reads": revision, "data": expected, "meta": {**expected, "stale_after_seconds": 120}}


@pytest.mark.parametrize("path", ["/status", "/pipeline/status"])
def test_one_snapshot_per_response_and_independent_next_request(snapshot_client, path):
    store, client = snapshot_client
    for revision in (1, 2):
        response = client.get("/api/v1" + path)
        assert response.status_code == 200
        assert_revision(response.json(), path, revision, store.reads)


@pytest.mark.parametrize("path", ["/status", "/pipeline/status"])
def test_failed_snapshot_remains_unavailable_and_next_request_can_recover(snapshot_client, path):
    store, client = snapshot_client
    store.fail_next = True
    failed = client.get("/api/v1" + path)
    assert failed.status_code == 503 and store.reads == 1
    assert "private storage diagnostic" not in failed.text
    recovered = client.get("/api/v1" + path)
    assert recovered.status_code == 200
    assert_revision(recovered.json(), path, 2, store.reads)
