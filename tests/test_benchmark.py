import pytest

from data_horizon.benchmark import percentile, run, target_result
from data_horizon.collector import replay
from data_horizon.config import Settings


def test_short_measurement_never_claims_sustained_pass():
    report = target_result(
        duration=5,
        elapsed=5,
        sent=500,
        rate=100,
        latencies=[1, 2],
        pending=0,
        caught_up=True,
        http_latencies=[0.1],
        http_errors=0,
    )
    assert report["status"] == "Smoke only" and not report["sustained_duration_met"]


@pytest.mark.parametrize(
    "patch",
    [
        {"latencies": [91]},
        {"pending": 1},
        {"caught_up": False},
        {"http_errors": 1},
        {"http_latencies": [1.1]},
        {"sent": 100},
    ],
)
def test_sustained_failure_gates(patch):
    fields = {
        "duration": 1800,
        "elapsed": 1800,
        "sent": 180000,
        "rate": 100,
        "latencies": [30],
        "pending": 0,
        "caught_up": True,
        "http_latencies": [0.1],
        "http_errors": 0,
    }
    assert target_result(**{**fields, **patch})["status"] == "Failed"


def test_measured_sustained_success_and_nearest_rank():
    assert percentile(list(range(100))) == 94
    assert (
        target_result(
            duration=1800,
            elapsed=1800,
            sent=180000,
            rate=100,
            latencies=[30],
            pending=0,
            caught_up=True,
            http_latencies=[0.1],
            http_errors=0,
        )["status"]
        == "Passed"
    )


def test_benchmark_refuses_live(monkeypatch):
    monkeypatch.setenv("DATA_MODE", "live")
    with pytest.raises(ValueError, match="synthetic"):
        run(duration=1)


def test_replay_settings_cannot_write_into_live_store(isolated_data_dir, monkeypatch, tmp_path):
    p = tmp_path / "empty.jsonl"
    p.write_text("")
    settings = Settings(mode="recorded", data_dir=isolated_data_dir)
    monkeypatch.setenv("DATA_MODE", "live")
    with pytest.raises(ValueError, match="namespaces"):
        replay(p, settings)
