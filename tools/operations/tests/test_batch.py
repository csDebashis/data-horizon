from datetime import timedelta
import json
import os
import sys

import pytest

from data_horizon.cli import backfill_windows, load_records, main
from data_horizon.config import configure_spark_python
from data_horizon.contracts import TransitEvent, utcnow
from data_horizon.store import Store


def test_worker_interpreter_defaults_and_operator_override(monkeypatch):
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)
    configure_spark_python()
    assert os.environ["PYSPARK_PYTHON"] == sys.executable == os.environ["PYSPARK_DRIVER_PYTHON"]
    monkeypatch.setenv("PYSPARK_PYTHON", "/opt/cluster/python")
    configure_spark_python()
    assert os.environ["PYSPARK_PYTHON"] == "/opt/cluster/python"


def test_last_partial_minute_is_included():
    instant = utcnow().replace(second=30, microsecond=0)
    windows = backfill_windows([{"event_time": instant.isoformat()}])
    assert windows == [instant.replace(second=0) + timedelta(minutes=1)]
    assert windows[0] > instant


def test_backfill_input_is_bounded(tmp_path):
    path = tmp_path / "large.jsonl"
    with path.open("wb") as stream:
        stream.truncate(64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="64MiB"):
        load_records(path)
    path.write_text("{}\n" * 100001)
    with pytest.raises(ValueError, match="100000"):
        load_records(path)


@pytest.mark.spark
@pytest.mark.feature("F005")
def test_real_spark_backfill_publishes_last_minute(isolated_data_dir, tmp_path):
    if os.getenv("RUN_SPARK") != "1":
        pytest.skip("BLOCKED: RUN_SPARK=1 and documented Java runtime required")
    instant = utcnow().replace(second=30, microsecond=0) - timedelta(minutes=2)
    event = TransitEvent(
        source="synthetic",
        route_id="1",
        trip_id="backfill-t1",
        stop_id="A",
        event_time=instant,
        observed_time=instant,
        scheduled_time=instant - timedelta(seconds=120),
        time_basis="observed",
        match_confidence="exact",
    ).model_dump(mode="json")
    path = tmp_path / "frozen.jsonl"
    path.write_text(json.dumps(event) + "\n")
    main(["backfill", "--input", str(path)])
    rows = Store().metrics(route_id="1")
    assert len(rows) == 1 and rows[0]["delay_mean_seconds"] == 120
    assert rows[0]["window_start"] == backfill_windows([event])[0].isoformat()
    assert Store().get_metadata("last_backfill")["count"] == 1
