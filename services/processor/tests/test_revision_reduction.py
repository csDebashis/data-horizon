"""Repeated forecast revisions are reduced before crossing the Spark input boundary."""
from datetime import datetime, timedelta, timezone
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from data_horizon.contracts import TransitEvent
from data_horizon.processor import aggregate_spark

pytestmark = [pytest.mark.case('F004-COL01', 'F005-COL01'),
              pytest.mark.evidence('synthetic', 'contract')]


def test_only_latest_trip_stop_rows_are_serialised_for_spark(monkeypatch):
    # This unit test stops at parallelize; genuine SQL execution has separate JVM tests.
    pyspark = ModuleType('pyspark')
    sql = ModuleType('pyspark.sql')
    sql.functions = ModuleType('pyspark.sql.functions')
    sql.Window = SimpleNamespace()
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, 'pyspark', pyspark)
    monkeypatch.setitem(sys.modules, 'pyspark.sql', sql)
    now = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
    events = [TransitEvent(source='synthetic', route_id='1', stop_id='A', trip_id=f't{trip}',
        event_time=now - timedelta(seconds=30 - revision), observed_time=now,
        scheduled_time=now - timedelta(seconds=120), time_basis='observed', match_confidence='exact')
        for trip in range(3) for revision in range(30)]
    captured = []
    class CapturedInput(Exception):
        pass
    def parallelize(rows, partitions):
        captured.extend(json.loads(row) for row in rows)
        raise CapturedInput
    spark = SimpleNamespace(read=SimpleNamespace(json=lambda rows: rows),
                            sparkContext=SimpleNamespace(parallelize=parallelize))
    with pytest.raises(CapturedInput):
        aggregate_spark(spark, events, as_of=now)
    assert len(captured) == 3
    assert {row['trip_id'] for row in captured} == {'t0', 't1', 't2'}
    assert {row['event_time'] for row in captured} == {(now - timedelta(seconds=1)).isoformat().replace('+00:00', 'Z')}
