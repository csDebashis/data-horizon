"""Processor-owned bounded backfill and spatial job orchestration."""

from __future__ import annotations

import hashlib
import json
from datetime import timedelta
from pathlib import Path

from .contracts import TransitEvent, timestamp


def load_records(path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("Backfill input must be one file up to64MiB; split larger corrections")
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        rows = []
        for batch in pq.ParquetFile(path).iter_batches(batch_size=10000):
            rows.extend(batch.to_pylist())
            if len(rows) > 100000:
                raise ValueError("Backfill input exceeds100000records; split larger corrections")
        return rows
    rows = []
    with path.open() as stream:
        for line in stream:
            if line.strip():
                rows.append(json.loads(line))
            if len(rows) > 100000:
                raise ValueError("Backfill input exceeds100000records; split larger corrections")
    return rows


def backfill_windows(records):
    return sorted(
        {timestamp(e["event_time"]).replace(second=0, microsecond=0) + timedelta(minutes=1) for e in records}
    )


def run_backfill(args, settings, store):
    from .processor import create_spark, aggregate_spark

    records = []
    for raw in load_records(args.input):
        try:
            records.append(TransitEvent.model_validate(raw).model_dump(mode="json"))
        except ValueError:
            store.quarantine(raw, "invalid_backfill_input")
    if not records:
        raise ValueError("Backfill input is empty")
    digest = hashlib.sha256(Path(args.input).read_bytes()).hexdigest()
    # Caller supplies a frozen complete reference interval; corrections stay bounded to its windows.
    windows = backfill_windows(records)
    result = []
    spark = create_spark("Data Horizon bounded historical correction")
    try:
        for window in windows:
            result.append(
                store.apply_batch(
                    records,
                    query_id="backfill-" + digest,
                    batch_id=window.isoformat(),
                    allow_late=True,
                    as_of=window,
                    aggregate_fn=lambda events, **kwargs: aggregate_spark(spark, events, **kwargs),
                )
            )
    finally:
        spark.stop()
    store.put_metadata(
        "last_backfill",
        {"input_sha256": digest, "windows": [w.isoformat() for w in windows], "count": len(records)},
    )
    return result


def run_graph(args, settings, store):
    from .processor import create_spark, validate_spatial

    spark = create_spark("Data Horizon spatial validation")
    try:
        result = {"edges": len(store.graph_edges()), "geometry": validate_spatial(spark, store.stops())}
    finally:
        spark.stop()
    return result
