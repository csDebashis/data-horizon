"""Synthetic-only sustained Kafka/Spark/SQL benchmark with measured visibility."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import threading
import time
from datetime import timedelta
from uuid import uuid4
import urllib.request

from .collector import atomic_write, producer_for, publish
from .config import Settings
from .contracts import TransitEvent, utcnow
from .processor import broker_end_offsets
from .store import Store, dumps


def percentile(values, q=0.95):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(len(values) * q) - 1)]


def target_result(
    *, duration, elapsed, sent, rate, latencies, pending, caught_up, http_latencies, http_errors
):
    p95 = percentile(latencies)
    api_p95 = percentile(http_latencies)
    sustained = duration >= 1800 and elapsed >= 1800 and sent >= duration * rate * 0.98
    passed = (
        sustained
        and bool(latencies)
        and p95 <= 90
        and pending == 0
        and caught_up
        and bool(http_latencies)
        and api_p95 <= 1
        and http_errors == 0
    )
    return {
        "status": "Passed" if passed else "Smoke only" if duration < 1800 else "Failed",
        "sustained_duration_met": sustained,
        "sampled_p95_visibility_seconds": p95,
        "api_p95_seconds": api_p95,
        "pending_latency_samples": pending,
        "checkpoint_caught_up": caught_up,
        "http_errors": http_errors,
    }


def run(duration=1800, rate=100, output=None, api_base_url=None):
    import os

    settings = Settings.load()
    if settings.environment == "production":
        raise ValueError("Benchmark refuses production environments")
    if settings.mode != "synthetic":
        raise ValueError("Benchmark requires isolated DATA_MODE=synthetic")
    if not 1 <= duration <= 7200 or not 1 <= rate <= 1000:
        raise ValueError("Benchmark duration/rate outside supported limits")
    if not settings.kafka_servers:
        raise ValueError("Benchmark requires Kafka and the Docker streaming pipeline")
    store = Store(ensure_schema=False)
    if store.engine != "postgresql":
        raise ValueError("Sustained benchmark requires PostgreSQL")
    token = settings.api_token
    base = api_base_url or os.getenv("API_BASE_URL", "http://api:8000")
    run_id = uuid4().hex[:12]
    output = Path(output or settings.data_dir / "reports" / f"benchmark-{run_id}.json")
    producer = producer_for(settings)
    samples = {}
    latencies = []
    api_latencies = []
    http_errors = []
    sizes = []
    lock = threading.Lock()
    finish = threading.Event()
    end_readers = threading.Event()
    started = utcnow()
    start = time.perf_counter()
    sent = 0
    last_log = start

    def observe():
        while not finish.wait(1):
            with lock:
                keys = list(samples)
            if not keys:
                continue
            try:
                with store.connect() as conn:
                    for offset in range(0, len(keys), 500):
                        batch = keys[offset : offset + 500]
                        rows = store.execute(
                            conn,
                            "SELECT event_id FROM transit_event WHERE namespace=? AND event_id IN ("
                            + ",".join("?" for _ in batch)
                            + ")",
                            (settings.mode, *batch),
                        ).fetchall()
                        seen = time.perf_counter()
                        with lock:
                            for row in rows:
                                began = samples.pop(row["event_id"], None)
                                if began is not None:
                                    latencies.append(seen - began)
            except Exception:
                continue  # Database outage appears as pending samples/failed latency target.

    def read_api():
        while not end_readers.is_set():
            before = time.perf_counter()
            try:
                request = urllib.request.Request(
                    base + "/api/v1/network/health",
                    headers={"Authorization": "Bearer " + token} if token else {},
                )
                with urllib.request.urlopen(request, timeout=5) as response:
                    body = json.loads(response.read())
                    if response.status != 200 or body.get("meta", {}).get("mode") != "synthetic":
                        raise ValueError("Unexpected API response")
                with lock:
                    api_latencies.append(time.perf_counter() - before)
            except Exception as exc:
                with lock:
                    http_errors.append(type(exc).__name__)
            end_readers.wait(1)

    observer = threading.Thread(target=observe, daemon=True)
    observer.start()
    readers = [threading.Thread(target=read_api, daemon=True) for _ in range(5)]
    for thread in readers:
        thread.start()
    error = None
    targets = {}
    caught_up = False
    try:
        while time.perf_counter() - start < duration:
            now = utcnow()
            events = []
            for i in range(rate):
                index = sent + i
                route = str(index % 6 + 1)
                stop = ["101N", "120N", "127N", "635N", "142N"][(index // 6) % 5]
                e = TransitEvent(
                    source="synthetic",
                    route_id=route,
                    trip_id=f"benchmark-{run_id}-{index % 360}",
                    stop_id=stop,
                    event_time=now,
                    observed_time=now,
                    scheduled_time=now - timedelta(seconds=60 + (index % 120)),
                    time_basis="observed",
                    match_confidence="exact",
                    scheduled_headway_seconds=300,
                    static_version="synthetic-v1",
                ).model_dump(mode="json")
                events.append(e)
            # One database-visibility observation per publication interval.
            with lock:
                samples[events[0]["event_id"]] = time.perf_counter()
            if len(sizes) < 100:
                sizes.extend(len(dumps(e).encode()) for e in events[:5])
            publish(settings, events, producer)
            sent += len(events)
            if time.perf_counter() - last_log >= 30:
                print(
                    dumps(
                        {
                            "run_id": run_id,
                            "elapsed_seconds": round(time.perf_counter() - start, 1),
                            "acknowledged_events": sent,
                            "pending_samples": len(samples),
                            "sampled_p95_seconds": percentile(latencies),
                        }
                    ),
                    flush=True,
                )
                last_log = time.perf_counter()
            remaining = sent / rate - (time.perf_counter() - start)
            if remaining > 0:
                time.sleep(min(remaining, 1))
        published_elapsed = time.perf_counter() - start
        targets = broker_end_offsets(settings) or {}
        deadline = time.perf_counter() + 180
        while time.perf_counter() < deadline:
            progress = store.get_metadata("streaming", {})
            offsets = progress.get("end_offsets", {})
            caught_up = bool(targets) and all(offsets.get(p, -1) >= v for p, v in targets.items())
            with lock:
                waiting = len(samples)
            if caught_up and waiting == 0:
                break
            time.sleep(2)
    except Exception as exc:
        error = type(exc).__name__
        published_elapsed = time.perf_counter() - start
    finally:
        end_readers.set()
        finish.set()
        observer.join(timeout=6)
        for thread in readers:
            thread.join(timeout=6)
        try:
            producer.close(timeout=10)
        except Exception as exc:
            error = error or type(exc).__name__
    assessment = target_result(
        duration=duration,
        elapsed=published_elapsed,
        sent=sent,
        rate=rate,
        latencies=latencies,
        pending=len(samples),
        caught_up=caught_up,
        http_latencies=api_latencies,
        http_errors=len(http_errors),
    )
    if error:
        assessment["status"] = "Failed"
    report = {
        "run_id": run_id,
        "mode": "synthetic",
        "started_at": started.isoformat(),
        "finished_at": utcnow().isoformat(),
        "requested_duration_seconds": duration,
        "publication_duration_seconds": published_elapsed,
        "target_events_per_second": rate,
        "acknowledged_events": sent,
        "measured_publication_rate": sent / published_elapsed if published_elapsed else None,
        "mean_payload_bytes": statistics.mean(sizes) if sizes else None,
        "visibility_samples": len(latencies),
        "api_readers": 5,
        "api_requests": len(api_latencies) + len(http_errors),
        "broker_target_offsets": targets,
        "last_streaming_progress": store.get_metadata("streaming", {}),
        "error_type": error,
        **assessment,
    }
    atomic_write(output, dumps(report).encode())
    print(dumps(report), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=int, default=1800)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--output")
    parser.add_argument("--api-base-url")
    args = parser.parse_args()
    result = run(args.duration, args.rate, args.output, args.api_base_url)
    raise SystemExit(1 if result["status"] == "Failed" else 0)


if __name__ == "__main__":
    main()
