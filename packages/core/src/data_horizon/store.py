"""Namespace-isolated serving store with atomic, retry-safe micro-batches.

PostgreSQL is the Docker backend. SQLite is an explicitly reported local/test backend.
All values use bound parameters. Only fixed, internal table identifiers enter SQL.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

from .analytics import aggregate
from .config import Settings
from .contracts import TransitEvent, timestamp, utcnow
from .health_retention import (
    HISTORY_LIMIT, RETENTION_SECONDS, current_provenance, eligible_entities, entity_key, retained_reading,
    retained_summary, scope_for,
)


def dumps(value):
    return json.dumps(value, default=str, sort_keys=True, allow_nan=False)


class Store:
    def __init__(self, data_dir=None, database_url=None, ensure_schema=None):
        self.settings = Settings.load()
        self.data_dir = Path(data_dir or self.settings.data_dir)
        self.url = self.settings.database_url if database_url is None else database_url
        self.mode = self.settings.mode
        self.engine = "postgresql" if self.url.startswith("postgresql") else "sqlite-local"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        if self.url and self.engine != "postgresql" and not self.url.startswith("sqlite:///"):
            raise ValueError("Unsupported DATABASE_URL")
        self.sqlite_path = (
            self.url.removeprefix("sqlite:///")
            if self.url.startswith("sqlite:///")
            else str(self.data_dir / "horizon.db")
        )
        if ensure_schema is None:
            ensure_schema = os.getenv("AUTO_INITIALIZE_SCHEMA", "true").lower() == "true"
        if ensure_schema:
            self.initialize_schema()

    @contextmanager
    def connect(self):
        if self.engine == "postgresql":
            import psycopg
            from psycopg.rows import dict_row

            conn = psycopg.connect(self.url, connect_timeout=5, row_factory=dict_row)
        else:
            conn = sqlite3.connect(self.sqlite_path, timeout=20)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def execute(self, conn, sql, args=()):
        return conn.execute(sql.replace("?", "%s") if self.engine == "postgresql" else sql, args)

    def initialize_schema(self):
        """Create the current baseline; existing older schemas require a reset."""
        statements = [
            "CREATE TABLE IF NOT EXISTS metadata (namespace TEXT NOT NULL,key TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,key))",
            "CREATE TABLE IF NOT EXISTS assets (namespace TEXT NOT NULL,kind TEXT NOT NULL,item_key TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,kind,item_key))",
            "CREATE TABLE IF NOT EXISTS transit_event (namespace TEXT NOT NULL,event_id TEXT NOT NULL,event_epoch DOUBLE PRECISION NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,event_id))",
            "CREATE INDEX IF NOT EXISTS events_by_time ON transit_event(namespace,event_epoch)",
            "CREATE TABLE IF NOT EXISTS analytics_result (namespace TEXT NOT NULL,item_key TEXT NOT NULL,route_id TEXT NOT NULL,stop_id TEXT NOT NULL,direction_id TEXT NOT NULL,window_start TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,item_key))",
            "CREATE INDEX IF NOT EXISTS analytics_by_time ON analytics_result(namespace,window_start)",
            "CREATE TABLE IF NOT EXISTS processed_batch (namespace TEXT NOT NULL,query_id TEXT NOT NULL,batch_id TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,query_id,batch_id))",
            "CREATE TABLE IF NOT EXISTS quarantine (namespace TEXT NOT NULL,item_id TEXT NOT NULL,created_at TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(namespace,item_id))",
        ]
        with self.connect() as conn:
            if self.engine == "postgresql":
                lock = int.from_bytes(hashlib.sha256(b"data-horizon:analytics-schema-baseline").digest()[:8], "big", signed=True)
                self.execute(conn, "SELECT pg_advisory_xact_lock(?)", (lock,))
            elif not conn.in_transaction:
                self.execute(conn, "BEGIN IMMEDIATE")
            for statement in statements:
                self.execute(conn, statement)
            self.execute(
                conn,
                "CREATE INDEX IF NOT EXISTS metrics_by_entity ON analytics_result(namespace,route_id,stop_id,direction_id,window_start)",
            )

    def _put(self, conn, key, value):
        self.execute(
            conn,
            "INSERT INTO metadata(namespace,key,payload) VALUES(?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET payload=excluded.payload",
            (self.mode, key, dumps(value)),
        )

    def put_metadata(self, key, value):
        with self.connect() as conn:
            self._put(conn, key, value)

    def _get(self, conn, key, default=None):
        row = self.execute(
            conn, "SELECT payload FROM metadata WHERE namespace=? AND key=?", (self.mode, key)
        ).fetchone()
        return json.loads(row["payload"]) if row else default

    def get_metadata(self, key, default=None):
        with self.connect() as conn:
            return self._get(conn, key, default)

    def replace_static(self, routes, stops, edges, *, metadata=None):
        with self.connect() as conn:
            for kind, items, identifier in [
                ("routes", routes, "route_id"),
                ("stops", stops, "stop_id"),
                ("edges", edges, None),
            ]:
                self.execute(conn, "DELETE FROM assets WHERE namespace=? AND kind=?", (self.mode, kind))
                for i, item in enumerate(items):
                    key = (
                        str(item[identifier])
                        if identifier
                        else f"{item['source']}>{item['target']}:{item.get('edge_type', 'travel')}"
                    )
                    self.execute(
                        conn,
                        "INSERT INTO assets(namespace,kind,item_key,payload) VALUES(?,?,?,?)",
                        (self.mode, kind, key, dumps(item)),
                    )
            if metadata is not None:
                self._put(conn, "static", metadata)

    def replace_assets(self, kind, items, id_field="id"):
        if kind != "alerts":
            raise ValueError("Unsupported asset type")
        with self.connect() as conn:
            self.execute(conn, "DELETE FROM assets WHERE namespace=? AND kind=?", (self.mode, kind))
            for index, item in enumerate(items):
                self.execute(
                    conn,
                    "INSERT INTO assets(namespace,kind,item_key,payload) VALUES(?,?,?,?)",
                    (self.mode, kind, str(item.get(id_field, index)), dumps(item)),
                )

    def _assets(self, conn, kind):
        return [
            json.loads(r["payload"])
            for r in self.execute(
                conn,
                "SELECT payload FROM assets WHERE namespace=? AND kind=? ORDER BY item_key LIMIT 10000",
                (self.mode, kind),
            ).fetchall()
        ]

    def assets(self, kind):
        with self.connect() as conn:
            return self._assets(conn, kind)

    def routes(self):
        return self.assets("routes")

    def stops(self, route_id=None):
        return [s for s in self.assets("stops") if not route_id or route_id in s.get("route_ids", [])]

    def graph_edges(self):
        return self.assets("edges")

    def alerts(self):
        from .analytics import alert_active

        now = utcnow()
        return [a for a in self.assets("alerts") if alert_active(a, now)]

    def _quarantine(self, conn, raw, reason):
        item = {"reason": reason, "raw": raw, "time": utcnow().isoformat()}
        key = hashlib.sha256(dumps({"raw": raw, "reason": reason}).encode()).hexdigest()
        self.execute(
            conn,
            "INSERT INTO quarantine(namespace,item_id,created_at,payload) VALUES(?,?,?,?) ON CONFLICT(namespace,item_id) DO NOTHING",
            (self.mode, key, item["time"], dumps(item)),
        )

    def quarantine(self, raw, reason):
        with self.connect() as conn:
            self._quarantine(conn, raw, reason)

    def events(self, limit=100000):
        with self.connect() as conn:
            return [
                json.loads(r["payload"])
                for r in self.execute(
                    conn,
                    "SELECT payload FROM transit_event WHERE namespace=? ORDER BY event_epoch DESC LIMIT ?",
                    (self.mode, min(max(int(limit), 1), 100000)),
                ).fetchall()
            ]

    def apply_batch(
        self,
        events,
        query_id="local-v1",
        batch_id="0",
        mode=None,
        aggregate_fn=None,
        as_of=None,
        allow_late=False,
    ):
        if mode is not None and mode != self.mode:
            raise ValueError("Batch namespace differs from store DATA_MODE")
        wall_clock = utcnow()
        try:
            future_skew = float(os.getenv("MAX_FUTURE_SKEW_SECONDS", "120"))
            if not math.isfinite(future_skew) or future_skew < 0:
                raise ValueError
            future_limit = wall_clock + timedelta(seconds=future_skew)
        except (ValueError, OverflowError) as exc:
            raise ValueError("MAX_FUTURE_SKEW_SECONDS must be finite, non-negative seconds") from exc
        result = {"processed": 0, "duplicates": 0, "invalid": 0, "late": 0, "batch_replayed": False}
        quality_sources = {}
        def quality_outcome(raw, outcome):
            source = raw.source if isinstance(raw, TransitEvent) else raw.get("source") if isinstance(raw, dict) else None
            source = source if source in {"mta-subway", "mta-vehicle-status", "synthetic", "recorded"} else "other"
            counts = quality_sources.setdefault(source, {"processed": 0, "duplicates": 0, "invalid": 0, "late": 0})
            counts[outcome] += 1
        with self.connect() as conn:
            # Serialise this namespace's counter read/update with its batch commit.
            # A replay returns before counters change, and a failed sink rolls back both.
            if self.engine == "postgresql":
                key = int.from_bytes(hashlib.sha256((self.mode + ":quality-v1").encode()).digest()[:8], "big", signed=True)
                self.execute(conn, "SELECT pg_advisory_xact_lock(?)", (key,))
            elif not conn.in_transaction:
                self.execute(conn, "BEGIN IMMEDIATE")
            if self.execute(
                conn,
                "SELECT payload FROM processed_batch WHERE namespace=? AND query_id=? AND batch_id=?",
                (self.mode, query_id, str(batch_id)),
            ).fetchone():
                return {**result, "batch_replayed": True}
            previous = self._get(conn, "processing", {})
            previous_max = timestamp(previous["as_of"]) if previous.get("as_of") else None
            maximum = previous_max
            for raw in events:
                try:
                    event = raw if isinstance(raw, TransitEvent) else TransitEvent.model_validate(raw)
                except (ValueError, TypeError):
                    self._quarantine(conn, raw, "invalid_schema")
                    result["invalid"] += 1
                    quality_outcome(raw, "invalid")
                    continue
                if event.city != "nyc" or event.route_id not in self.settings.routes:
                    self._quarantine(conn, event.model_dump(mode="json"), "outside_route_scope")
                    result["invalid"] += 1
                    quality_outcome(raw, "invalid")
                    continue
                if event.event_time > future_limit:
                    self._quarantine(conn, event.model_dump(mode="json"), "future_event_time")
                    result["invalid"] += 1
                    quality_outcome(raw, "invalid")
                    continue
                if (
                    previous_max
                    and event.event_time < previous_max - timedelta(seconds=self.settings.lateness_seconds)
                    and not allow_late
                ):
                    self._quarantine(conn, event.model_dump(mode="json"), "beyond_watermark")
                    result["late"] += 1
                    quality_outcome(raw, "late")
                    continue
                data = event.model_dump(mode="json")
                cur = self.execute(
                    conn,
                    "INSERT INTO transit_event(namespace,event_id,event_epoch,payload) VALUES(?,?,?,?) ON CONFLICT(namespace,event_id) DO NOTHING",
                    (self.mode, event.event_id, event.event_time.timestamp(), dumps(data)),
                )
                result["processed" if cur.rowcount else "duplicates"] += 1
                quality_outcome(raw, "processed" if cur.rowcount else "duplicates")
                maximum = max(maximum, event.event_time) if maximum else event.event_time
            # A live feature snapshot describes the current decision time.
            # Source freshness is kept separately on each resulting entity.
            clock = (
                timestamp(as_of) if as_of else wall_clock if self.mode == "live" else maximum or wall_clock
            )
            rows = [
                json.loads(r["payload"])
                for r in self.execute(
                    conn,
                    "SELECT payload FROM transit_event WHERE namespace=? AND event_epoch>? AND event_epoch<=? ORDER BY event_epoch",
                    (self.mode, (clock - timedelta(minutes=30)).timestamp(), clock.timestamp()),
                ).fetchall()
            ]
            alerts = self._assets(conn, "alerts")
            weather = self._get(conn, "weather", {})
            sources = self._get(conn, "sources", {})
            route_context = {route["route_id"]: route for route in self._assets(conn, "routes")}
            static = self._get(conn, "static", {})
            eligible = eligible_entities(rows, clock, static.get("static_version"))
            metrics = (aggregate_fn or aggregate)(
                rows, as_of=clock, alerts=alerts, weather=weather, route_context=route_context
            )
            for metric in metrics:
                # A callback cannot authorise retention using active metadata alone.
                metric.pop("health_scope", None)
                health_scope = (scope_for(self.settings, self.mode, static, sources, metric)
                                if entity_key(metric) in eligible else None)
                if health_scope is not None:
                    metric["health_scope"] = health_scope
                if self.mode == "live" and not self._alerts_available(sources, wall_clock):
                    metric.update(
                        health_score=None,
                        health_level="unknown",
                        reason="A fresh successful service-alert observation is required",
                    )
                key = "|".join(
                    str(metric[k]) for k in ("route_id", "direction_id", "stop_id", "window_start")
                )
                self.execute(
                    conn,
                    "INSERT INTO analytics_result(namespace,item_key,route_id,stop_id,direction_id,window_start,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(namespace,item_key) DO UPDATE SET payload=excluded.payload",
                    (
                        self.mode,
                        key,
                        metric["route_id"],
                        metric["stop_id"],
                        metric["direction_id"],
                        metric["window_start"],
                        dumps(metric),
                    ),
                )
            processing = {
                "as_of": maximum.isoformat() if maximum else None,
                "processed_at": utcnow().isoformat(),
                "query_id": query_id,
                "batch_id": str(batch_id),
                "last_batch": result,
                "feature_version": "v1",
                "time_basis": next(iter({m.get("time_basis", "unavailable") for m in metrics}))
                if len({m.get("time_basis", "unavailable") for m in metrics}) == 1
                else "mixed"
                if metrics
                else "unavailable",
            }
            self._put(conn, "processing", processing)
            self.execute(
                conn,
                "INSERT INTO processed_batch(namespace,query_id,batch_id,payload) VALUES(?,?,?,?)",
                (self.mode, query_id, str(batch_id), dumps(result)),
            )
            minute = wall_clock.replace(second=0, microsecond=0).isoformat()
            quality_key = "quality:v1:" + minute
            bucket = self._get(conn, quality_key, {"minute_start": minute, "sources": {}})
            for source, counts in quality_sources.items():
                target = bucket["sources"].setdefault(source, {"processed": 0, "duplicates": 0, "invalid": 0, "late": 0})
                for outcome, count in counts.items():
                    target[outcome] += count
            self._put(conn, quality_key, bucket)
            if self._get(conn, "quality_coverage") is None:
                self._put(conn, "quality_coverage", {"began_at": wall_clock.isoformat(), "basis": "Committed processing attempts in UTC minute buckets; replayed batch IDs excluded"})
            self.execute(conn, "DELETE FROM metadata WHERE namespace=? AND key LIKE ? AND key<?", (self.mode, "quality:v1:%", "quality:v1:" + (wall_clock - timedelta(hours=48)).replace(second=0, microsecond=0).isoformat()))
            # Bounded serving history; durable raw/Parquet remains the replay source.
            if maximum:
                self.execute(
                    conn,
                    "DELETE FROM transit_event WHERE namespace=? AND event_epoch<?",
                    (self.mode, (maximum - timedelta(minutes=31)).timestamp()),
                )
                self.execute(
                    conn,
                    "DELETE FROM analytics_result WHERE namespace=? AND window_start<?",
                    (self.mode, (maximum - timedelta(days=2)).isoformat()),
                )
        return result

    def metrics(self, route_id=None, limit=1000, stop_id=None):
        clauses = ["namespace=?"]
        params = [self.mode]
        if route_id:
            clauses.append("route_id=?")
            params.append(route_id)
        if stop_id:
            clauses.append("stop_id=?")
            params.append(stop_id)
        params.append(min(max(int(limit), 1), 10000))
        with self.connect() as conn:
            return [
                json.loads(r["payload"])
                for r in self.execute(
                    conn,
                    "SELECT payload FROM analytics_result WHERE "
                    + " AND ".join(clauses)
                    + " ORDER BY window_start DESC,item_key LIMIT ?",
                    tuple(params),
                ).fetchall()
            ]

    def _alerts_available(self, sources, now):
        source = sources.get("alerts", {}) if isinstance(sources, dict) else {}
        success = source.get("last_success") if isinstance(source, dict) else None
        if not success:
            return False
        try:
            age = (now - timestamp(success)).total_seconds()
        except (TypeError, ValueError, AttributeError, OverflowError):
            return False
        return 0 <= age <= self.settings.stale_seconds

    def latest_metrics(self):
        with self.connect() as conn:
            rows = [
                json.loads(r["payload"])
                for r in self.execute(
                    conn,
                    "SELECT winner.payload FROM (SELECT item_key,ROW_NUMBER() OVER("
                    "PARTITION BY route_id,direction_id,stop_id ORDER BY window_start DESC,item_key DESC) "
                    "AS entity_rank FROM analytics_result WHERE namespace=?) AS latest "
                    "JOIN analytics_result AS winner ON winner.namespace=? AND winner.item_key=latest.item_key "
                    "WHERE latest.entity_rank=1",
                    (self.mode, self.mode),
                ).fetchall()
            ]
            sources = self._get(conn, "sources", {})
            static = self._get(conn, "static", {})
        now = utcnow()
        expected_scopes = {}
        for row in rows:
            key = entity_key(row)
            expected = scope_for(self.settings, self.mode, static, sources, row)
            if key is not None:
                expected_scopes[key] = expected
            row["health_retention_eligible"] = retained_reading(
                row, expected, now, self.settings.stale_seconds
            ) is not None
            source_time = row.get("source_as_of") or row.get("as_of") or row.get("window_start")
            try:
                age = (now - timestamp(source_time)).total_seconds() if source_time else None
            except (TypeError, ValueError, AttributeError, OverflowError):
                age = None
            row["source_as_of"] = source_time
            row["freshness_seconds"] = max(0, age) if age is not None else None
            row["stale"] = age > self.settings.stale_seconds if age is not None else None
            if age is None or row["stale"]:
                row.update(
                    health_score=None,
                    health_level="unknown",
                    reason="Entity source data is stale"
                    if age is not None
                    else "Entity source time is unavailable",
                )
            elif self.mode == "live" and not self._alerts_available(sources, now):
                row.update(
                    health_score=None,
                    health_level="unknown",
                    reason="A fresh successful service-alert observation is required",
                )
        waiting = {entity_key(row) for row in rows if row.get("health_score") is None}
        waiting.discard(None)
        retained = {}
        if waiting:
            # Include the minute containing the lower bound; eligibility still
            # uses the exact original source age, never this bucket boundary.
            earliest = (now - timedelta(seconds=RETENTION_SECONDS)).replace(second=0, microsecond=0)
            with self.connect() as conn:
                history = self.execute(
                    conn,
                    "SELECT payload FROM analytics_result WHERE namespace=? AND window_start>=? AND window_start<=? "
                    "ORDER BY window_start DESC,item_key DESC LIMIT ?",
                    (self.mode, earliest.isoformat(), now.isoformat(), HISTORY_LIMIT),
                ).fetchall()
            for item in history:
                candidate = json.loads(item["payload"])
                key = entity_key(candidate)
                if key not in waiting or key in retained:
                    continue
                recent = retained_reading(candidate, expected_scopes.get(key), now, self.settings.stale_seconds)
                if recent is not None:
                    retained[key] = recent
        for row in rows:
            if row.get("health_score") is None:
                recent = retained.get(entity_key(row))
                if recent is not None:
                    recent["reason"] = row.get("reason") or "Waiting for current health inputs."
                row["last_valid_health"] = recent
        return rows

    def anomalies(self):
        return [r for r in self.latest_metrics() if r.get("anomaly") and r.get("stale") is False]


    def status_snapshot(self):
        """Read source and processing state without counting accumulated history."""
        with self.connect() as conn:
            processing = self._get(conn, "processing", {})
            sources = self._get(conn, "sources", {})
            weather = self._get(conn, "weather", {})
            kafka = self._get(conn, "kafka", {})
            collector_error = self._get(conn, "collector_error", {})
            streaming = self._get(conn, "streaming", {})
            mirrors = self._get(conn, "event_mirrors", {})
        as_of = processing.get("as_of")
        freshness = max(0, (utcnow() - timestamp(as_of)).total_seconds()) if as_of else None
        return {
            "mode": self.mode,
            "as_of": as_of,
            "freshness_seconds": freshness,
            "stale": freshness is None or freshness > self.settings.stale_seconds,
            "time_basis": processing.get("time_basis", "unavailable"),
            "engine": self.engine,
            "streaming": streaming,
            "sources": sources,
            "weather": weather,
            "kafka": kafka,
            "event_topics": self.settings.event_topics,
            "event_mirrors": mirrors,
            "collector_error": collector_error,
            "processing": processing,
        }

    def pipeline_status(self):
        state = self.status_snapshot()
        with self.connect() as conn:
            state["counts"] = {
                name: self.execute(
                    conn, f"SELECT COUNT(*) AS n FROM {table} WHERE namespace=?", (self.mode,)
                ).fetchone()["n"]
                for name, table in [
                    ("events", "transit_event"),
                    ("metrics", "analytics_result"),
                    ("quarantine", "quarantine"),
                    ("batches", "processed_batch"),
                ]
            }
        return state

    def network_health(self):
        from statistics import mean

        rows = self.latest_metrics()
        status = self.status_snapshot()
        now = utcnow()
        routes = []
        current_contributors = []
        for route in self.routes():
            subset = [r for r in rows if r["route_id"] == route["route_id"]]
            scored = [r for r in subset if r.get("health_score") is not None]
            scores = [r["health_score"] for r in scored]
            delays = [
                r["delay_mean_seconds"]
                for r in subset
                if r.get("delay_mean_seconds") is not None and r.get("stale") is False
            ]
            score = round(mean(scores), 2) if scores and not status["stale"] else None
            if score is not None:
                current_contributors.extend(scored)
            recent_readings = []
            if score is None:
                for row in subset:
                    recent = row.get("last_valid_health")
                    if recent is None and row.get("health_retention_eligible") is True:
                        recent = retained_reading(row, row.get("health_scope"), now, self.settings.stale_seconds)
                    if recent is not None:
                        recent_readings.append(recent)
            recent = retained_summary(recent_readings, now,
                reason="Current processed source data is stale" if status["stale"] else "Waiting for current route health inputs.")
            routes.append(
                {
                    **route,
                    "health_score": score,
                    "health_level": "unknown"
                    if score is None
                    else "good"
                    if score >= 80
                    else "watch"
                    if score >= 50
                    else "disrupted",
                    **current_provenance(scored if score is not None else [], now, self.settings.stale_seconds),
                    "last_valid_health": recent,
                    "delay_mean_seconds": mean(delays) if delays else None,
                    "stop_count": len(subset),
                    "anomaly_count": sum(bool(r.get("anomaly")) for r in subset if r.get("stale") is False),
                }
            )
        valid = [r["health_score"] for r in routes if r["health_score"] is not None]
        score = round(mean(valid), 2) if valid else None
        recent_routes = [route["last_valid_health"] for route in routes if route["last_valid_health"] is not None]
        return {
            "health_score": score,
            **current_provenance(current_contributors if score is not None else [], now, self.settings.stale_seconds),
            "last_valid_health": retained_summary(recent_routes, now, reason="Waiting for current network health inputs.",
                route_count=len(recent_routes), route_weighted=True) if score is None else None,
            "routes": routes,
            "route_count": len(routes),
            "stop_count": len(self.stops()),
            "anomaly_count": sum(bool(r.get("anomaly")) for r in rows if r.get("stale") is False),
            "unknown_count": sum(r["health_score"] is None for r in routes),
            "stale": status["stale"],
        }
