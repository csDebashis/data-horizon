"""Spark Structured Streaming, SQL calculations and durable Parquet output."""

from __future__ import annotations

import os
import json
import threading

from .analytics import health_score, weather_as_of
from .alert_scope import alert_matches, latest_entity_contexts
from .config import Settings, configure_spark_python
from .contracts import TransitEvent, timestamp, utcnow
from .store import Store, dumps


def broker_end_offsets(settings):
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_servers.split(","),
        group_id=None,
        enable_auto_commit=False,
        api_version_auto_timeout_ms=3000,
        request_timeout_ms=10000,
        session_timeout_ms=6000,
    )
    try:
        partitions = consumer.partitions_for_topic(settings.transit_topic)
        if partitions is None:
            return None
        positions = consumer.end_offsets([TopicPartition(settings.transit_topic, p) for p in partitions])
        return {f"{p.topic}:{p.partition}": int(offset) for p, offset in positions.items()}
    finally:
        consumer.close()


def progress_offsets(progress):
    result = {}
    for source in progress.get("sources", []):
        raw = source.get("endOffset")
        if isinstance(raw, str):
            raw = json.loads(raw)
        if isinstance(raw, dict):
            for topic, partitions in raw.items():
                if isinstance(partitions, dict):
                    result.update({f"{topic}:{p}": int(offset) for p, offset in partitions.items()})
    return result


def create_spark(name="Data Horizon"):
    configure_spark_python()
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_PARTITIONS", "4"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )
    if os.getenv("SPARK_JARS_PACKAGES"):
        builder = builder.config("spark.jars.packages", os.environ["SPARK_JARS_PACKAGES"])
    if os.getenv("SPARK_DRIVER_HOST"):
        builder = builder.config("spark.driver.host", os.environ["SPARK_DRIVER_HOST"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def aggregate_spark(spark, events, *, as_of=None, alerts=None, weather=None, route_context=None):
    """Spark SQL equivalent of the independently tested Python reference."""
    from pyspark.sql import functions as F, Window
    from .analytics import alert_active, latest_event_revisions

    now = timestamp(as_of) if as_of else utcnow()
    events = latest_event_revisions(events, as_of=now)
    if not events:
        return []
    alert_contexts = latest_entity_contexts(events, now, route_context=route_context)
    serialised = [dumps(e.model_dump(mode="json") if isinstance(e, TransitEvent) else e) for e in events]
    df = spark.read.json(spark.sparkContext.parallelize(serialised, min(4, len(events))))
    needed = {
        "observed_time": "string",
        "predicted_time": "string",
        "scheduled_time": "string",
        "cancelled": "boolean",
        "scheduled_headway_seconds": "double",
        "match_confidence": "string",
    }
    for col, typ in needed.items():
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast(typ))
        else:
            df = df.withColumn(col, F.col(col).cast(typ))
    df = df.withColumn("event_ts", F.to_timestamp("event_time")).filter(
        (F.col("event_ts") > F.lit(now).cast("timestamp") - F.expr("INTERVAL 30 MINUTES"))
        & (F.col("event_ts") <= F.lit(now))
    )
    keys = ["route_id", "direction_id", "stop_id"]
    latest = Window.partitionBy(*keys, "trip_id").orderBy(F.col("event_ts").desc(), F.col("event_id").desc())
    revisions = df.withColumn("revision", F.row_number().over(latest)).filter(F.col("revision") == 1)
    # Retain a key even when all its latest trip revisions are cancellations.
    # Its unknown snapshot must supersede previously healthy serving state.
    entities = revisions.groupBy(*keys).agg(
        F.max(F.col("event_ts").cast("double")).alias("source_as_of_epoch")
    )
    df = (
        revisions.filter(~F.coalesce(F.col("cancelled"), F.lit(False)))
        .withColumn("arrival_ts", F.to_timestamp(F.coalesce("observed_time", "predicted_time")))
        # Keep forecast/actual arrival gaps, but never compare a guessed schedule.
        .withColumn("scheduled_ts", F.when(
            F.col("match_confidence") == "exact", F.to_timestamp("scheduled_time")
        ))
        .withColumn(
            "delay",
            F.when(
                (F.col("time_basis") == "observed") & (F.col("match_confidence") == "exact"),
                F.to_timestamp("observed_time").cast("double") - F.col("scheduled_ts").cast("double"),
            ).when(
                (F.col("time_basis") == "predicted") & (F.col("match_confidence") == "exact"),
                F.to_timestamp("predicted_time").cast("double") - F.col("scheduled_ts").cast("double"),
            ),
        )
    )
    aggregations = [
        F.avg("delay").alias("delay_mean_seconds"),
        F.stddev_pop("delay").alias("delay_stddev_seconds"),
        F.count("*").alias("event_count"),
        F.expr(
            "percentile(CASE WHEN match_confidence = 'exact' AND scheduled_headway_seconds > 0 "
            "THEN scheduled_headway_seconds END,0.5)"
        ).alias("explicit_headway"),
        F.collect_set("time_basis").alias("bases"),
    ]
    for minutes in (5, 15, 30):
        value = F.when(
            F.col("event_ts") > F.lit(now).cast("timestamp") - F.expr(f"INTERVAL {minutes} MINUTES"),
            F.col("delay"),
        )
        aggregations.extend(
            [
                F.avg(value).alias(f"rolling_{minutes}m_mean"),
                F.stddev_pop(value).alias(f"rolling_{minutes}m_stddev"),
            ]
        )
    result = entities.join(df.groupBy(*keys).agg(*aggregations), keys, "left").withColumn(
        "event_count", F.coalesce(F.col("event_count"), F.lit(0))
    )
    for value, alias in [("arrival_ts", "headway_seconds"), ("scheduled_ts", "derived_headway")]:
        sequence = Window.partitionBy(*keys).orderBy(value, "trip_id", "event_id")
        gaps = (
            df.filter(F.col(value).isNotNull())
            .withColumn(
                "gap", F.col(value).cast("double") - F.lag(F.col(value).cast("double")).over(sequence)
            )
            .filter(F.col("gap") > 0)
            .groupBy(*keys)
            .agg(F.expr("percentile(gap,0.5)").alias(alias))
        )
        result = result.join(gaps, keys, "left")
    output = []
    for record in result.collect():
        row = record.asDict(recursive=True)
        source_as_of = timestamp(row.pop("source_as_of_epoch")).isoformat()
        scheduled = row.pop("explicit_headway") or row.pop("derived_headway", None)
        row.pop("derived_headway", None)
        row["scheduled_headway_seconds"] = scheduled
        row["headway_deviation"] = (
            (row["headway_seconds"] - scheduled) / scheduled
            if row["headway_seconds"] is not None and scheduled
            else None
        )
        applicable = [
            a
            for a in (alerts or [])
            if alert_active(a, now)
            and any(
                alert_matches(a, context)
                for context in (
                    alert_contexts.get((row["route_id"], str(row["direction_id"]), row["stop_id"]))
                    or [
                        {
                            **(route_context or {}).get(row["route_id"], {}),
                            "route_id": row["route_id"],
                            "stop_id": row["stop_id"],
                            "direction_id": row["direction_id"],
                        }
                    ]
                )
            )
        ]
        row["alert_count"] = len(applicable)
        row.update(
            health_score(
                row["delay_mean_seconds"],
                row["headway_deviation"],
                max((a.get("severity", 0.5) for a in applicable), default=0),
            )
        )
        row["anomaly"] = (
            "high_delay"
            if row["delay_mean_seconds"] is not None and row["delay_mean_seconds"] >= 300
            else "irregular_headway"
            if row["headway_deviation"] is not None and abs(row["headway_deviation"]) >= 0.5
            else None
        )
        bases = row.pop("bases") or []
        row.update(
            time_basis="observed"
            if bases == ["observed"]
            else "predicted"
            if "predicted" in bases
            else "unavailable",
            window_start=now.replace(second=0, microsecond=0).isoformat(),
            as_of=now.isoformat(),
            source_as_of=source_as_of,
            weather_temperature_c=weather_as_of(weather, now).get("temperature_c"),
            feature_version="v1",
        )
        output.append(row)
    return sorted(output, key=lambda x: (x["route_id"], x["direction_id"], x["stop_id"]))


def validate_spatial(spark, stops):
    """A real Sedona query; missing Sedona is a failed prerequisite, not a fake pass."""
    from sedona.spark import SedonaContext

    SedonaContext.create(spark)
    frame = spark.createDataFrame(
        [(s["stop_id"], float(s["stop_lon"]), float(s["stop_lat"])) for s in stops], ["stop_id", "lon", "lat"]
    )
    frame.createOrReplaceTempView("transit_stops")
    return [
        r.asDict()
        for r in spark.sql(
            "SELECT stop_id, ST_IsValid(ST_Point(lon,lat)) AS valid, ST_AsText(ST_Point(lon,lat)) AS geometry FROM transit_stops"
        ).collect()
    ]


def transit_stream(spark, settings):
    """Only canonical transit enters analytics; mirror snapshots have no consumer here."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_servers)
        .option("subscribe", settings.transit_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000")))
        .option("failOnDataLoss", "true")
        .load()
    )


def run_processor(once=False):
    settings = Settings.load()
    if not settings.kafka_servers:
        raise ValueError("Spark streaming requires KAFKA_BOOTSTRAP_SERVERS")
    spark = create_spark()
    generation = os.getenv("CHECKPOINT_GENERATION", "v1")
    checkpoint = settings.data_dir / "checkpoints" / settings.mode / generation
    store = Store()
    stop_heartbeat = threading.Event()
    active_query = {}

    def heartbeat():
        while not stop_heartbeat.is_set():
            (settings.data_dir / "processor-heartbeat").touch()
            query = active_query.get("query")
            if query:
                progress = query.lastProgress or {}
                offsets = progress_offsets(progress)
                snapshot = {
                    "query_active": query.isActive,
                    "last_progress_at": progress.get("timestamp"),
                    "input_rows_per_second": progress.get("inputRowsPerSecond"),
                    "processed_rows_per_second": progress.get("processedRowsPerSecond"),
                    "batch_duration_ms": progress.get("durationMs", {}).get("triggerExecution"),
                    "num_input_rows": progress.get("numInputRows"),
                    "batch_id": progress.get("batchId"),
                    "end_offsets": offsets,
                    "kafka_lag": None,
                }
                try:
                    latest = broker_end_offsets(settings)
                    snapshot["broker_end_offsets"] = latest
                    if latest is not None and offsets:
                        snapshot["kafka_lag"] = sum(
                            max(0, offset - offsets.get(partition, 0)) for partition, offset in latest.items()
                        )
                except Exception as exc:
                    snapshot["broker_measurement_error"] = type(exc).__name__
                try:
                    store.put_metadata("streaming", snapshot)
                except Exception:
                    pass  # Keep liveness alive; SQL readiness/freshness reports dependency failure.
            stop_heartbeat.wait(20)

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    input_df = transit_stream(spark, settings)

    def consume(batch, batch_id):
        records = []
        for row in batch.selectExpr("CAST(value AS STRING) AS value").toLocalIterator():
            try:
                records.append(TransitEvent.model_validate_json(row["value"]).model_dump(mode="json"))
            except (ValueError, TypeError):
                store.quarantine({"payload": row["value"][:4096]}, "invalid_kafka_event")

        def calculate(events, **kwargs):
            metrics = aggregate_spark(spark, events, **kwargs)
            if metrics:
                folder = (
                    settings.data_dir
                    / "curated"
                    / settings.mode
                    / "features"
                    / generation
                    / f"batch-{batch_id}"
                )
                # Atomic Spark output commit; retry replaces only this deterministic batch path.
                feature_df = spark.read.json(spark.sparkContext.parallelize([dumps(m) for m in metrics]))
                feature_df.write.mode("overwrite").parquet(str(folder))
            return metrics

        result = store.apply_batch(
            records, query_id=f"spark-{generation}", batch_id=str(batch_id), aggregate_fn=calculate
        )
        print(
            dumps({"batch_id": batch_id, "result": result, "engine": "Spark Structured Streaming"}),
            flush=True,
        )

    writer = (
        input_df.writeStream.foreachBatch(consume)
        .option("checkpointLocation", str(checkpoint))
        .queryName(f"data-horizon-{settings.mode}-{generation}")
    )
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=os.getenv("PROCESSING_TRIGGER", "60 seconds"))
    try:
        query = writer.start()
        active_query["query"] = query
        query.awaitTermination()
    finally:
        stop_heartbeat.set()
        thread.join(timeout=2)
        spark.stop()


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description="Data Horizon Spark processor and bounded jobs")
    parser.add_argument("command", nargs="?", choices=("run", "backfill", "graph"), default="run")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--input")
    args = parser.parse_args(argv)
    if args.command == "run":
        run_processor(once=args.once)
        return
    if args.command == "backfill" and not args.input:
        parser.error("this job requires --input")
    from .processor_jobs import run_backfill, run_graph

    action = {"backfill": run_backfill, "graph": run_graph}[args.command]
    print(dumps(action(args, Settings.load(), Store())), flush=True)


if __name__ == "__main__":
    main()
