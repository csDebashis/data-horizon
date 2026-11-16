"""Operational commands. Modes are explicit so fixtures cannot masquerade as live data."""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import Settings
from .store import Store, dumps


from .processor_jobs import backfill_windows as backfill_windows, load_records as load_records


def main(argv=None):
    parser = argparse.ArgumentParser(description="Data Horizon operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db")
    for command in ("collect", "process", "demo"):
        p = sub.add_parser(command)
        p.add_argument("--once", action="store_true")
    p = sub.add_parser("ingest-static")
    p.add_argument("--file", required=True)
    p = sub.add_parser("replay")
    p.add_argument("--input", required=True)
    p.add_argument("--speed", type=float, default=1)
    p = sub.add_parser("backfill")
    p.add_argument("--input", required=True)
    sub.add_parser("evaluate")
    sub.add_parser("graph")
    sub.add_parser("source-check")
    args = parser.parse_args(argv)
    settings = Settings.load()
    if args.command == "init-db":
        Store(ensure_schema=True)
        print(dumps({"status": "initialized"}))
        return
    if args.command in {"collect", "demo"}:
        from .collector import run_collector

        if args.command == "demo" and settings.mode != "synthetic":
            parser.error("demo requires DATA_MODE=synthetic")
        run_collector(args.once)
        return
    if args.command == "process":
        from .processor import run_processor

        run_processor(args.once)
        return
    store = Store()
    if args.command == "ingest-static":
        from .collector import ingest_static

        result = ingest_static(Path(args.file).read_bytes(), store, settings)
    elif args.command == "replay":
        from .collector import replay, producer_for

        producer = producer_for(settings)
        try:
            result = replay(args.input, settings, speed=args.speed, producer=producer)
        finally:
            if producer:
                producer.close(timeout=10)
    elif args.command == "backfill":
        from .processor_jobs import run_backfill

        result = run_backfill(args, settings, store)
    elif args.command == "graph":
        from .processor_jobs import run_graph

        result = run_graph(args, settings, store)
    elif args.command == "source-check":
        from .collector import fetch, archive_response, decode_feed

        raw = fetch(settings.realtime_url)
        manifest = archive_response(settings, "gtfs-realtime", raw, source_url=settings.realtime_url)
        _, _, quality = decode_feed(raw, settings)
        result = {
            "manifest": manifest,
            "quality": quality,
        }
    else:
        result = {
            "pipeline": store.pipeline_status(),
            "release_ready": False,
            "reason": "Use the acceptance execution report for release approval",
        }
    print(dumps(result), flush=True)


if __name__ == "__main__":
    main()
