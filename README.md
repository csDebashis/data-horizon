# Data Horizon

Data Horizon collects and analyses New York subway data for routes 1–6. It combines public timetables, arrival forecasts, service alerts and weather, then presents the results in a five-page dashboard.

## Application

| Page | Main use |
| --- | --- |
| Network overview | Check route health, unusual readings and service alerts. |
| Route analysis | View every route platform, search or filter readings, export CSV and compare charts. |
| Graph and what-if | Calculate bounded effects through directed station connections. |
| Pipeline | Check source freshness, data quality, processing rates and Kafka progress. |
| About | Understand the application and its data flow. |

Page data updates without a full-page reload. Times use the viewer’s local timezone. Source age and the last successful page fetch are shown separately. There is no end-user login; service keys stay between the application servers.

## Architecture

```text
Public sources → Collector → Kafka → Spark → PostgreSQL → FastAPI → Next.js UI
                     ↓                 ↓
               Original files    Analytical Parquet
                     └──── Local data lake ────┘
```

The collector, processor, API and UI are separate projects in one repository. Kafka carries five event domains; Spark directly processes transit events. PostgreSQL serves current results. Raw files, manifests, Parquet and checkpoints support replay and investigation. Kafdrop provides optional private Kafka inspection.

| Folder | Responsibility |
| --- | --- |
| `frontend/` | Node.js, Next.js, React, TypeScript and Material UI website |
| `services/collector/` | Feed collection, timetable matching and event publication |
| `services/processor/` | Spark streaming, batch and graph jobs |
| `services/api/` | Bounded FastAPI analytics interface |
| `packages/core/` | Shared contracts, validation, calculations, graph and storage |
| `tools/operations/` | Manual database, replay and evaluation commands |
| `scripts/` | Setup, build checks, backups and operational utilities |
| `configs/` | Selected routes, sources, proxy and monitoring settings |
| `tests/` | Cross-component and browser verification |

## Run

Follow [SETUP.md](SETUP.md) for prerequisites and first-time installation. From a prepared checkout:

```bash
make DATA_MODE=live setup
make DATA_MODE=live up
make DATA_MODE=live status
```

Open the [dashboard](http://127.0.0.1:8501) or [API reference](http://127.0.0.1:8000/docs). Use `DATA_MODE=synthetic` for labelled sample data. Run one mode on the local ports at a time. Normal `make down` preserves data volumes.

```bash
make test
make frontend-check
make kafdrop-up
```

Kafdrop opens at [localhost:9000](http://127.0.0.1:9000). Tests create ignored JUnit output; integration and browser checks have separate prerequisites in the setup guide.

Public arrival predictions remain forecasts. Eligible exact timetable matches are required for schedule comparisons, and missing measurements remain unavailable. The graph scenario is a bounded rule-based calculation. Docker Compose includes a Linux VM configuration; actual HTTPS, off-host backup and restore must be verified on that VM.
