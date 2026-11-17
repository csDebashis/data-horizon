# Setup and operation

Run commands from the repository root unless stated otherwise. Use a Git checkout: image builds record the source commit and reject uncommitted runtime changes.

## Requirements

- Python 3.11 and Git.
- Docker Desktop, or Docker Engine with Compose. The VM override needs Compose 2.24.4 or later.
- At least 12 GiB available to Docker for the local stack, with additional host memory for the operating system and builds.
- At least 60 GB free disk space before the initial downloads and images.
- Node.js 24 and npm for host-side frontend development and checks. Docker installs its own Node environment.
- Internet access to the public feeds, image registries, Python/npm packages and official Maven dependencies.

Java 17 and Spark are included in the processor image. Ordinary host unit tests do not need Java. Host Spark tests require their optional dependencies and a compatible Java runtime.

## First-time local setup

Create the Python environment and install the locked workspace requirements:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.dev.txt
make preflight
```

Start Docker before the preflight. Then prepare and launch Live mode:

```bash
make DATA_MODE=live setup
make DATA_MODE=live up
make DATA_MODE=live status
```

`setup` generates private settings, builds images and uploads credentials into private Docker volumes. Existing credential files are preserved. Normal `up` uses prepared images without rebuilding or clearing data. Allow the first timetable import and Spark batch to complete; check Pipeline and logs for progress.

| Address | Purpose |
| --- | --- |
| `http://127.0.0.1:8501` | Dashboard |
| `http://127.0.0.1:8000/docs` | Interactive API reference |
| `http://127.0.0.1:8000/health/live` | API process health |
| `http://127.0.0.1:8000/health/ready` | API database readiness |
| `http://127.0.0.1:9000` | Optional Kafdrop viewer |
| `http://127.0.0.1:8082` | Optional alternative Kafka viewer |

Kafka and PostgreSQL have no normal host port. Application and viewer ports bind to loopback. Prometheus is an optional internal service.

There is no end-user login or default administrator password. Generated API keys identify software access. The Node server adds its private key when calling FastAPI; browser code does not receive it.

## Configuration and data modes

Setup creates an ignored `.env` file and private `.secrets/` directory. `.env.example` lists base settings. Keep generated passwords, API keys and the backup key outside Git. Do not replace credentials during routine rebuilds: runtime upload refuses a mismatch rather than silently rotating keys.

Source settings are in `configs/nyc.yaml`, initially selecting routes 1–6. Rebuild relevant images after changing files copied into them. A custom `SOURCE_CONFIG` path must exist inside the images.

| Setting or rule | Default |
| --- | --- |
| Feed polling | 30 seconds |
| Current-data freshness | 120 seconds |
| Timetable refresh in Compose | 3,600 seconds |
| Failed timetable refresh retry | 300 seconds |
| Spark trigger | 60 seconds |
| `MAX_OFFSETS_PER_TRIGGER` | 20,000 records |

The record limit is configurable through `.env` and Compose. Larger batches can improve catch-up but may increase memory and batch time; measure before raising it.

To switch to sample data, stop the previous mode first:

```bash
make DATA_MODE=live down
make DATA_MODE=synthetic setup
make DATA_MODE=synthetic up
```

Modes have separate storage namespaces/projects but share local application ports. Use the selected `DATA_MODE` consistently for setup, start, logs, stop and backup. Stopping a mode preserves its volumes. Recorded checks require saved genuine inputs; missing inputs remain a blocked prerequisite.

## Daily operation

```bash
make DATA_MODE=live status
make DATA_MODE=live logs
make DATA_MODE=live down
make DATA_MODE=live up
```

For bounded logs, use `docker compose logs --tail 100 collector`; replace the service with `processor`, `api` or `dashboard` as needed.

The UI normally updates the visible page every 30 seconds, retaining controls and the document. It displays the last successful fetch in local time. Source age remains based on the original data: a successful fetch can return stale input. A separate labelled last-valid health score can remain for up to five minutes from its original source time; fetching or navigating does not renew it.

Do not delete volumes, prune resources or apply a development reset to fix routine startup. Preserve source archives, Kafka history, database and matching checkpoints together. Missing Kafka offsets must be investigated through retained inputs and controlled replay, rather than disabling data-loss checks.

## Kafka inspection

With the selected stack running:

```bash
make DATA_MODE=live kafdrop-up
make DATA_MODE=live kafdrop-logs
make DATA_MODE=live kafdrop-stop
```

Kafdrop shows topics, partitions and bounded message samples. Create, delete and send controls are disabled. It receives no application credentials, data-folder mount or Docker socket. The stop command affects only the viewer.

The alternative viewer uses `make kafka-ui-up` and `make kafka-ui-stop`. Keep viewer ports private on a VM and use SSH forwarding.

## Dependency downloads and builds

Run `make build` after committing runtime changes. `VCS_REF` must be the exact committed source. Generated test output is excluded from build-source checks and Docker context; real source changes are not.

Spark/Kafka/Sedona dependencies are resolved from official Maven sources. `docker/spark-dependencies/manifest.json` locks the expected files and checksums. If the exact retained image in the lock is available locally, `python scripts/export_spark_dependencies.py` can export a complete verified cache. Cache JARs are ignored by Git. Partial or changed caches are rejected. Otherwise use normal official resolution; do not change checksums to bypass a failure.

## Verification

Run normal Python and frontend checks:

```bash
make test
cd frontend
npm ci
cd ..
make frontend-check
```

Python writes JUnit XML under `reports/raw/`; Node writes `frontend/reports/unit.xml`. These are generated, ignored files. No saved report is needed to install the application. Unit results do not establish real transport, VM deployment or sustained capacity.

Use an isolated Synthetic stack for integration. Stop another mode first because the application ports are shared:

```bash
make DATA_MODE=synthetic setup
make DATA_MODE=synthetic up
make integration
```

The integration runner uses the dedicated `horizon_test` database and owned test topics/schemas. Do not run interruption or destructive recovery tests against active Live data.

Install the browser engine before browser checks, and keep the website running:

```bash
python -m playwright install chromium
make browser
```

Controlled browser tests supply labelled synthetic replies. They verify presentation, errors and refresh behaviour, separately from genuine Live checks. To check the launched Live website:

```bash
RUN_LIVE_BROWSER=1 EXPECTED_DATA_MODE=live python -m pytest tests/e2e/test_live_dashboard.py tests/e2e/test_live_navigation_recovery.py --junitxml=reports/raw/live-browser.xml
```

The read-only Live API contract uses setup-generated private keys:

```bash
RUN_LIVE_API_CONTRACT=1 LIVE_API_REPORT_FILE=reports/raw/live-api.json python -m pytest tests/test_live_api_contract.py --junitxml=reports/raw/live-api.xml
```

Public-feed, recorded, Spark, performance and recovery tests have explicit prerequisites in their source. Keep skipped or blocked checks visible rather than presenting them as passed.

## Linux VM deployment

The production override adds separate Spark master/worker containers and higher database/broker limits. Configured memory limits total about 26–27 GiB before OS overhead. Allow at least 32 GiB RAM and review CPU/disk headroom for the intended workload. Local Compose uses a smaller profile.

1. Prepare and test images, publish them to a registry and record their immutable digests. Keep the source commit available.
2. On the Linux VM, install Git, Python 3.11 and Docker/Compose. Create the Python environment as above, then run `make bootstrap`.
3. Configure the ignored `.env` with actual deployment values:

| Setting | Required value |
| --- | --- |
| `DATA_MODE` | `live` |
| `APP_ENV` | `production` |
| `API_IMAGE`, `COLLECTOR_IMAGE`, `SPARK_IMAGE`, `DASHBOARD_IMAGE`, `OPS_IMAGE` | Tested registry references ending in `@sha256:` and the full digest |
| `DOMAIN` | Real DNS name pointing to the VM |
| `BACKUP_DESTINATION` | Planned off-host backup destination |
| `BACKUP_KEY_FILE` | Private, separately protected encryption-key path |

Authenticate Docker to a private registry if required. Pull application images before initialising runtime volumes:

```bash
docker compose -f compose.yaml -f compose.production.yaml pull runtime-init collector processor api dashboard spark-master spark-worker
python scripts/upload_runtime.py
make production-check
make production-up
```

`production-up` builds PostgreSQL/proxy support images and starts the immutable application images. Do not use the local image-building `make setup` after changing aliases to production digest references.

Allow inbound 80/443 for the HTTPS proxy. Keep Kafka, PostgreSQL, Spark and viewers private. Set DNS before checking certificates. The override removes the ordinary API/UI host ports; use the configured HTTPS domain.

The settings check is not VM certification. Verify actual HTTPS, health, source/batch progress, private networks and off-host restore on that machine. One VM remains a single failure point.

## Backup and restore

With the selected stack running, create a new encrypted PostgreSQL logical backup:

```bash
make DATA_MODE=live backup OUTPUT=/secure/backups/horizon-backup.aesgcm
```

Use a new filename each time. The command refuses overwrite and uses `.secrets/backup_key` unless `BACKUP_KEY_FILE` is exported. A custom key file must be private and contain the required 256-bit hexadecimal key. Keep access to the key separate from the encrypted backup.

This creates a local encrypted file and checksum metadata, not an automatic off-host copy. Transfer it to the external destination and verify that copy. PostgreSQL backup does not capture Kafka, raw files, Parquet or checkpoints; protect them consistently or plan replay.

Test restore only into a quiet, disposable `horizon_test` database:

```bash
make DATA_MODE=live restore INPUT=/secure/backups/horizon-backup.aesgcm CONFIRM_DATABASE=horizon_test
```

Restore authenticates the file before using plaintext, then replaces objects in that target. Compare counts and canonical results before accepting recovery. Production restore requires a separate agreed outage and procedure; do not change the target casually.

## Common problems

| Symptom | Checks |
| --- | --- |
| Docker unavailable | Start the engine; check `docker info` and `make preflight`. |
| Initial dashboard empty | Allow timetable import/first Spark batch; inspect Pipeline and service logs. |
| Live data stale | Compare feed time, collector success and Spark batch/offset progress. A heartbeat alone is insufficient. |
| Build source rejected | Commit intended runtime changes or use the matching checkout; retain source validation. |
| Spark dependency failure | Check Maven connectivity and the complete verified cache. |
| Credential mismatch | Preserve existing files/volumes and identify the correct key set; routine setup does not rotate them. |
| Port in use | Stop the other mode/application without removing its data volumes. |
| Route score unavailable | Check age and exact timetable comparison coverage; missing input is not zero delay. |
