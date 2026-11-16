"""Initialise private runtime volumes from bounded Docker stdin, never a host mount."""

from pathlib import Path
import os
import argparse
import json
import sys

MOUNTS = {
    "writer": {"database_url": "database_url"},
    "reader": {"api_database_url": "database_url", "api_token": "api_token", "public_api_token": "public_api_token"},
    "public-ui": {"public_api_token": "public_api_token"},
    "service": {"api_token": "api_token"},
    "migration": {"migration_database_url": "database_url", "test_database_url": "test_database_url"},
    "postgres": {
        name: name
        for name in ("postgres_password", "migration_password", "writer_password", "reader_password")
    },
}


def prepare(
    payload=None,
    runtime_root=Path("/runtime"),
    data=Path("/data"),
    monitoring=Path("/monitoring"),
    chown=os.chown,
):
    required = {name for files in MOUNTS.values() for name in files}
    if payload is not None and (
        set(payload) != required or any(not isinstance(v, str) or not v.strip() for v in payload.values())
    ):
        raise ValueError("Runtime secret upload has missing, unknown or empty setting names")
    for group, files in MOUNTS.items():
        for source, target in files.items():
            destination = runtime_root / group / target
            if payload is not None and destination.exists():
                if destination.read_bytes() != (payload[source].strip() + "\n").encode():
                    raise ValueError("Existing runtime credentials differ; use the explicit rotation procedure")
    for group, files in MOUNTS.items():
        folder = runtime_root / group
        folder.mkdir(parents=True, exist_ok=True)
        owner = 999 if group == "postgres" else 10001
        for source, target in files.items():
            destination = folder / target
            if payload is not None:
                value = (payload[source].strip() + "\n").encode()
                if not destination.exists() or destination.read_bytes() != value:
                    temporary = destination.with_name(destination.name + "." + str(os.getpid()) + ".tmp")
                    try:
                        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                        with os.fdopen(descriptor, "wb") as stream:
                            stream.write(value)
                            stream.flush()
                            os.fsync(stream.fileno())
                        temporary.replace(destination)
                    finally:
                        temporary.unlink(missing_ok=True)
            if not destination.exists() or not destination.read_bytes().strip():
                raise ValueError("Runtime credentials are not initialised; run make setup for the selected DATA_MODE")
            destination.chmod(0o600)
            chown(destination, owner, owner)
        chown(folder, owner, owner)
        folder.chmod(0o700)
    data.mkdir(exist_ok=True)
    chown(data, 10001, 10001)
    for name in ("raw", "parquet", "curated", "checkpoints", "reports", "quarantine", "outbox"):
        child = data / name
        child.mkdir(exist_ok=True)
        chown(child, 10001, 10001)
    monitoring.mkdir(exist_ok=True)
    chown(monitoring, 10001, 10001)
    print("Runtime directories prepared; secret contents were not logged.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stdin", action="store_true")
    args = parser.parse_args()
    payload = None
    if args.stdin:
        raw = sys.stdin.buffer.read(131073)
        if len(raw) > 131072:
            raise ValueError("Runtime secret upload exceeds size limit")
        payload = json.loads(raw)
    prepare(payload)
