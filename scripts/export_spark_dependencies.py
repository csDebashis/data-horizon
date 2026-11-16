"""Explicitly export only checksum-locked dependency JARs from a retained image."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

try:
    from scripts.fetch_spark_jars import DEFAULT_CACHE, inspect_cache, read_lock
    from scripts.preflight import docker_binary
except ModuleNotFoundError:
    from fetch_spark_jars import DEFAULT_CACHE, inspect_cache, read_lock
    from preflight import docker_binary

JAR_DIRECTORY = "/usr/local/lib/python3.11/site-packages/pyspark/jars"


def export_dependencies(cache_dir: Path, *, docker: str, run=subprocess.run) -> dict:
    lock = read_lock(cache_dir / "manifest.json")
    existing = inspect_cache(cache_dir, lock)
    report = {"source_image": lock["source_image"], "source_revision": lock["source_revision"],
              "files_verified": len(lock["jars"]), "bytes_verified": lock["total_bytes"],
              "scope": "Dependency bytes only; no application code, credentials, volumes or running services."}
    if existing:
        return {**report, "status": "already-verified"}

    def command(arguments, *, timeout=30):
        result = run([docker, *arguments], text=True, capture_output=True, timeout=timeout, check=False)
        if result.returncode:
            raise RuntimeError("Docker dependency export failed during " + arguments[0])
        return result.stdout.strip()

    image_format = ('{"id":{{json .Id}},"revision":'
                    '{{json (index .Config.Labels "org.opencontainers.image.revision")}},'
                    '"volumes":{{json (index .Config "Volumes")}}}')
    image = json.loads(command(["image", "inspect", "--format", image_format, lock["source_image"]]))
    if image.get("id") != lock["source_image"] or image.get("revision") != lock["source_revision"] or image.get("volumes"):
        raise ValueError("Source image identity/provenance or volume declaration is not approved")
    container = None
    created = []
    try:
        container = command(["create", "--network", "none", "--read-only", "--cap-drop", "ALL",
                             "--security-opt", "no-new-privileges:true", "--entrypoint", "/bin/true",
                             lock["source_image"]])
        if not re.fullmatch(r"[a-f0-9]{64}", container):
            raise RuntimeError("Docker did not return an exact temporary container identity")
        with tempfile.TemporaryDirectory(prefix="horizon-spark-dependencies-") as temporary:
            staging = Path(temporary)
            for row in lock["jars"]:
                command(["cp", f"{container}:{JAR_DIRECTORY}/{row['name']}", str(staging / row["name"])], timeout=60)
            files = inspect_cache(staging, lock)
            # Activate only the complete verified set. Never overwrite an input
            # which appeared after the initial check or copy a symlink target.
            for source in files:
                destination = cache_dir / source.name
                with destination.open("xb") as output, source.open("rb") as source_stream:
                    created.append(destination)
                    shutil.copyfileobj(source_stream, output, length=1024 * 1024)
            inspect_cache(cache_dir, lock)
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    finally:
        if container and re.fullmatch(r"[a-f0-9]{64}", container):
            command(["rm", container])
    return {**report, "status": "exported-and-verified"}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--docker", default=docker_binary())
    args = parser.parse_args(argv)
    if not args.docker:
        parser.error("Docker CLI is required to export the pinned retained image")
    print(json.dumps(export_dependencies(args.cache_dir, docker=str(args.docker)), indent=2))


if __name__ == "__main__":
    main()
