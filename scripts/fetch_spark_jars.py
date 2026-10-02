"""Install pinned Spark dependencies from official Maven or an explicit verified cache."""
from __future__ import annotations

import argparse
import hashlib
from importlib import metadata
import json
from pathlib import Path
import re
import shutil

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = ROOT / "docker/spark-dependencies"
COORDINATES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9",
    "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.1",
)
VERSIONS = {"pyspark": "3.5.9", "apache-sedona": "1.9.1"}
REQUIRED_JARS = {
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.9.jar",
    "org.apache.sedona_sedona-spark-shaded-3.5_2.12-1.9.1.jar",
}
MAX_TOTAL_BYTES = 160 * 1024 * 1024


def read_lock(path: Path) -> dict:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise ValueError("A bounded ordinary dependency manifest is required")
    lock = json.loads(path.read_text())
    if (not isinstance(lock, dict) or lock.get("schema_version") != 1
            or lock.get("pyspark_version") != VERSIONS["pyspark"]
            or lock.get("sedona_version") != VERSIONS["apache-sedona"]
            or tuple(lock.get("coordinates", ())) != COORDINATES
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", str(lock.get("source_image", "")))
            or not re.fullmatch(r"[a-f0-9]{40}", str(lock.get("source_revision", "")))):
        raise ValueError("Dependency manifest version or provenance is invalid")
    rows = lock.get("jars")
    if not isinstance(rows, list) or len(rows) != 12:
        raise ValueError("The exact twelve dependency JARs are required")
    names = set()
    total = 0
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Invalid dependency entry")
        name, size, digest = row.get("name"), row.get("bytes"), row.get("sha256")
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+\.jar", name)
                or Path(name).name != name or name in names
                or type(size) is not int or not 0 < size <= 128 * 1024 * 1024
                or not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)):
            raise ValueError("Unsafe, duplicate or unbounded dependency entry")
        names.add(name)
        total += size
    if not REQUIRED_JARS <= names or total > MAX_TOTAL_BYTES or lock.get("total_bytes") != total:
        raise ValueError("Dependency set or total byte bound is invalid")
    return lock


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inspect_cache(folder: Path, lock: dict) -> list[Path] | None:
    """None means no cache supplied; every incomplete or altered cache is rejected."""
    if folder.is_symlink() or not folder.is_dir():
        raise ValueError("Dependency cache must be an ordinary directory")
    expected = {row["name"]: row for row in lock["jars"]}
    entries = list(folder.iterdir())
    allowed = set(expected) | {"README.md", "manifest.json"}
    if any(path.name not in allowed or path.is_symlink() or not path.is_file() for path in entries):
        raise ValueError("Dependency cache has an extra, unsafe or symlink input")
    present = {path.name for path in entries if path.name.endswith(".jar")}
    if not present:
        return None
    if present != set(expected):
        raise ValueError("Partial dependency cache; all twelve JARs must match the lock")
    result = []
    for name, row in sorted(expected.items()):
        path = folder / name
        if path.stat().st_size != row["bytes"] or file_sha256(path) != row["sha256"]:
            raise ValueError("Dependency cache size/checksum mismatch: " + name)
        result.append(path)
    return result


def installed_versions() -> dict[str, str]:
    return {name: metadata.version(name) for name in VERSIONS}


def install_files(files: list[Path], target: Path) -> None:
    if target.is_symlink():
        raise ValueError("Spark JAR target must not be a symlink")
    target.mkdir(parents=True, exist_ok=True)
    if any((target / path.name).is_symlink() for path in files):
        raise ValueError("Spark dependency destination must not be a symlink")
    for path in files:
        shutil.copy2(path, target / path.name)


def resolve_online(target: Path, lock: dict) -> None:
    """Keep official online resolution as the clean-checkout default."""
    from pyspark.sql import SparkSession

    ivy = Path("/tmp/horizon-ivy")
    spark = (
        SparkSession.builder.master("local[1]").appName("image-dependency-check")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.jars.ivy", str(ivy))
        .config("spark.jars.repositories", "https://repo.maven.apache.org/maven2")
        .config("spark.jars.packages", ",".join(COORDINATES)).getOrCreate()
    )
    try:
        spark.range(1).count()
    finally:
        spark.stop()
    sources = inspect_cache(ivy / "jars", lock)
    if not sources:
        raise RuntimeError("Official Maven dependencies were not resolved")
    install_files(sources, target)
    shutil.rmtree(ivy)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--target", type=Path)
    args = parser.parse_args(argv)
    lock = read_lock(args.cache_dir / "manifest.json")
    if installed_versions() != VERSIONS:
        raise ValueError("Installed PySpark/Sedona versions do not match the dependency lock")
    target = args.target
    if target is None:
        import pyspark
        target = Path(pyspark.__file__).parent / "jars"
    sources = inspect_cache(args.cache_dir, lock)
    if sources is None:
        mode = "official-maven"
        resolve_online(target, lock)
    else:
        mode = "verified-retained-image-cache"
        install_files(sources, target)
    provenance = {"installation_mode": mode, "lock": lock}
    (target / "data-horizon-dependency-manifest.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(f"Installed twelve checksum-verified dependency JARs using {mode}.")


if __name__ == "__main__":
    main()
