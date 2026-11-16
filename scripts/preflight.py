"""Read-only checks before a full Docker launch."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import subprocess


def disk_ok(path: Path, minimum_gb: float = 60) -> tuple[bool, float]:
    available = shutil.disk_usage(path).free / (1024 ** 3)
    return available >= minimum_gb, round(available, 2)


def docker_binary() -> str | None:
    found = shutil.which("docker")
    fallback = Path("/Applications/Docker.app/Contents/Resources/bin/docker")
    return found or (str(fallback) if fallback.exists() else None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum-gb", type=float, default=60)
    parser.add_argument("--path", type=Path, default=Path.cwd())
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    enough, free = disk_ok(args.path, args.minimum_gb)
    result = {"disk_free_gb": free, "minimum_gb": args.minimum_gb, "disk_ok": enough}
    executable = docker_binary()
    try:
        if executable is None:
            raise RuntimeError("Docker CLI is not installed")
        check = subprocess.run([executable, "info", "--format", "{{json .}}"],
                               capture_output=True, text=True, timeout=20, check=True)
        info = json.loads(check.stdout)
        result.update(docker_ready=True, architecture=info.get("Architecture"),
                      docker_cpus=info.get("NCPU"), docker_memory_bytes=info.get("MemTotal"))
    except (RuntimeError, OSError, subprocess.SubprocessError, ValueError) as exc:
        result.update(docker_ready=False, reason=type(exc).__name__ + ": Docker daemon is unavailable")
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0 if enough and result.get("docker_ready") else 1


if __name__ == "__main__":
    raise SystemExit(main())
