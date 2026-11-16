"""Upload required settings via Docker stdin; never expose values in CLI arguments."""

import json
import os
from pathlib import Path
import subprocess

try:
    from scripts.bootstrap import bootstrap, effective_environ
    from scripts.prepare_runtime import MOUNTS
    from scripts.preflight import docker_binary
except ModuleNotFoundError:
    from bootstrap import bootstrap, effective_environ
    from prepare_runtime import MOUNTS
    from preflight import docker_binary


def upload(root):
    bootstrap(root)
    required = {name for group in MOUNTS.values() for name in group}
    payload = {name: (root / ".secrets" / name).read_text().strip() for name in required}
    binary = docker_binary()
    if not binary:
        raise RuntimeError("Docker CLI is unavailable")
    environment = effective_environ(root)
    environment["PATH"] = str(Path(binary).parent) + os.pathsep + environment.get("PATH", "")
    subprocess.run(
        [
            binary,
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "runtime-init",
            "python",
            "scripts/prepare_runtime.py",
            "--stdin",
        ],
        cwd=root,
        env=environment,
        input=json.dumps(payload),
        text=True,
        check=True,
        timeout=120,
    )
    print("Runtime credentials initialised through Docker stdin.")


if __name__ == "__main__":
    upload(Path(__file__).resolve().parents[1])
