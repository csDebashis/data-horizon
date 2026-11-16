"""Reset only explicitly selected, labelled Data Horizon development data.

Dry-run is the default. Credentials and secret volumes are never reset. This is
development baseline tooling, not schema migration or production recovery.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess

try:
    from scripts.bootstrap import effective_environ
    from scripts.preflight import docker_binary
except ModuleNotFoundError:
    from bootstrap import effective_environ
    from preflight import docker_binary


DATA_VOLUMES = frozenset({"datasets", "postgres-data", "kafka-data", "prometheus-data"})
REQUIRED_VOLUMES = frozenset({"datasets", "postgres-data", "kafka-data"})
MODES = frozenset({"live", "synthetic", "recorded"})
PROJECT_LABEL = "com.docker.compose.project"
VOLUME_LABEL = "com.docker.compose.volume"
WORKDIR_LABEL = "com.docker.compose.project.working_dir"


class DevelopmentReset:
    def __init__(self, root: Path, mode: str, *, runner=subprocess.run, environ=None, binary=None):
        self.root = Path(root).resolve(strict=True)
        if mode not in MODES:
            raise ValueError("An explicit supported DATA_MODE is required")
        if not (self.root / "compose.yaml").is_file():
            raise ValueError("The repository Compose file is required")
        self.mode = mode
        self.project = "data-horizon-" + mode
        self.environ = effective_environ(self.root, environ)
        self.environ["DATA_MODE"] = mode
        if self.environ.get("APP_ENV", "development").strip().lower() == "production":
            raise ValueError("Development reset refuses production")
        if self.environ.get("COMPOSE_PROJECT_NAME", self.project) != self.project:
            raise ValueError("Compose project override does not match the selected Data Horizon mode")
        self.binary = binary or docker_binary()
        if not self.binary:
            raise RuntimeError("Docker CLI is unavailable")
        self.runner = runner

    def _run(self, arguments, *, timeout=60):
        try:
            result = self.runner(
                [self.binary, *arguments], cwd=self.root, env=self.environ,
                text=True, capture_output=True, check=False, timeout=timeout,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RuntimeError("Docker reset inspection or operation could not run") from exc
        if result.returncode:
            # Complete Compose output can contain operator configuration. Never
            # print it, the process environment or a rendered configuration.
            raise RuntimeError("Docker reset command failed (exit " + str(result.returncode) + ")")
        return result.stdout

    def _json(self, arguments):
        try:
            return json.loads(self._run(arguments))
        except json.JSONDecodeError as exc:
            raise RuntimeError("Docker returned an invalid inspection document") from exc

    def _compose(self):
        return ["compose", "--project-name", self.project, "-f", str(self.root / "compose.yaml"),
                "--profile", "*"]

    def _container(self, identifier):
        details = self._json(["inspect", identifier])[0]
        labels = details.get("Config", {}).get("Labels") or {}
        if labels.get(PROJECT_LABEL) != self.project:
            raise ValueError("A data volume is used by an unrelated container")
        workdir = labels.get(WORKDIR_LABEL)
        if not workdir or Path(workdir).resolve() != self.root:
            raise ValueError("Project container ownership does not match this repository")
        settings = dict(item.split("=", 1) for item in (details.get("Config", {}).get("Env") or [])
                        if "=" in item)
        if settings.get("APP_ENV", "").strip().lower() == "production":
            raise ValueError("Development reset refuses an existing production container")
        config_files = labels.get("com.docker.compose.project.config_files", "")
        if "compose.production" in config_files:
            raise ValueError("Development reset refuses the production Compose deployment")
        return {"id": details["Id"], "service": labels.get("com.docker.compose.service", "unknown")}

    def _volume(self, name, logical):
        detail = self._json(["volume", "inspect", name])[0]
        labels = detail.get("Labels") or {}
        if labels.get(PROJECT_LABEL) != self.project or labels.get(VOLUME_LABEL) != logical:
            raise ValueError("Data volume labels do not match the selected project and logical volume")
        if detail.get("Driver") != "local" or detail.get("Options"):
            raise ValueError("Development reset requires ordinary local named volumes")
        if detail.get("Scope", "local") != "local":
            raise ValueError("Development reset refuses nonlocal data volumes")

    def plan(self):
        configuration = self._json([*self._compose(), "config", "--format", "json"])
        if configuration.get("name") != self.project:
            raise ValueError("Rendered Compose project does not match the selected mode")
        services = configuration.get("services", {})
        if "proxy" in services or any(
            str((service.get("environment") or {}).get("APP_ENV", "")).strip().lower() == "production"
            for service in services.values()
        ):
            raise ValueError("Development reset refuses production configuration")
        definitions = configuration.get("volumes", {})
        if not REQUIRED_VOLUMES <= definitions.keys():
            raise ValueError("Required project data volume declarations are missing")
        volume_names = {json.loads(line) for line in self._run(
            ["volume", "ls", "--format", "{{json .Name}}"]
        ).splitlines() if line.strip()}
        containers = [self._container(identifier) for identifier in self._run(
            ["ps", "-aq", "--filter", "label=" + PROJECT_LABEL + "=" + self.project]
        ).split()]
        selected = []
        for logical in sorted(DATA_VOLUMES & definitions.keys()):
            definition = definitions[logical]
            name = definition.get("name")
            if definition.get("external") or definition.get("driver_opts"):
                raise ValueError("Development reset refuses external or host-backed data volumes")
            if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", name):
                raise ValueError("A resolved named data volume is required")
            exists = name in volume_names
            if exists:
                self._volume(name, logical)
                for identifier in self._run(["ps", "-aq", "--filter", "volume=" + name]).split():
                    self._container(identifier)
            selected.append({"logical": logical, "name": name, "exists": exists})
        names = [row["name"] for row in selected]
        if len(set(names)) != len(names):
            raise ValueError("Distinct logical data volumes cannot share one name")
        protected_names = {
            value["name"] for key, value in definitions.items()
            if key not in DATA_VOLUMES and isinstance(value.get("name"), str)
        }
        if protected_names.intersection(names):
            raise ValueError("A data volume cannot also be a protected secret or other volume")
        return {
            "project": self.project, "mode": self.mode, "dry_run": True,
            "data_volumes": selected, "project_containers": sorted(containers, key=lambda row: row["id"]),
            "protected_volume_names": sorted(protected_names),
            "preserved": ["credentials and all secret volumes", ".env", ".secrets", "Git",
                          "documents and reports", "local IDE state", "unrelated Docker projects"],
            "next_step": "Reinitialize protected runtime credentials, then start the rebuilt baseline stack.",
        }

    def execute(self, plan):
        # Inspect ownership again immediately before the destructive phase.
        if self.plan() != plan:
            raise ValueError("Project resources changed after planning; inspect a new reset plan")
        self._run([*self._compose(), "down", "--remove-orphans", "--timeout", "30"], timeout=120)
        removed = []
        for volume in plan["data_volumes"]:
            if volume["exists"]:
                self._volume(volume["name"], volume["logical"])
                self._run(["volume", "rm", volume["name"]])
                removed.append(volume["name"])
        return {**plan, "dry_run": False, "reset_complete": True, "removed_volumes": removed}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=sorted(MODES), required=True)
    parser.add_argument("--execute", action="store_true", help="Delete only the inspected project data volumes")
    args = parser.parse_args(argv)
    try:
        reset = DevelopmentReset(Path(__file__).resolve().parents[1], args.mode)
        plan = reset.plan()
        result = reset.execute(plan) if args.execute else plan
    except (ValueError, RuntimeError) as exc:
        print(json.dumps({"status": "refused", "reason": str(exc)}))
        return 2
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
