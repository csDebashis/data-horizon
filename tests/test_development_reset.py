"""Scoped destructive-operation policy using a completely fake Docker engine."""
from __future__ import annotations

import copy
import json
from types import SimpleNamespace

import pytest

from scripts.reset_development import (
    DATA_VOLUMES, PROJECT_LABEL, VOLUME_LABEL, WORKDIR_LABEL, DevelopmentReset,
)


pytestmark = [pytest.mark.case("F012-TC15"), pytest.mark.evidence("synthetic", "security")]


class FakeDocker:
    def __init__(self, root, mode="live"):
        self.root = root
        self.calls = []
        self.mode = mode
        self.project = "data-horizon-" + mode
        logical = sorted(DATA_VOLUMES | {"reader-secrets", "postgres-secrets"})
        self.configuration = {
            "name": self.project, "services": {"api": {"environment": {"APP_ENV": "development"}}},
            "volumes": {key: {"name": self.project + "_" + key} for key in logical},
        }
        self.volumes = {
            self.project + "_" + key: {
                "Name": self.project + "_" + key, "Driver": "local", "Scope": "local", "Options": {},
                "Labels": {PROJECT_LABEL: self.project, VOLUME_LABEL: key},
            } for key in logical
        }
        self.volumes["unrelated-data"] = {"Name": "unrelated-data", "Labels": {PROJECT_LABEL: "other"}}
        self.containers = {
            "own-api": {"Id": "own-api", "Config": {"Env": ["APP_ENV=development"], "Labels": {
                PROJECT_LABEL: self.project, WORKDIR_LABEL: str(root), "com.docker.compose.service": "api",
            }}},
            "other-app": {"Id": "other-app", "Config": {"Env": [], "Labels": {
                PROJECT_LABEL: "other-project", WORKDIR_LABEL: str(root / "other"),
            }}},
        }
        self.mounts = {self.project + "_" + name: ["own-api"] for name in DATA_VOLUMES}

    def __call__(self, arguments, **kwargs):
        assert kwargs["capture_output"] and kwargs["env"]["DATA_MODE"] == self.mode
        args = arguments[1:]
        self.calls.append(args)
        result = None
        if args[0] == "compose":
            if args[-3:] == ["config", "--format", "json"]:
                result = json.dumps(self.configuration)
            elif "down" in args:
                assert "--volumes" not in args and "-v" not in args
                assert args[args.index("--project-name") + 1] == self.project
                self.containers.pop("own-api", None)
                result = ""
        elif args[:2] == ["volume", "ls"]:
            result = "\n".join(json.dumps(name) for name in self.volumes)
        elif args[:2] == ["volume", "inspect"]:
            result = json.dumps([self.volumes[args[2]]])
        elif args[:2] == ["volume", "rm"]:
            del self.volumes[args[2]]
            result = ""
        elif args[:2] == ["ps", "-aq"]:
            condition = args[-1]
            if condition.startswith("label="):
                result = "\n".join(key for key, value in self.containers.items()
                                   if value["Config"]["Labels"][PROJECT_LABEL] == self.project)
            elif condition.startswith("volume="):
                result = "\n".join(key for key in self.mounts.get(condition[7:], []) if key in self.containers)
        elif args[0] == "inspect":
            result = json.dumps([self.containers[args[1]]])
        assert result is not None, args
        return SimpleNamespace(returncode=0, stdout=result, stderr="")


@pytest.fixture
def reset_fixture(tmp_path):
    (tmp_path / "compose.yaml").write_text("name: data-horizon-live\n")
    for filename in [".env", ".secrets/api_token", "docs/feature.md", "reports/result.json", ".idea/workspace.xml"]:
        path = tmp_path / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("preserved-" + filename)
    docker = FakeDocker(tmp_path)
    reset = DevelopmentReset(tmp_path, "live", runner=docker, environ={"APP_ENV": "development"}, binary="docker")
    return reset, docker, tmp_path


def mutating_calls(docker):
    return [call for call in docker.calls if "down" in call or call[:2] == ["volume", "rm"]]


def test_reset_plan_is_read_only_and_does_not_touch_credentials(reset_fixture):
    reset, docker, root = reset_fixture
    initial = copy.deepcopy(docker.volumes)
    files = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    plan = reset.plan()
    assert plan["dry_run"] is True
    assert {row["logical"] for row in plan["data_volumes"]} == DATA_VOLUMES
    assert all(row["exists"] for row in plan["data_volumes"])
    assert set(plan["protected_volume_names"]) == {
        "data-horizon-live_reader-secrets", "data-horizon-live_postgres-secrets",
    }
    assert docker.volumes == initial and not mutating_calls(docker)
    assert all(path.read_bytes() == value for path, value in files.items())


def test_reset_execution_removes_only_labelled_data_and_preserves_credentials(reset_fixture):
    reset, docker, root = reset_fixture
    files = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    result = reset.execute(reset.plan())
    assert result["reset_complete"] is True and result["dry_run"] is False
    assert set(result["removed_volumes"]) == {"data-horizon-live_" + key for key in DATA_VOLUMES}
    assert set(docker.volumes) == {
        "data-horizon-live_reader-secrets", "data-horizon-live_postgres-secrets", "unrelated-data",
    }
    assert set(docker.containers) == {"other-app"}
    assert all(path.read_bytes() == value for path, value in files.items())
    assert len(mutating_calls(docker)) == 5


@pytest.mark.parametrize("change", ["external", "missing-labels", "wrong-project", "wrong-logical", "host-backed", "foreign-user"])
def test_reset_refuses_unsafe_volumes_before_stopping_anything(reset_fixture, change):
    reset, docker, _ = reset_fixture
    logical = "datasets"
    definition = docker.configuration["volumes"][logical]
    volume = docker.volumes[definition["name"]]
    if change == "external":
        definition["external"] = True
    elif change == "missing-labels":
        volume["Labels"] = {}
    elif change == "wrong-project":
        volume["Labels"][PROJECT_LABEL] = "some-other-project"
    elif change == "wrong-logical":
        volume["Labels"][VOLUME_LABEL] = "reader-secrets"
    elif change == "host-backed":
        volume["Options"] = {"device": "/host/user-files", "o": "bind"}
    else:
        docker.mounts[definition["name"]].append("other-app")
    with pytest.raises(ValueError):
        reset.plan()
    assert not mutating_calls(docker)


@pytest.mark.parametrize("change", ["environment", "rendered", "running", "production-file", "different-root", "project-override"])
def test_reset_refuses_production_or_wrong_project(reset_fixture, change):
    reset, docker, root = reset_fixture
    if change in {"environment", "project-override"}:
        setting = {"APP_ENV": "PRODUCTION"} if change == "environment" else {"COMPOSE_PROJECT_NAME": "other"}
        with pytest.raises(ValueError):
            DevelopmentReset(root, "live", runner=docker, environ=setting, binary="docker")
    else:
        own = docker.containers["own-api"]["Config"]
        if change == "rendered":
            docker.configuration["services"]["api"]["environment"]["APP_ENV"] = "production"
        elif change == "running":
            own["Env"] = ["APP_ENV=production"]
        elif change == "production-file":
            own["Labels"]["com.docker.compose.project.config_files"] = str(root / "compose.production.yaml")
        else:
            own["Labels"][WORKDIR_LABEL] = str(root / "different-repository")
        with pytest.raises(ValueError):
            reset.plan()
    assert not mutating_calls(docker)


def test_reset_rechecks_labels_before_the_destructive_phase(reset_fixture):
    reset, docker, _ = reset_fixture
    plan = reset.plan()
    docker.volumes["data-horizon-live_kafka-data"]["Labels"][PROJECT_LABEL] = "other"
    with pytest.raises(ValueError):
        reset.execute(plan)
    assert not mutating_calls(docker)


def test_reset_rejects_aliasing_a_secret_volume(reset_fixture):
    reset, docker, _ = reset_fixture
    docker.configuration["volumes"]["reader-secrets"]["name"] = "data-horizon-live_datasets"
    with pytest.raises(ValueError, match="protected"):
        reset.plan()
    assert not mutating_calls(docker)


def test_reset_handles_an_already_empty_project_without_pruning(reset_fixture):
    reset, docker, _ = reset_fixture
    docker.containers.pop("own-api")
    for name in DATA_VOLUMES:
        del docker.volumes["data-horizon-live_" + name]
    plan = reset.plan()
    assert not any(row["exists"] for row in plan["data_volumes"])
    result = reset.execute(plan)
    assert result["removed_volumes"] == []
    assert not any(call[:2] == ["volume", "rm"] for call in docker.calls)
    assert "unrelated-data" in docker.volumes


def test_reset_invalid_mode_fails_before_docker(reset_fixture):
    _, docker, root = reset_fixture
    with pytest.raises(ValueError, match="explicit supported"):
        DevelopmentReset(root, "../other", runner=docker, environ={}, binary="docker")
    assert not docker.calls


@pytest.mark.parametrize("mode", ["synthetic", "recorded"])
def test_reset_selects_only_the_explicit_mode_project(reset_fixture, mode):
    _, _, root = reset_fixture
    docker = FakeDocker(root, mode)
    reset = DevelopmentReset(root, mode, runner=docker, environ={}, binary="docker")
    plan = reset.plan()
    assert plan["project"] == "data-horizon-" + mode
    assert all(row["name"].startswith("data-horizon-" + mode + "_") for row in plan["data_volumes"])
    assert not mutating_calls(docker)
