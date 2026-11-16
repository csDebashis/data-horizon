"""Synthetic cache/export mechanics do not certify JVM compatibility or Maven reachability."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import subprocess

import pytest

from scripts import export_spark_dependencies as exporter
from scripts import fetch_spark_jars as fetcher

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.feature("F012"), pytest.mark.evidence("synthetic", "security")]


def fixture_bytes(name):
    return ("synthetic unit-test JAR bytes: " + name).encode()


@pytest.fixture
def cache(tmp_path):
    folder = tmp_path / "cache"
    folder.mkdir()
    lock = copy.deepcopy(json.loads((ROOT / "docker/spark-dependencies/manifest.json").read_text()))
    for row in lock["jars"]:
        payload = fixture_bytes(row["name"])
        row.update(bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
    lock["total_bytes"] = sum(row["bytes"] for row in lock["jars"])
    (folder / "manifest.json").write_text(json.dumps(lock))
    (folder / "README.md").write_text("Synthetic test fixture; no real dependency code.")
    return folder, lock


def populate(folder, lock):
    for row in lock["jars"]:
        (folder / row["name"]).write_bytes(fixture_bytes(row["name"]))


def test_actual_dependency_lock_has_exact_versions_and_retained_provenance():
    lock = fetcher.read_lock(ROOT / "docker/spark-dependencies/manifest.json")
    assert lock["source_image"] == "sha256:1ee4c0efbe9c5ded05e79abb6c1346c10f85bfb091bd1350e8eff4f61d776795"
    assert lock["source_revision"] == "9b6712b14e03cdad39ac291e99bc13a2db24f527"
    assert len(lock["jars"]) == 12 and lock["total_bytes"] == 140972082
    assert lock["pyspark_version"] == "3.5.9" and lock["sedona_version"] == "1.9.1"


@pytest.mark.parametrize("name", ["../escape.jar", "/tmp/escape.jar", "bad/name.jar", "bad\\name.jar"])
def test_manifest_rejects_unsafe_paths_before_cache_access(cache, name):
    folder, lock = cache
    lock["jars"][0]["name"] = name
    (folder / "manifest.json").write_text(json.dumps(lock))
    with pytest.raises(ValueError, match="Unsafe"):
        fetcher.read_lock(folder / "manifest.json")


@pytest.mark.parametrize("field,value", [("source_image", "mutable:latest"), ("pyspark_version", "3.5.8"),
                                         ("sedona_version", "1.8.0"), ("source_revision", "HEAD")])
def test_manifest_rejects_unpinned_identity_and_changed_versions(cache, field, value):
    folder, lock = cache
    lock[field] = value
    (folder / "manifest.json").write_text(json.dumps(lock))
    with pytest.raises(ValueError, match="provenance"):
        fetcher.read_lock(folder / "manifest.json")


def test_complete_cache_installs_without_online_resolution(cache, tmp_path, monkeypatch):
    folder, lock = cache
    populate(folder, lock)
    target = tmp_path / "target"
    monkeypatch.setattr(fetcher, "installed_versions", lambda: fetcher.VERSIONS)
    monkeypatch.setattr(fetcher, "resolve_online", lambda *a: pytest.fail("Complete verified cache must stay offline"))
    fetcher.main(["--cache-dir", str(folder), "--target", str(target)])
    assert {path.name for path in target.glob("*.jar")} == {row["name"] for row in lock["jars"]}
    for row in lock["jars"]:
        assert (target / row["name"]).read_bytes() == fixture_bytes(row["name"])
    assert json.loads((target / "data-horizon-dependency-manifest.json").read_text())["installation_mode"] == "verified-retained-image-cache"


def test_empty_cache_keeps_official_online_path(cache, tmp_path, monkeypatch):
    folder, lock = cache
    calls = []
    monkeypatch.setattr(fetcher, "installed_versions", lambda: fetcher.VERSIONS)

    def online(target, supplied_lock):
        calls.append(supplied_lock["coordinates"])
        target.mkdir()
    monkeypatch.setattr(fetcher, "resolve_online", online)
    fetcher.main(["--cache-dir", str(folder), "--target", str(tmp_path / "target")])
    assert calls == [list(fetcher.COORDINATES)]


@pytest.mark.parametrize("damage", ["partial", "extra", "symlink", "directory", "wrong-size", "wrong-hash"])
def test_cache_must_be_complete_safe_and_checksum_exact(cache, damage, tmp_path):
    folder, lock = cache
    populate(folder, lock)
    first = folder / lock["jars"][0]["name"]
    if damage == "partial":
        first.unlink()
    elif damage == "extra":
        (folder / "unapproved.jar").write_bytes(b"extra")
    elif damage == "symlink":
        copied = tmp_path / "outside.jar"
        copied.write_bytes(first.read_bytes())
        first.unlink()
        first.symlink_to(copied)
    elif damage == "directory":
        (folder / "nested").mkdir()
    elif damage == "wrong-size":
        first.write_bytes(b"short")
    else:
        first.write_bytes(b"X" * first.stat().st_size)
    with pytest.raises(ValueError):
        fetcher.inspect_cache(folder, lock)


def test_symlink_cache_directory_and_manifest_are_rejected(cache, tmp_path):
    folder, lock = cache
    alias = tmp_path / "alias"
    alias.symlink_to(folder, target_is_directory=True)
    with pytest.raises(ValueError, match="ordinary directory"):
        fetcher.inspect_cache(alias, lock)
    manifest = folder / "manifest.json"
    actual = tmp_path / "manifest.json"
    manifest.rename(actual)
    manifest.symlink_to(actual)
    with pytest.raises(ValueError, match="ordinary dependency manifest"):
        fetcher.read_lock(manifest)


def test_cache_cannot_be_installed_into_different_runtime_versions(cache, tmp_path, monkeypatch):
    folder, lock = cache
    populate(folder, lock)
    monkeypatch.setattr(fetcher, "installed_versions", lambda: {"pyspark": "3.5.8", "apache-sedona": "1.9.1"})
    with pytest.raises(ValueError, match="Installed"):
        fetcher.main(["--cache-dir", str(folder), "--target", str(tmp_path / "target")])
    assert not (tmp_path / "target").exists()


class FakeDocker:
    container = "f" * 64

    def __init__(self, lock, corrupt=False, wrong_revision=False):
        self.lock, self.corrupt, self.wrong_revision = lock, corrupt, wrong_revision
        self.commands = []

    def __call__(self, command, **kwargs):
        self.commands.append(command[1:])
        assert kwargs["capture_output"] and kwargs["text"]
        operation = command[1]
        output = ""
        if operation == "image":
            output = json.dumps({"id": self.lock["source_image"],
                                 "revision": "0" * 40 if self.wrong_revision else self.lock["source_revision"],
                                 "volumes": None})
        elif operation == "create":
            assert "--network" in command and "none" in command and "--entrypoint" in command
            assert not any(arg in command for arg in ("--volume", "-v", "--mount", "-p", "--publish"))
            output = self.container
        elif operation == "cp":
            source = command[2]
            name = source.rsplit("/", 1)[1]
            assert source == f"{self.container}:{exporter.JAR_DIRECTORY}/{name}"
            assert name in {row["name"] for row in self.lock["jars"]}
            payload = fixture_bytes(name)
            Path(command[3]).write_bytes(b"corrupt" if self.corrupt else payload)
        elif operation == "rm":
            assert command == [command[0], "rm", self.container]
        else:
            pytest.fail("Exporter must not start/stop/reset services: " + operation)
        return subprocess.CompletedProcess(command, 0, output, "")


def test_export_copies_only_twelve_approved_files_and_removes_only_own_stopped_container(cache):
    folder, lock = cache
    docker = FakeDocker(lock)
    report = exporter.export_dependencies(folder, docker="test-docker", run=docker)
    assert report["status"] == "exported-and-verified" and report["files_verified"] == 12
    assert len([command for command in docker.commands if command[0] == "cp"]) == 12
    assert docker.commands[-1] == ["rm", docker.container]
    assert fetcher.inspect_cache(folder, lock)
    command_count = len(docker.commands)
    assert exporter.export_dependencies(folder, docker="test-docker", run=docker)["status"] == "already-verified"
    assert len(docker.commands) == command_count


def test_corrupt_export_never_activates_partial_cache_and_still_removes_own_container(cache):
    folder, lock = cache
    docker = FakeDocker(lock, corrupt=True)
    with pytest.raises(ValueError, match="checksum"):
        exporter.export_dependencies(folder, docker="test-docker", run=docker)
    assert not list(folder.glob("*.jar"))
    assert docker.commands[-1] == ["rm", docker.container]


def test_export_refuses_wrong_source_revision_before_creating_container(cache):
    folder, lock = cache
    docker = FakeDocker(lock, wrong_revision=True)
    with pytest.raises(ValueError, match="provenance"):
        exporter.export_dependencies(folder, docker="test-docker", run=docker)
    assert len(docker.commands) == 1 and docker.commands[0][0] == "image"
