"""Operator-selected source settings reach each independent container."""

from pathlib import Path

import pytest
import yaml

from data_horizon.config import Settings

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.feature("F001"), pytest.mark.feature("F012"),
              pytest.mark.case("F001-TC07"), pytest.mark.evidence("synthetic", "unit")]


def test_runtime_and_operations_select_the_shipped_operator_configuration():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    # Credential initialisation and the fixture-only test profile are separate.
    for service in ("collector", "processor", "api", "database-init", "benchmark"):
        assert compose["services"][service]["environment"]["SOURCE_CONFIG"] == "${SOURCE_CONFIG:-/app/configs/nyc.yaml}", service


def test_explicit_file_precedes_packaged_defaults_and_invalid_file_fails(tmp_path, monkeypatch):
    config = tmp_path / "operator.yaml"
    config.write_text("routes: ['2']\nstale_seconds: 75\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SOURCE_CONFIG", str(config))
    assert Settings.load().routes == ("2",)
    assert Settings.load().stale_seconds == 75
    monkeypatch.setenv("SOURCE_CONFIG", str(tmp_path / "missing.yaml"))
    with pytest.raises(FileNotFoundError):
        Settings.load()
    config.write_text("routes: ['unsupported']\n")
    monkeypatch.setenv("SOURCE_CONFIG", str(config))
    with pytest.raises(ValueError, match="Unsupported route scope"):
        Settings.load()


def test_installed_default_is_independent_of_unrelated_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SOURCE_CONFIG", raising=False)
    assert Settings.load().routes == ("1", "2", "3", "4", "5", "6")
