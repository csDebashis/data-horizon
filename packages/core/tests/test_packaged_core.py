"""Shared-library packaging does not depend on repository cwd or worker projects."""

from importlib import metadata
from pathlib import Path

import data_horizon

from data_horizon.config import Settings


def test_core_namespace_and_packaged_defaults_outside_repository(tmp_path, monkeypatch):
    monkeypatch.delenv("SOURCE_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    assert data_horizon.__spec__.origin is None
    assert metadata.version("data-horizon-core") == "0.2.0"
    assert Settings.load().routes == ("1", "2", "3", "4", "5", "6")
    assert not Path("configs/nyc.yaml").exists()
