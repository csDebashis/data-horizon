"""Common test isolation and JUnit requirement metadata."""
import os
from pathlib import Path
import pytest

# A test must never silently enter the live namespace.
os.environ.setdefault("DATA_MODE", "synthetic")


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    folder = tmp_path / "data"
    folder.mkdir()
    monkeypatch.setenv("DATA_DIR", str(folder))
    monkeypatch.setenv("DATA_MODE", "synthetic")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL_FILE", raising=False)
    return folder


def pytest_collection_modifyitems(items):
    for item in items:
        cases = [str(arg) for marker in item.iter_markers("case") for arg in marker.args]
        features = sorted({case.split("-")[0] for case in cases})
        features += [str(arg) for marker in item.iter_markers("feature") for arg in marker.args]
        basis, level = "synthetic", "unit"
        evidence = item.get_closest_marker("evidence")
        if evidence and len(evidence.args) == 2:
            basis, level = map(str, evidence.args)
        else:
            for candidate in ("browser", "recovery", "performance", "live", "integration", "spark"):
                if item.get_closest_marker(candidate):
                    level = candidate
                    break
            if item.get_closest_marker("live"):
                basis = "live"
        item.user_properties.extend([
            ("case_id", ",".join(cases)), ("feature", ",".join(sorted(set(features)))),
            ("data_basis", basis), ("test_level", level),
        ])


@pytest.fixture
def project_root():
    return Path(__file__).resolve().parent
