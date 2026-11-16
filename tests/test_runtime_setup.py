import json

import pytest

from scripts.prepare_runtime import MOUNTS, prepare
from data_horizon.collector import archive_response
from data_horizon.config import Settings
from data_horizon.benchmark import run


def payload():
    return {name: "private-" + name for files in MOUNTS.values() for name in files}


def test_runtime_secret_roles_stdin_and_repeatability(tmp_path, capsys):
    runtime = tmp_path / "runtime"
    data = tmp_path / "data"
    monitor = tmp_path / "monitor"
    values = payload()
    calls = []
    prepare(values, runtime, data, monitor, lambda *args: calls.append(args))
    prepare(None, runtime, data, monitor, lambda *args: calls.append(args))
    assert (runtime / "writer" / "database_url").read_text().strip() == values["database_url"]
    assert not (runtime / "writer" / "api_token").exists()
    assert not (runtime / "ui" / "database_url").exists()
    assert {path.name for path in (runtime / "public-ui").iterdir()} == {"public_api_token"}
    assert (runtime / "service/api_token").read_text().strip() == values["api_token"]
    assert (runtime / "public-ui/public_api_token").read_text().strip() == values["public_api_token"]
    assert (runtime / "reader" / "api_token").stat().st_mode & 0o777 == 0o600
    captured = capsys.readouterr().out
    assert all(value not in captured for value in values.values())
    with pytest.raises(ValueError, match="rotation"):
        prepare({**values, "api_token": "changed"}, runtime, data, monitor, lambda *args: None)
    assert (runtime / "reader" / "api_token").read_text().strip() == values["api_token"]




def test_cli_init_db_creates_current_tables_idempotently(isolated_data_dir, capsys):
    from data_horizon.cli import main
    from data_horizon.store import Store

    main(["init-db"])
    store = Store(ensure_schema=False)
    with store.connect() as conn:
        tables = {row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert tables == {
        "metadata", "assets", "transit_event", "analytics_result", "processed_batch", "quarantine",
    }
    store.put_metadata("current-baseline", {"preserved": True})
    main(["init-db"])
    assert store.get_metadata("current-baseline") == {"preserved": True}
    assert [json.loads(line) for line in capsys.readouterr().out.splitlines()] == [
        {"status": "initialized"}, {"status": "initialized"},
    ]




def test_runtime_rejects_partial_or_uninitialised_upload(tmp_path):
    args = (tmp_path / "runtime", tmp_path / "data", tmp_path / "monitor", lambda *args: None)
    with pytest.raises(ValueError, match="missing"):
        prepare({"api_token": "not-enough"}, *args)
    with pytest.raises(ValueError, match="not initialised"):
        prepare(None, *args)


def test_production_non_serving_role_does_not_need_an_api_token():
    settings = Settings(
        environment="production", mode="live", database_url="postgresql://operator@db/horizon", api_token=""
    )
    assert settings.api_token == "" and settings.database_url.startswith("postgresql")


def test_benchmark_cannot_run_in_production(monkeypatch):
    monkeypatch.setenv("DATA_MODE", "synthetic")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DATABASE_URL", "postgresql://operator@db/horizon")
    with pytest.raises(ValueError, match="production"):
        run(duration=1)


def test_raw_quota_preserves_old_data(isolated_data_dir, monkeypatch):
    settings = Settings.load()
    first = archive_response(settings, "fixture", b"original")
    from pathlib import Path

    old = Path(first["path"])
    monkeypatch.setenv("MAX_RAW_GB", "0.000000001")
    with pytest.raises(OSError, match="RAW_ARCHIVE_LIMIT"):
        archive_response(settings, "fixture", b"new")
    assert old.read_bytes() == b"original"


@pytest.mark.parametrize(
    "name,value", [("MIN_FREE_GB", "nan"), ("MIN_FREE_GB", "-1"), ("MAX_RAW_GB", "inf"), ("MAX_RAW_GB", "0")]
)
def test_invalid_archive_limits(isolated_data_dir, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match="limits"):
        archive_response(Settings.load(), "fixture", b"new")


def test_existing_archive_corruption_is_visible(isolated_data_dir):
    from pathlib import Path

    settings = Settings.load()
    record = archive_response(settings, "fixture", b"original")
    Path(record["path"]).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum"):
        archive_response(settings, "fixture", b"original")


def test_missing_runtime_credentials_points_to_existing_setup_command(tmp_path, project_root):
    with pytest.raises(ValueError, match='make setup'):
        prepare(None, tmp_path / 'runtime', tmp_path / 'data', tmp_path / 'monitor', lambda *args: None)
    makefile = (project_root / 'Makefile').read_text()
    assert 'setup: bootstrap build' in makefile


def test_fresh_vm_prepares_helper_images_before_no_build_start(project_root):
    makefile = (project_root / 'Makefile').read_text()
    target = makefile.split('production-up: production-check\n', 1)[1].split('\n\n', 1)[0]
    assert 'build postgres proxy' in target
    assert 'up -d --no-build' in target
    assert target.index('build postgres proxy') < target.index('up -d --no-build')
