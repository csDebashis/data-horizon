import io
import json
import os
import shutil
import subprocess
import sys

from cryptography.exceptions import InvalidTag
import pytest

from scripts.backup import backup, decrypt_stream, encrypt_stream, read_key, restore
from scripts.bootstrap import bootstrap, effective_environ
from scripts.preflight import disk_ok, docker_binary
from scripts.production_check import validate
from scripts.run_integration import REPORT_BEGIN, REPORT_END, extract_junit

PRODUCTION_IMAGES = {name: "example.invalid/" + name.lower() + "@sha256:" + "a" * 64
                     for name in ("API_IMAGE", "COLLECTOR_IMAGE", "SPARK_IMAGE", "DASHBOARD_IMAGE", "OPS_IMAGE")}


@pytest.fixture
def clean_bootstrap_environment(monkeypatch):
    for name in tuple(os.environ):
        if name.startswith("BOOTSTRAP_") or name in {"APP_ENV", "DEFAULT_BOOTSTRAP_USERS_FILE"}:
            monkeypatch.delenv(name)


def test_bootstrap_generates_independent_private_credentials(tmp_path, clean_bootstrap_environment):
    names = bootstrap(tmp_path)
    assert "api_token" in names and "backup_key" in names
    secrets = tmp_path / ".secrets"
    passwords = [(secrets / (name + "_password")).read_text() for name in ("postgres", "migration", "writer", "reader")]
    assert len(set(passwords)) == 4
    for path in secrets.iterdir():
        assert path.stat().st_mode & 0o077 == 0
    original = (secrets / "api_token").read_text()
    assert bootstrap(tmp_path) == []
    assert (secrets / "api_token").read_text() == original










@pytest.mark.case("F012-TC03")
@pytest.mark.evidence("synthetic", "security")
def test_rendered_compose_limits_ports_and_privileges(project_root):
    executable = docker_binary()
    if not executable:
        pytest.skip("BLOCKED: Docker Compose is not installed")
    environment = {**os.environ, **PRODUCTION_IMAGES, "DATA_MODE": "live", "DOMAIN": "transit.example.invalid"}
    result = subprocess.run([executable, "compose", "-f", "compose.yaml", "-f", "compose.production.yaml",
                             "config", "--format", "json"], cwd=project_root, env=environment,
                            text=True, capture_output=True, check=True, timeout=30)
    services = json.loads(result.stdout)["services"]
    assert not services["api"].get("ports")
    assert not services["dashboard"].get("ports")
    assert not services["postgres"].get("ports")
    assert not services["kafka"].get("ports")
    assert {port["target"] for port in services["proxy"]["ports"]} == {80, 443}
    for name in ("api", "dashboard", "collector", "processor", "db-roles"):
        service = services[name]
        assert service["read_only"] is True
        assert "ALL" in service["cap_drop"]
        assert int(service["mem_limit"]) > 0 and float(service["cpus"]) > 0
        assert "no-new-privileges:true" in service["security_opt"]
        assert not service.get("privileged")
        assert all(mount.startswith("/") for mount in service.get("tmpfs", []))
    assert services["database-init"]["depends_on"]["db-roles"]["condition"] == "service_completed_successfully"
    assert services["db-roles"]["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert services["dashboard"]["environment"]["PUBLIC_API_TOKEN_FILE"] == "/run/secrets/public_api_token"
    assert services["dashboard"]["environment"]["API_TOKEN_FILE"] == "/run/service/api_token"
    assert services["dashboard"]["environment"]["PUBLIC_API_BASE_URL"] == "https://transit.example.invalid"
    assert services["api"]["environment"]["DASHBOARD_PUBLIC_URL"] == "https://transit.example.invalid"
    assert "BOOTSTRAP_USERS_FILE" not in services["api"]["environment"]
    assert "DASHBOARD_PASSWORD_HASH" not in services["proxy"]["environment"]
    for service, filename in (("api", "services/api/Dockerfile"), ("collector", "services/collector/Dockerfile"),
                              ("processor", "services/processor/Dockerfile"),
                              ("dashboard", "frontend/Dockerfile"), ("database-init", "tools/operations/Dockerfile")):
        assert services[service]["build"]["dockerfile"] == filename
    assert services["dashboard"]["command"] == ["node", "server.js"]
    assert "basic_auth" not in (project_root / "configs/Caddyfile").read_text()
    mounted = {mount["source"] for service in services.values() for mount in service.get("volumes", []) if mount["type"] == "volume"}
    assert "ui-secrets" not in mounted


def test_production_configuration_fails_closed():
    errors = validate({})
    assert any("immutable" in error for error in errors)
    assert any("DOMAIN" in error for error in errors)
    assert any("BACKUP" in error for error in errors)




@pytest.mark.case("F012-TC08")
@pytest.mark.evidence("synthetic", "recovery")
def test_disk_guard_refuses_insufficient_capacity_without_deleting_files(tmp_path, monkeypatch):
    evidence = tmp_path / "important-evidence"
    evidence.write_text("preserve")
    monkeypatch.setattr(shutil, "disk_usage", lambda path: type("Usage", (), {"free": 5 * 1024**3})())
    valid, available = disk_ok(tmp_path, 60)
    assert valid is False and available == 5
    assert evidence.read_text() == "preserve"


def test_encrypted_backup_round_trip_and_random_nonce():
    key = os.urandom(32)
    source = b"Public transit database export\n" * 10000
    encrypted, other = io.BytesIO(), io.BytesIO()
    encrypt_stream(io.BytesIO(source), encrypted, key)
    encrypt_stream(io.BytesIO(source), other, key)
    assert encrypted.getvalue() != other.getvalue()
    assert source[:100] not in encrypted.getvalue()
    recovered = io.BytesIO()
    encrypted.seek(0)
    decrypt_stream(encrypted, recovered, key)
    assert recovered.getvalue() == source


def test_tampered_backup_fails_authentication():
    key = os.urandom(32)
    encrypted = io.BytesIO()
    encrypt_stream(io.BytesIO(b"database content"), encrypted, key)
    content = bytearray(encrypted.getvalue())
    content[-17] ^= 1
    with pytest.raises(InvalidTag):
        decrypt_stream(io.BytesIO(content), io.BytesIO(), key)


def test_backup_key_permissions_and_restore_confirmation(tmp_path):
    key = tmp_path / "key"
    key.write_text(os.urandom(32).hex())
    key.chmod(0o600)
    assert len(read_key(key)) == 32
    key.chmod(0o644)
    with pytest.raises(ValueError, match="permissions"):
        read_key(key)
    with pytest.raises(ValueError, match="confirm-database"):
        restore(tmp_path / "nonexistent", key, "horizon", "horizon_test")


def test_existing_backup_is_preserved_before_starting_a_database_export(tmp_path, monkeypatch):
    key = tmp_path / "key"
    key.write_text(os.urandom(32).hex())
    key.chmod(0o600)
    output = tmp_path / "existing.aesgcm"
    output.write_bytes(b"prior authenticated backup")
    def unexpected(*args, **kwargs):
        raise AssertionError("No database process should start for an existing destination")
    monkeypatch.setattr(subprocess, "Popen", unexpected)
    with pytest.raises(FileExistsError, match="already exists"):
        backup(output, key)
    assert output.read_bytes() == b"prior authenticated backup"


def test_integration_junit_transport_survives_temporary_container_filesystem():
    import base64
    xml = b'<testsuites><testsuite><testcase name="database"/><testcase name="broker"/></testsuite></testsuites>'
    output = "ordinary pytest output\n" + REPORT_BEGIN + "\n" + base64.b64encode(xml).decode() + "\n" + REPORT_END + "\n"
    assert extract_junit(output) == xml


@pytest.mark.parametrize("output", [
    "five tests passed but no saved evidence",
    REPORT_BEGIN + "\nnot-valid-base64!\n" + REPORT_END,
    REPORT_END + "\n" + REPORT_BEGIN,
    REPORT_BEGIN + "\nPHRlc3RzdWl0ZS8+\n" + REPORT_END,
])
def test_integration_junit_transport_rejects_missing_or_malformed_evidence(output):
    with pytest.raises(ValueError, match="Integration JUnit"):
        extract_junit(output)


@pytest.mark.parametrize("action", ["backup", "restore"])
@pytest.mark.parametrize("selection", ["explicit", "environment", "default"])
def test_backup_cli_key_precedence_fails_before_database_mutation(tmp_path, project_root, action, selection):
    environment = dict(os.environ)
    environment.pop("BACKUP_KEY_FILE", None)
    explicit = tmp_path / "explicit-missing-key"
    configured = tmp_path / "environment-missing-key"
    if selection in {"explicit", "environment"}:
        environment["BACKUP_KEY_FILE"] = str(configured)
    output = tmp_path / "must-not-exist" / "backup.aesgcm"
    arguments = [sys.executable, str(project_root / "scripts/backup.py"), action]
    if action == "backup":
        arguments += ["--output", str(output)]
    else:
        source = tmp_path / "existing-encrypted-input"
        source.write_bytes(b"not decrypted until the selected key is validated")
        arguments += ["--input", str(source), "--confirm-database", "horizon_test"]
    if selection == "explicit":
        arguments += ["--key-file", str(explicit)]
    expected = str(explicit) if selection == "explicit" else str(configured) if selection == "environment" else ".secrets/backup_key"
    result = subprocess.run(arguments, cwd=tmp_path, env=environment,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert "FileNotFoundError" in result.stderr
    assert expected in result.stderr
    assert not output.parent.exists()


def test_setup_preserves_old_private_files_without_requiring_accounts(tmp_path):
    folder = tmp_path / '.secrets'
    folder.mkdir()
    retired = folder / 'bootstrap_users.json'
    retired.write_bytes(b'old private account configuration')
    existing = folder / 'api_token'
    existing.write_bytes(b'existing-service-key\n')
    bootstrap(tmp_path)
    assert retired.read_bytes() == b'old private account configuration'
    assert existing.read_bytes() == b'existing-service-key\n'
    assert not (folder / 'experience_password').exists()
    assert not (folder / 'notifier_password').exists()
    assert not (folder / 'sample-login.env').exists()


def test_dotenv_settings_are_read_and_explicit_environment_wins(tmp_path):
    (tmp_path / '.env').write_text('DATA_MODE=recorded\nAPP_ENV=development\n')
    assert effective_environ(tmp_path, {'DATA_MODE': 'synthetic'}) == {
        'DATA_MODE': 'synthetic', 'APP_ENV': 'development'
    }
