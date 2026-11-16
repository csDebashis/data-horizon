"""CHG-0084/0085: private Kafdrop and bounded existing-topic inspection.

RUN_KAFDROP=1 enables actual viewer checks. Write probes target a random absent
name only; no existing topic, message or consumer offset is modified. Each run
records its actual DATA_MODE instead of claiming synthetic evidence is live.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
from urllib.parse import quote, urlsplit
import uuid

import httpx
import pytest

from scripts.preflight import docker_binary

ROOT = Path(__file__).resolve().parents[1]
IMAGE = (
    "obsidiandynamics/kafdrop:4.3.1@sha256:"
    "22f63776b5ccea972050ea73adf242b3f5a4403a2537b4413c4cc646d3c788d5"
)
SERVICES = ("api", "collector", "processor", "dashboard", "kafka", "postgres")
DOMAINS = (
    "transit-events", "service-alerts", "vehicle-status",
    "weather-observations", "static-snapshots",
)
FLAGS = {
    "--topic.createEnabled=false", "--topic.deleteEnabled=false",
    "--message.sendEnabled=false", "--cors.enabled=false",
    "--springdoc.api-docs.enabled=false", "--springdoc.swagger-ui.enabled=false",
    "--management.endpoints.web.exposure.include=health,info",
    "--management.endpoint.health.show-details=never", "--spring.jmx.enabled=false",
}
pytestmark = [pytest.mark.feature("F012"), pytest.mark.case("F012-COL04")]
BASIS = os.environ.get("DATA_MODE", "synthetic")


def docker(*arguments, mode=None, timeout=60):
    binary = docker_binary()
    if not binary:
        pytest.skip("Docker CLI is not installed")
    environment = dict(os.environ)
    if mode:
        environment["DATA_MODE"] = mode
    return subprocess.run(
        [binary, *arguments], cwd=ROOT, env=environment, check=True,
        capture_output=True, text=True, timeout=timeout,
    ).stdout


def rendered_compose(production=False, port=9000):
    images = {
        key: "example.invalid/" + key.lower() + "@sha256:" + "a" * 64
        for key in ("API_IMAGE", "COLLECTOR_IMAGE", "SPARK_IMAGE", "DASHBOARD_IMAGE", "OPS_IMAGE")
    }
    binary = docker_binary()
    if not binary:
        pytest.skip("Docker Compose is not installed")
    arguments = [binary, "compose", "-f", "compose.yaml"]
    if production:
        arguments += ["-f", "compose.production.yaml"]
    result = subprocess.run(
        [*arguments, "--profile", "kafdrop", "config", "--format", "json"],
        cwd=ROOT, env={**os.environ, **images, "DOMAIN": "transit.example.invalid",
                       "DATA_MODE": "live", "KAFDROP_PORT": str(port)},
        capture_output=True, text=True, check=True, timeout=30,
    )
    # Do not print other services' rendered environment settings.
    return json.loads(result.stdout)


@pytest.mark.evidence("synthetic", "security")
@pytest.mark.parametrize("production,port", [(False, 9000), (True, 9000), (False, 19000)])
def test_configuration_is_private_read_only_and_bounded(production, port):
    services = rendered_compose(production, port)["services"]
    viewer = services["kafdrop"]
    assert viewer["image"] == IMAGE and viewer["profiles"] == ["kafdrop"]
    assert viewer["ports"] == [{"mode": "ingress", "host_ip": "127.0.0.1", "target": 9000,
                                "published": str(port), "protocol": "tcp"}]
    assert set(viewer["networks"]) == {"backend"}
    assert viewer["user"] == "10001:10001" and viewer["read_only"] is True
    assert viewer["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in viewer["security_opt"]
    assert not any(viewer.get(key) for key in ("volumes", "secrets", "privileged", "devices"))
    assert viewer["tmpfs"] == ["/tmp:size=128m,mode=1777"]
    assert int(viewer["mem_limit"]) == 768 * 1024**2 and float(viewer["cpus"]) == 0.5
    assert viewer["logging"] == {"driver": "json-file", "options": {"max-size": "10m", "max-file": "3"}}
    environment = viewer["environment"]
    assert environment["KAFKA_BROKERCONNECT"] == "kafka:9092"
    assert set(shlex.split(environment["CMD_ARGS"])) == FLAGS
    assert not any(re.search(r"PASSWORD|TOKEN|SECRET|DATABASE_URL", key) for key in environment)
    health = viewer["healthcheck"]
    assert health["test"][:3] == ["CMD", "bash", "-ec"]
    assert "/dev/tcp/127.0.0.1/9000" in health["test"][3]
    assert "/actuator/health" in health["test"][3]
    # `compose config` re-escapes dollar signs for reusable output. Docker's
    # actual Healthcheck command is checked separately against a single `$`.
    assert '"$$status"' in health["test"][3]
    assert health["timeout"] == "5s" and health["retries"] == 8
    assert viewer["depends_on"]["kafka"]["condition"] == "service_healthy"
    assert not services["kafka"].get("ports") and not services["postgres"].get("ports")
    assert "kafdrop" not in (ROOT / "configs/Caddyfile").read_text()


@pytest.mark.evidence("synthetic", "security")
def test_optional_viewer_is_not_an_application_dependency():
    configuration = rendered_compose()
    assert configuration["services"]["kafdrop"]["profiles"] == ["kafdrop"]
    for name, service in configuration["services"].items():
        if name != "kafdrop":
            assert "kafdrop" not in service.get("depends_on", {})


@pytest.mark.evidence("synthetic", "unit")
@pytest.mark.parametrize("make_environment", [
    {"MAKELEVEL": "0", "MAKEFLAGS": "", "GNUMAKEFLAGS": ""},
    {"MAKELEVEL": "1", "MAKEFLAGS": "w", "GNUMAKEFLAGS": ""},
], ids=["ordinary-make", "inherited-directory-printing"])
@pytest.mark.parametrize("target,action", [("kafdrop-up", "up"), ("kafdrop-stop", "stop"),
                                          ("kafdrop-logs", "logs")])
def test_make_commands_target_only_the_viewer(target, action, make_environment):
    result = subprocess.run(
        ["make", "--no-print-directory", "-n", target], cwd=ROOT, capture_output=True, text=True,
        env={**os.environ, **make_environment, "DATA_MODE": "synthetic"}, check=True, timeout=30,
    )
    commands = [shlex.split(line) for line in result.stdout.splitlines() if line.strip()]
    assert len(commands) == 1
    command = commands[0]
    assert command[1:5] == ["compose", "--profile", "kafdrop", action]
    assert command[-1] == "kafdrop"
    if action == "up":
        assert "--no-deps" in command and "--no-build" in command
        assert "--force-recreate" not in command
    if action == "logs":
        assert command[-3:] == ["--tail", "100", "kafdrop"]


def mode():
    value = os.environ.get("DATA_MODE", "synthetic")
    assert value in {"live", "recorded", "synthetic"}
    return value


def viewer_url():
    value = os.environ.get("KAFDROP_URL", "http://127.0.0.1:9000").rstrip("/")
    parsed = urlsplit(value)
    assert parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}
    assert not parsed.username and not parsed.password and parsed.path in {"", "/"}
    return value


def service_ids():
    result = {}
    for service in SERVICES:
        identifier = docker("compose", "ps", "-q", service).strip()
        assert re.fullmatch(r"[a-f0-9]{64}", identifier), f"Start the existing {service} service first"
        result[service] = identifier
    return result


def broker_topics():
    output = docker(
        "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "kafka:9092", "--list",
    )
    return sorted(line.strip() for line in output.splitlines() if line.strip())


def evidence_path(label, extension="json"):
    folder = Path(os.environ.get("KAFDROP_EVIDENCE_DIR", "reports/kafdrop/" + mode()))
    folder.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return folder / f"{label}-{stamp}.{extension}"


def save_evidence(label, content):
    path = evidence_path(label)
    path.write_text(json.dumps({"checked_at_utc": datetime.now(timezone.utc).isoformat(),
                                "data_basis": mode(), **content}, indent=2) + "\n")
    return path


def json_read(client, path, **kwargs):
    response = client.get(path, headers={"Accept": "application/json"}, **kwargs)
    assert response.status_code == 200, f"Kafdrop read {path}: HTTP {response.status_code}"
    return response.json()


@pytest.mark.integration
@pytest.mark.evidence(BASIS, "integration")
@pytest.mark.skipif(os.environ.get("RUN_KAFDROP") != "1", reason="Needs an explicitly launched Kafdrop viewer")
def test_running_viewer_reads_existing_messages_and_blocks_write_controls(record_testsuite_property):
    original_ids, original_topics = service_ids(), broker_topics()
    topic = f"data-horizon.{mode()}.transit-events.v1"
    expected_topics = {f"data-horizon.{mode()}.{domain}.v1" for domain in DOMAINS}
    probe = "data-horizon-kafdrop-probe-" + uuid.uuid4().hex
    assert probe not in original_topics
    viewer_id = docker("compose", "--profile", "kafdrop", "ps", "-q", "kafdrop").strip()
    inspected = json.loads(docker("inspect", viewer_id))[0]
    assert inspected["Config"]["Image"] == IMAGE
    assert inspected["Config"]["User"] == "10001:10001"
    assert '"$status"' in inspected["Config"]["Healthcheck"]["Test"][3]
    assert "$$status" not in inspected["Config"]["Healthcheck"]["Test"][3]
    assert inspected["HostConfig"]["ReadonlyRootfs"] is True
    assert not inspected["Mounts"]
    assert inspected["State"]["Health"]["Status"] == "healthy"
    with httpx.Client(base_url=viewer_url(), timeout=30, trust_env=False) as client:
        health = json_read(client, "/actuator/health")
        assert health["status"] == "UP" and set(health) <= {"status", "groups"}
        assert set(health.get("groups", [])) <= {"liveness", "readiness"}
        assert isinstance(json_read(client, "/actuator/info"), dict)
        for path in ("/actuator/env", "/actuator/configprops", "/actuator/beans", "/actuator/heapdump",
                     "/v3/api-docs", "/swagger-ui.html", "/swagger-ui/index.html"):
            assert client.get(path).status_code == 404
        cross_origin = client.get("/topic", headers={"Origin": "https://untrusted.example.invalid"})
        assert "access-control-allow-origin" not in cross_origin.headers
        topics = json_read(client, "/topic")
        assert expected_topics <= {row["name"] for row in topics}
        details = json_read(client, "/topic/" + quote(topic, safe=""))
        partitions = details["partitions"]
        assert details["name"] == topic and partitions
        available = [part for part in partitions if part["size"] > part["firstOffset"]]
        assert available, "Expected existing transit messages"
        partition = available[0]
        parameters = {"partition": partition["id"], "offset": partition["firstOffset"],
                      "count": 3, "format": "DEFAULT", "keyFormat": "DEFAULT", "isAnyProto": "false"}
        messages = json_read(client, "/topic/" + topic + "/messages", params=parameters)
        assert 1 <= len(messages) <= 3
        for message in messages:
            assert isinstance(message["offset"], int) and message["partition"] == partition["id"]
            value = json.loads(message["message"])
            assert {"route_id", "stop_id", "trip_id", "event_time", "time_basis"} <= set(value)
        created = client.post("/topic", headers={"Accept": "application/json"},
                              data={"name": probe, "partitionsNumber": "1", "replicationFactor": "1"})
        assert "Not configured to be created" in created.text
        # These probes never address a real topic. Some disabled upstream forms
        # render an error page rather than a 403, so broker state is decisive.
        deleted = client.post("/topic/" + probe + "/delete")
        send_status, send_response_error = None, None
        try:
            sent = client.post("/topic/" + probe + "/addmessage",
                               data={"topic": probe, "topicPartition": "0", "value": "readonly-probe"})
            send_status = sent.status_code
        except httpx.RemoteProtocolError as exc:
            # The disabled handler renders an incomplete error page for an
            # absent topic. Preserve that upstream response defect; the strict
            # broker-state and existing-message checks below still decide safety.
            send_response_error = str(exc)
        assert client.get("/topic/" + probe, headers={"Accept": "application/json"}).status_code == 404
        retained = json_read(client, "/topic/" + topic + "/messages", params=parameters)
        assert retained == messages, "Existing sampled messages must remain unchanged"
    assert broker_topics() == original_topics
    assert service_ids() == original_ids
    path = save_evidence("read-only", {
        "viewer_url": viewer_url(), "image": IMAGE, "viewer_container_id": viewer_id,
        "original_service_ids": original_ids, "unchanged_service_ids": True,
        "registered_topics": original_topics, "unchanged_topics": True, "probe_topic": probe,
        "topic": topic, "partition_count": len(partitions), "message_sample_count": len(messages),
        "message_offsets": [message["offset"] for message in messages],
        "sample_sha256": hashlib.sha256(json.dumps(messages, sort_keys=True).encode()).hexdigest(),
        "write_probe_statuses": {"create": created.status_code, "delete": deleted.status_code,
                                  "send": send_status},
        "disabled_send_response_error": send_response_error,
        "scope": "Existing messages read; write probes address only one random absent topic. No app data reset.",
    })
    record_testsuite_property("kafdrop_evidence", str(path))
    record_testsuite_property("kafdrop_data_basis", mode())


@pytest.mark.integration
@pytest.mark.evidence(BASIS, "integration")
@pytest.mark.skipif(os.environ.get("RUN_KAFDROP") != "1", reason="Needs an explicitly launched Kafdrop viewer")
def test_viewer_stop_start_preserves_existing_containers():
    before = service_ids()
    docker("compose", "--profile", "kafdrop", "stop", "kafdrop")
    try:
        assert service_ids() == before
        assert httpx.get("http://127.0.0.1:8000/health/ready", timeout=15, trust_env=False).status_code == 200
    finally:
        docker("compose", "--profile", "kafdrop", "up", "-d", "--no-deps", "--no-build",
               "--wait", "--wait-timeout", "180", "kafdrop", timeout=200)
    assert service_ids() == before
    save_evidence("viewer-restart", {"original_service_ids": before, "unchanged_service_ids": True,
                                     "scope": "Only Kafdrop stopped/started; API readiness remained available."})


@pytest.mark.browser
@pytest.mark.evidence(BASIS, "browser")
@pytest.mark.skipif(os.environ.get("RUN_KAFDROP_BROWSER") != "1", reason="Needs the launched Kafdrop browser target")
def test_browser_inspects_existing_topic_without_mutation_requests(record_testsuite_property):
    from playwright.sync_api import expect, sync_playwright

    topic = f"data-horizon.{mode()}.transit-events.v1"
    writes, errors = [], []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, timezone_id="Asia/Kolkata")
        page.on("request", lambda request: writes.append(request.method + " " + request.url)
                if request.method not in {"GET", "HEAD", "OPTIONS"} else None)
        page.on("pageerror", lambda error: errors.append({"message": str(error), "stack": error.stack}))
        try:
            page.goto(viewer_url(), wait_until="domcontentloaded")
            page.get_by_role("link", name=topic, exact=True).click()
            expect(page.get_by_role("heading", name="Topic: " + topic, exact=True)).to_be_visible()
            for button in page.get_by_role("button", name=re.compile("delete", re.I)).all():
                if button.is_visible():
                    expect(button).to_be_disabled()
            page.goto(viewer_url() + "/topic/" + topic + "/allmessages?count=3", wait_until="domcontentloaded")
            expect(page.get_by_text("route_id", exact=False).first).to_be_visible()
            for button in page.get_by_role("button", name=re.compile("send|produce", re.I)).all():
                if button.is_visible():
                    expect(button).to_be_disabled()
            page.screenshot(path=str(evidence_path("topic-messages", "png")), full_page=True)
            page.goto(viewer_url() + "/topic/create", wait_until="domcontentloaded")
            for button in page.locator('button[type="submit"], input[type="submit"]').all():
                if button.is_visible():
                    expect(button).to_be_disabled()
            page.screenshot(path=str(evidence_path("creation-disabled", "png")), full_page=True)
            assert not writes
            # Upstream 4.3.1 tries to style an absent GitHub star-count element.
            # Keep this observed warning in evidence; it does not control Kafka
            # browsing. Do not hide unrelated browser errors or bypass writes.
            unexpected = [error for error in errors if not (
                error["message"] == "Cannot read properties of null (reading 'setAttribute')"
                and re.search(r"\bat restyle \(", error["stack"])
            )]
            assert not unexpected, unexpected
            path = save_evidence("browser", {"write_requests": writes, "upstream_console_warnings": errors,
                                             "topic": topic, "message_count_per_partition": 3,
                                             "scope": "Topic/partitions/messages render; create/delete/send unavailable."})
            record_testsuite_property("kafdrop_browser_evidence", str(path))
            record_testsuite_property("kafdrop_upstream_warning_count", str(len(errors)))
        finally:
            browser.close()
