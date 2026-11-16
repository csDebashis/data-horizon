"""CHG-0060 optional local Kafka inspection, without changing broker state.

Configuration rendering needs Docker Compose but does not start containers. The
actual viewer and browser checks are opt-in; they inspect the existing mode topic
and probe DELETE only against a unique, explicitly absent topic.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote
import uuid

import httpx
import pytest

from scripts.preflight import docker_binary

ROOT = Path(__file__).resolve().parents[1]
IMAGE = (
    "ghcr.io/kafbat/kafka-ui:v1.5.0@sha256:"
    "7cda86a33344160309fdb65146332e4da65db81a945614f2fe32e210803f6fd1"
)
PRODUCTION_IMAGES = {
    name: "example.invalid/" + name.lower() + "@sha256:" + "a" * 64
    for name in ("API_IMAGE", "COLLECTOR_IMAGE", "SPARK_IMAGE", "NOTIFIER_IMAGE", "DASHBOARD_IMAGE", "OPS_IMAGE")
}


def rendered_compose(*, production=False, port=8082, profile=True):
    binary = docker_binary()
    if not binary:
        pytest.skip("BLOCKED: Docker Compose is not installed")
    arguments = [binary, "compose", "-f", "compose.yaml"]
    if production:
        arguments += ["-f", "compose.production.yaml"]
    if profile:
        arguments += ["--profile", "kafka-ui"]
    environment = {
        **os.environ, **PRODUCTION_IMAGES, "DOMAIN": "transit.example.invalid",
        "DATA_MODE": "live", "KAFKA_UI_PORT": str(port),
    }
    # Never retain the complete rendered environment: other services hold
    # operator-configured bootstrap values which are outside this test's scope.
    result = subprocess.run(
        [*arguments, "config", "--format", "json"], cwd=ROOT,
        env=environment, text=True, capture_output=True, check=True, timeout=30,
    )
    return json.loads(result.stdout)


@pytest.mark.feature("F012")
@pytest.mark.evidence("synthetic", "security")
@pytest.mark.parametrize("production,port", [(False, 8082), (True, 8082), (False, 18082)])
def test_kafka_viewer_config_is_private_bounded_and_read_only(production, port):
    services = rendered_compose(production=production, port=port)["services"]
    viewer = services["kafka-ui"]
    assert viewer["image"] == IMAGE
    assert viewer["profiles"] == ["kafka-ui"]
    assert viewer["ports"] == [{"mode": "ingress", "host_ip": "127.0.0.1", "target": 8080,
                                "published": str(port), "protocol": "tcp"}]
    assert set(viewer["networks"]) == {"backend"}
    assert not viewer.get("volumes") and not viewer.get("secrets")
    assert viewer["read_only"] is True and not viewer.get("privileged")
    assert str(viewer["user"]).split(":")[0] not in {"", "root", "0"}
    assert "ALL" in viewer["cap_drop"]
    assert "no-new-privileges:true" in viewer["security_opt"]
    assert int(viewer["mem_limit"]) == 768 * 1024**2
    assert float(viewer["cpus"]) == 0.5
    assert viewer.get("tmpfs") and all(mount.startswith("/tmp:") for mount in viewer["tmpfs"])
    assert viewer["logging"]["options"]["max-size"] == "10m"
    assert viewer["logging"]["options"]["max-file"] == "3"
    health = viewer["healthcheck"]
    assert not health.get("disable")
    assert "wget" in " ".join(health["test"]) and "/actuator/health" in " ".join(health["test"])
    assert viewer["depends_on"]["kafka"]["condition"] == "service_healthy"
    environment = viewer["environment"]
    assert environment["KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS"] == "kafka:9092"
    assert environment["KAFKA_CLUSTERS_0_READONLY"] == "true"
    for name in ("DYNAMIC_CONFIG_ENABLED", "MCP_ENABLED", "SWAGGER_UI_ENABLED"):
        assert environment[name] == "false"
    assert not any(re.search(r"PASSWORD|TOKEN|SECRET|DATABASE_URL", name) for name in environment)
    assert not services["kafka"].get("ports") and not services["postgres"].get("ports")
    caddy = (ROOT / "configs/Caddyfile").read_text()
    assert "kafka-ui" not in caddy and "8082" not in caddy


@pytest.mark.feature("F012")
@pytest.mark.evidence("synthetic", "security")
def test_kafka_viewer_is_optional_and_never_an_application_dependency():
    configuration = rendered_compose()
    assert configuration["services"]["kafka-ui"]["profiles"] == ["kafka-ui"]
    for name, service in configuration["services"].items():
        if name != "kafka-ui":
            assert "kafka-ui" not in service.get("depends_on", {})


def viewer_client():
    url = os.environ.get("KAFKA_UI_URL", "http://127.0.0.1:8082").rstrip("/")
    return httpx.Client(base_url=url, timeout=30)


def current_topic():
    mode = os.environ.get("DATA_MODE", "live")
    assert mode in {"live", "synthetic", "recorded"}
    return f"data-horizon.{mode}.transit-events.v1"


def read_json(client, path, **kwargs):
    response = client.get(path, **kwargs)
    assert response.status_code == 200, f"Viewer read failed: {path}: HTTP{response.status_code}"
    return response.json()


def current_cluster(client):
    clusters = read_json(client, "/api/clusters")
    assert len(clusters) == 1, "Only the existing mode cluster should be configured"
    cluster = clusters[0]
    assert cluster["readOnly"] is True
    assert cluster["status"] == "ONLINE"
    return cluster


def evidence_folder():
    folder = Path(os.environ.get("KAFKA_UI_EVIDENCE_DIR", "reports/kafka-ui-60"))
    folder.mkdir(parents=True, exist_ok=True)
    return folder


@pytest.mark.integration
@pytest.mark.case("F012-TC14")
@pytest.mark.evidence("live", "integration")
@pytest.mark.skipif(os.environ.get("RUN_KAFKA_UI") != "1", reason="Requires explicitly launched Kafka viewer")
def test_running_viewer_reads_existing_events_and_denies_mutations(record_testsuite_property):
    topic = current_topic()
    with viewer_client() as client:
        assert read_json(client, "/actuator/health")["status"] == "UP"
        cluster = current_cluster(client)
        prefix = "/api/clusters/" + quote(cluster["name"], safe="")
        topics = read_json(client, prefix + "/topics", params={"page": 1, "perPage": 25})
        expected_topics = {
            f"data-horizon.{os.environ.get('DATA_MODE', 'live')}.{kind}.v1"
            for kind in ("transit-events", "service-alerts", "vehicle-status", "weather-observations", "static-snapshots")
        }
        assert expected_topics <= {row["name"] for row in topics["topics"]}
        details = read_json(client, prefix + "/topics/" + topic)
        assert details["name"] == topic
        assert details["partitions"] and all("partition" in row for row in details["partitions"])
        groups = read_json(client, prefix + "/consumer-groups/paged", params={"page": 1, "perPage": 25})
        assert isinstance(groups["consumerGroups"], list)
        messages = []
        with client.stream("GET", prefix + "/topics/" + topic + "/messages/v2",
                           params={"mode": "LATEST", "limit": 5},
                           headers={"Accept": "text/event-stream"}) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            for line in response.iter_lines():
                if line.startswith("data:"):
                    event = json.loads(line[5:].strip())
                    if event.get("type") == "MESSAGE":
                        messages.append(event["message"])
                    if event.get("type") == "DONE":
                        break
        assert 1 <= len(messages) <= 5
        fields = set()
        for message in messages:
            assert isinstance(message["offset"], int) and message["offset"] >= 0
            assert isinstance(message["partition"], int) and message["partition"] >= 0
            body = json.loads(message["value"])
            assert isinstance(body, dict) and body
            fields.update(body)
        probe = "data-horizon-readonly-probe-" + uuid.uuid4().hex
        missing = client.get(prefix + "/topics/" + probe)
        assert missing.status_code == 404, "Mutation probe must target an absent topic only"
        denied = client.delete(prefix + "/topics/" + probe)
        assert denied.status_code == 405
        error = denied.json()
        assert error["code"] == 4003
        assert "read-only" in error["message"].lower()
        assert client.get(prefix + "/topics/" + probe).status_code == 404
        assert client.get("/mcp/sse").status_code == 404
        assert client.get("/mcp/message").status_code == 404
    summary = {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "cluster": cluster["name"], "read_only": True, "topic": topic, "registered_topics": sorted(expected_topics),
        "partition_count": len(details["partitions"]), "message_sample_count": len(messages),
        "message_field_names": sorted(fields), "consumer_group_count": len(groups["consumerGroups"]),
        "mutation_http_status": denied.status_code, "mutation_error_code": error["code"],
        "scope": "Existing public-transit records only; no topic creation, production or offset changes.",
    }
    (evidence_folder() / "viewer-readonly-evidence.json").write_text(json.dumps(summary, indent=2) + "\n")
    record_testsuite_property("kafka_ui_existing_message_sample_count", str(len(messages)))
    record_testsuite_property("kafka_ui_topic", topic)


@pytest.mark.browser
@pytest.mark.case("F012-TC14")
@pytest.mark.evidence("live", "browser")
@pytest.mark.skipif(os.environ.get("RUN_KAFKA_UI_BROWSER") != "1", reason="Requires explicitly launched Kafka viewer browser")
def test_kafka_viewer_browser_opens_existing_topic_and_messages():
    from playwright.sync_api import expect, sync_playwright

    folder = evidence_folder()
    with viewer_client() as client:
        cluster = current_cluster(client)
        base = str(client.base_url).rstrip("/")
    topic = current_topic()
    cluster_path = "/ui/clusters/" + quote(cluster["name"], safe="")
    writes = []
    errors = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000}, timezone_id="Asia/Kolkata")
        page.on("request", lambda request: writes.append(request.method + " " + request.url)
                if request.method not in {"GET", "HEAD", "OPTIONS"} else None)
        page.on("pageerror", lambda error: errors.append(str(error)))
        try:
            page.goto(base + cluster_path + "/all-topics", wait_until="domcontentloaded")
            row = page.get_by_role("link", name=topic, exact=True)
            expect(row).to_be_visible(timeout=30000)
            row.click()
            expect(page.get_by_text(topic, exact=True).first).to_be_visible()
            messages_link = page.get_by_role("link", name="Messages", exact=True)
            with page.expect_response(lambda response: "/messages/v2?" in response.url
                                      and response.request.method == "GET", timeout=30000) as pending:
                messages_link.click()
            assert pending.value.status == 200
            expect(page.get_by_role("columnheader", name="Offset", exact=True)).to_be_visible(timeout=30000)
            expect(page.get_by_role("cell", name=re.compile(r"^\d+$")).first).to_be_visible(timeout=30000)
            # The pinned UI keeps its closed producer form translated beyond the
            # viewport. Playwright is_visible includes that off-screen form; the
            # actual toolbar launcher and every on-screen write control must be
            # disabled. The server-side denial is checked independently above.
            toolbar_produce = page.locator('button[type="button"]').filter(
                has_text=re.compile(r"^Produce Message\s*$"))
            expect(toolbar_produce).to_be_disabled()
            for label in ("Produce Message", "Create Topic", "Delete Topic", "Clear Messages"):
                for button in page.get_by_role("button", name=re.compile(label, re.I)).all():
                    box = button.bounding_box()
                    if box and box["x"] < 1440 and box["x"] + box["width"] > 0 and box["y"] < 1000:
                        expect(button).to_be_disabled()
            page.screenshot(path=str(folder / "viewer-topic-messages.png"), full_page=True)
            assert not writes, "Read-only browsing must not issue mutation requests"
            assert not errors, "Kafka viewer browser raised JavaScript errors"
        finally:
            page.screenshot(path=str(folder / "viewer-final-browser.png"), full_page=True)
            browser.close()
