"""Launched-stack browser checks. No API data is mocked in this module."""
from __future__ import annotations

import os
import re

import pytest
from playwright.sync_api import expect

DATA_MODE = os.getenv("EXPECTED_DATA_MODE", "live")
pytestmark = [pytest.mark.browser, pytest.mark.evidence(DATA_MODE, "browser"),
              pytest.mark.skipif(os.getenv("RUN_LIVE_BROWSER") != "1", reason="Set RUN_LIVE_BROWSER=1 for the launched stack")]
if DATA_MODE == "live":
    pytestmark.append(pytest.mark.live)


def wait_fetched(page):
    expect(page.get_by_test_id("page-last-fetched")).to_be_visible(timeout=45000)
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Updating data", timeout=45000)


@pytest.mark.feature("F011")
@pytest.mark.case("F011-COL01")
def test_launched_stack_five_pages_and_source_truth(page, dashboard_url, screenshot_dir):
    page.goto(dashboard_url)
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible(timeout=45000)
    wait_fetched(page)
    expected_mode = os.getenv("EXPECTED_DATA_MODE", "live")
    expect(page.get_by_test_id("source-status")).to_contain_text("Data: " + expected_mode)
    page.evaluate("window.liveDocumentMarker = 'original'")
    for label, heading in [("Network overview", "Network overview"), ("Route analysis", "Route analysis"),
                           ("Graph and what-if", "Graph and what-if"), ("Pipeline", "Pipeline"),
                           ("About", "About Data Horizon")]:
        page.get_by_role("navigation", name="Main navigation").get_by_role("link", name=label, exact=True).click()
        expect(page.get_by_role("heading", name=heading, exact=True)).to_be_visible(timeout=45000)
        if label != "About":
            wait_fetched(page)
        assert page.evaluate("window.liveDocumentMarker") == "original"
        expect(page.get_by_role("button", name="Sign in", exact=True)).to_have_count(0)
        page.screenshot(path=str(screenshot_dir / ("live-" + label.lower().replace(" ", "-") + ".png")), full_page=True)
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    def check_safe_fields(value):
        if isinstance(value, dict):
            for key, item in value.items():
                assert key not in {"artifact_path", "artifact_dir", "model_path", "model_dir", "password", "token", "dsn"}
                check_safe_fields(item)
        elif isinstance(value, list):
            for item in value:
                check_safe_fields(item)
    for path in ("pipeline/status", "network/health"):
        response = page.request.get(dashboard_url + "/bff/api/v1/" + path)
        assert response.status == 200
        check_safe_fields(response.json()["data"])



@pytest.mark.feature("F008")
@pytest.mark.case("F008-COL01")
def test_launched_graph_calculation(page, dashboard_url, screenshot_dir):
    page.goto(dashboard_url + "/?view=scenario")
    expect(page.get_by_role("button", name="Run scenario", exact=True)).to_be_enabled(timeout=45000)
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("10")
    page.get_by_role("spinbutton", name=re.compile(r"^Time horizon \(minutes\)")).fill("30")
    with page.expect_response(lambda response: response.url.endswith("/bff/api/v1/what-if") and response.request.method == "POST") as received:
        page.get_by_role("button", name="Run scenario", exact=True).click()
    assert received.value.status == 200
    payload = received.value.json()
    assert payload["data"]["origin"]["stop_id"]
    assert isinstance(payload["data"]["affected_stations"], list)
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_be_visible()
    if payload["data"]["affected_stations"]:
        expect(page.get_by_role("table", name="Scenario results", exact=True)).to_be_visible()
    page.screenshot(path=str(screenshot_dir / "live-graph-calculation.png"), full_page=True)


@pytest.mark.feature("F011")
@pytest.mark.case("F011-COL02")
def test_launched_page_refresh_and_local_time(page, dashboard_url):
    calls = []
    page.on("request", lambda request: calls.append(request.url) if "/bff/api/v1/" in request.url else None)
    page.goto(dashboard_url + "/?view=route")
    wait_fetched(page)
    expect(page.get_by_test_id("selection-context")).to_be_visible(timeout=45000)
    page.evaluate("window.liveDocumentMarker = 'same'")
    first = page.get_by_test_id("page-last-fetched").get_attribute("datetime")
    expect(page.get_by_test_id("page-last-fetched")).not_to_have_attribute("datetime", first, timeout=45000)
    assert page.evaluate("window.liveDocumentMarker") == "same"
    assert not any(re.search(r"/(anomalies|pipeline/status|model/status|what-if)(\?|$)", value) for value in calls)
    expect(page.get_by_test_id("local-zone")).to_have_text("Asia/Kolkata")
    expect(page.get_by_test_id("page-last-fetched")).to_contain_text("+05:30")
    page.get_by_role("button", name="Refresh data", exact=True).click()
    wait_fetched(page)
    assert page.evaluate("window.liveDocumentMarker") == "same"


@pytest.mark.feature("F011")
@pytest.mark.case("F011-COL04")
def test_launched_complete_catalogue_membership_and_coverage(page, dashboard_url, screenshot_dir, tmp_path):
    import csv
    import json
    from urllib.parse import quote

    page.goto(dashboard_url + "/?view=route&route=1")
    expect(page.get_by_test_id("coverage-context")).to_contain_text("Route 1", timeout=45000)
    wait_fetched(page)
    routes_response = page.request.get(dashboard_url + "/bff/api/v1/routes")
    assert routes_response.status == 200
    verification = {"mode": DATA_MODE, "routes": []}
    page.evaluate("window.coverageMembershipDocument = 'same'")
    for route in routes_response.json()["data"]:
        identifier = str(route["route_id"])
        catalogue_response = page.request.get(dashboard_url + "/bff/api/v1/stops?route_id=" + quote(identifier) + "&limit=2000")
        coverage_response = page.request.get(dashboard_url + "/bff/api/v1/routes/" + quote(identifier) + "/coverage?limit=2000")
        assert catalogue_response.status == coverage_response.status == 200
        catalogue = catalogue_response.json()["data"]
        coverage_payload = coverage_response.json()
        assert coverage_payload["meta"]["mode"] == DATA_MODE
        data = coverage_payload["data"]
        expected_ids = {str(stop["stop_id"]) for stop in catalogue}
        actual_ids = {str(stop["stop_id"]) for stop in data["stops"]}
        assert len(data["stops"]) == len(actual_ids) == len(expected_ids) == data["counts"]["catalogue"]
        assert actual_ids == expected_ids
        assert not data["truncated"]
        assert sum(data["counts"][key] for key in ("fresh", "stale", "no_reading")) == len(catalogue)
        for stop in data["stops"]:
            if stop["reading_status"] != "fresh":
                assert all(stop[field] is None for field in ("delay_mean_seconds", "headway_seconds", "scheduled_headway_seconds", "health_score"))
        page.get_by_role("combobox", name="Route", exact=True).select_option(identifier)
        expect(page.get_by_test_id("coverage-context")).to_contain_text(f"Route {identifier} ·", timeout=45000)
        table = page.get_by_role("table", name="Platform coverage", exact=True)
        expect(table.get_by_role("row")).to_have_count(len(catalogue) + 1)
        count = page.get_by_test_id("metric-card").filter(has_text="Catalogue platforms").get_by_test_id("metric-value")
        expect(count).to_have_text(str(len(catalogue)))
        displayed_ids = set(table.locator("tbody tr td:nth-child(2)").all_text_contents())
        assert displayed_ids == expected_ids
        assert page.evaluate("window.coverageMembershipDocument") == "same"
        verification["routes"].append({"route_id": identifier, "catalogue_count": len(catalogue),
            "counts": data["counts"], "api_ids": sorted(actual_ids), "ui_ids": sorted(displayed_ids),
            "mode": coverage_payload["meta"]["mode"], "as_of": coverage_payload["meta"].get("as_of")})
        if identifier == "1":
            with page.expect_download() as download:
                page.get_by_role("button", name="Download Platform coverage CSV", exact=True).click()
            output = tmp_path / download.value.suggested_filename
            download.value.save_as(output)
            exported = list(csv.DictReader(output.read_text().splitlines()))
            assert {row["stop_id"] for row in exported} == expected_ids
            assert len(exported) == len(catalogue)
            assert all(row["data_mode"] == DATA_MODE for row in exported)
            (screenshot_dir / (DATA_MODE + "-route-1-platform-coverage.csv")).write_text(output.read_text())
    (screenshot_dir / (DATA_MODE + "-coverage-verification.json")).write_text(json.dumps(verification, indent=2) + "\n")
    expect(page.get_by_role("heading", name="Recent reading history", exact=True)).to_be_visible()
    page.screenshot(path=str(screenshot_dir / (DATA_MODE + "-complete-platform-coverage.png")), full_page=True)


@pytest.mark.feature("F009")
@pytest.mark.case("F009-COL05")
def test_launched_model_routes_removed_and_about_clean(page, dashboard_url):
    calls = []
    page.on("request", lambda request: calls.append(request.url) if "/bff/api/v1/" in request.url else None)
    page.goto(dashboard_url + "/?view=model")
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible(timeout=45000)
    wait_fetched(page)
    links = page.get_by_role("navigation", name="Main navigation").get_by_role("link")
    expect(links).to_have_count(5)
    page.evaluate("window.retiredModelDocument = 'same'")
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="About", exact=True).click()
    expect(page.get_by_role("heading", name="About Data Horizon", exact=True)).to_be_visible()
    expect(page.get_by_role("table", name="Page guide")).not_to_contain_text("Model experiment")
    expect(page.get_by_text("Live, recorded and synthetic data are labelled separately.", exact=False)).to_have_count(0)
    expect(page.get_by_text("Data Horizon is not an official MTA travel service.", exact=False)).to_have_count(0)
    page.go_back()
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible(timeout=45000)
    assert page.evaluate("window.retiredModelDocument") == "same"
    assert not any(re.search(r"/(model/status|predictions)(\?|$)", call) for call in calls)
    for path in ("model/status", "predictions"):
        assert page.request.get(dashboard_url + "/bff/api/v1/" + path).status == 404
