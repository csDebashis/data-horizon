"""Fresh Live checks for the three reproduced UI defects; no server faults or mode changes."""
import os
import re

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.live, pytest.mark.feature("F011"),
              pytest.mark.evidence("live", "browser"),
              pytest.mark.skipif(os.getenv("RUN_LIVE_BROWSER") != "1", reason="Set RUN_LIVE_BROWSER=1 for Live verification")]


def settled(page):
    expect(page.get_by_test_id("loading-state")).to_have_count(0, timeout=45000)
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Updating data", timeout=45000)
    expect(page.get_by_test_id("source-status")).to_contain_text("Data: live")


@pytest.mark.case("F011-COL02")
def test_live_route_platform_back_forward_state(page, dashboard_url, screenshot_dir):
    page.goto(dashboard_url + "/?view=route&route=2")
    settled(page)
    payload = page.request.get(dashboard_url + "/bff/api/v1/routes/2/coverage?limit=2000").json()
    assert payload["meta"]["mode"] == "live"
    target = next((row for row in payload["data"]["stops"] if row["stop_id"] == "257S"), payload["data"]["stops"][-1])
    label = f'{target["stop_name"]} ({target["stop_id"]})'
    platform = page.get_by_role("combobox", name="Platform", exact=True)
    platform.fill(target["stop_id"])
    page.get_by_role("option", name=label, exact=True).click()
    page.get_by_label("Search platforms", exact=True).fill(target["stop_id"])
    page.get_by_role("combobox", name="Reading status", exact=True).select_option(target["reading_status"])
    page.evaluate("window.liveRecoveryDocument = 'same'")
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="Graph and what-if", exact=True).click()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    page.go_back()
    settled(page)
    expect(platform).to_have_value(label)
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value(target["stop_id"])
    expect(page.get_by_role("combobox", name="Reading status", exact=True)).to_have_value(target["reading_status"])
    page.go_forward()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    page.go_back()
    settled(page)
    expect(platform).to_have_value(label)
    assert page.evaluate("window.liveRecoveryDocument") == "same"
    page.evaluate("window.scrollTo(0,0)")
    page.screenshot(path=str(screenshot_dir / "live-back-restored-platform.png"), full_page=True)


@pytest.mark.case("F011-COL01")
def test_live_scenario_error_recovery_with_browser_local_fault(page, dashboard_url, screenshot_dir):
    page.goto(dashboard_url + "/?view=scenario&route=1")
    settled(page)
    with page.expect_response(lambda response: response.url.endswith("/bff/api/v1/what-if") and response.request.method == "POST") as success:
        page.get_by_role("button", name="Run scenario", exact=True).click()
    assert success.value.status == 200
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("20")
    page.route("**/bff/api/v1/what-if", lambda route: route.abort("failed"))
    page.get_by_role("button", name="Run scenario", exact=True).click()
    error = page.get_by_role("alert").filter(has_text="Failed to fetch")
    expect(error).to_contain_text("Run scenario")
    expect(error).not_to_contain_text("Refresh data")
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_have_count(0)
    page.screenshot(path=str(screenshot_dir / "live-graph-browser-fault-guidance.png"), full_page=True)
    page.unroute("**/bff/api/v1/what-if")
    page.get_by_role("button", name="Refresh data", exact=True).click()
    settled(page)
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_have_count(0)
    with page.expect_response(lambda response: response.url.endswith("/bff/api/v1/what-if") and response.request.method == "POST") as retry:
        page.get_by_role("button", name="Run scenario", exact=True).click()
    assert retry.value.status == 200
    assert retry.value.json()["data"]["delay_seconds"] == 1200
    expect(error).to_have_count(0)
    page.screenshot(path=str(screenshot_dir / "live-graph-retry-success.png"), full_page=True)


@pytest.mark.case("F011-COL01")
def test_live_serving_database_label(page, dashboard_url, screenshot_dir):
    page.goto(dashboard_url + "/?view=pipeline")
    settled(page)
    expect(page.get_by_text("Serving database: postgresql", exact=False)).to_be_visible()
    expect(page.get_by_text("Engine: postgresql", exact=False)).to_have_count(0)
    page.screenshot(path=str(screenshot_dir / "live-pipeline-database-label.png"), full_page=True)
