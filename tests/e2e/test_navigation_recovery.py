"""Focused regression for defects reproduced on the fresh Live baseline."""
import re

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F011"), pytest.mark.evidence("synthetic", "browser")]


def settled(page):
    expect(page.get_by_test_id("loading-state")).to_have_count(0, timeout=30000)
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Updating data")


@pytest.mark.case("F011-COL02")
def test_back_forward_restores_only_matching_route_source_filters(page, dashboard_url, sample_api):
    page.goto(dashboard_url + "/?view=route&route=2")
    settled(page)
    page.evaluate("window.navigationRecoveryDocument = 'same'; history.replaceState({...history.state, frameworkProbe: 'kept'}, '', location.href)")
    platform = page.get_by_role("combobox", name="Platform", exact=True)
    platform.fill("102N")
    page.get_by_role("option", name="Second Station (102N)", exact=True).click()
    page.get_by_label("Search platforms", exact=True).fill("102N")
    page.get_by_role("combobox", name="Reading status", exact=True).select_option("fresh")
    assert page.evaluate("history.state.frameworkProbe") == "kept"
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="Graph and what-if", exact=True).click()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    page.go_back()
    settled(page)
    expect(platform).to_have_value("Second Station (102N)")
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value("102N")
    expect(page.get_by_role("combobox", name="Reading status", exact=True)).to_have_value("fresh")
    page.go_forward()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    page.go_back()
    settled(page)
    expect(platform).to_have_value("Second Station (102N)")
    page.get_by_role("combobox", name="Route", exact=True).select_option("1")
    settled(page)
    expect(platform).to_have_value("All platforms")
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value("")
    page.go_back()
    settled(page)
    expect(platform).to_have_value("Second Station (102N)")
    sample_api["mode"] = "recorded"
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(page.get_by_test_id("source-status")).to_contain_text("Data: recorded")
    settled(page)
    expect(platform).to_have_value("All platforms")
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value("")
    page.evaluate("history.replaceState({...history.state, dataHorizonRouteV1: {route:'2',scope:'route:recorded',platform:'../bad',search:'x'.repeat(101),status:'invalid'}}, '', location.href)")
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="Graph and what-if", exact=True).click()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    page.go_back()
    settled(page)
    expect(platform).to_have_value("All platforms")
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value("")
    expect(page.get_by_role("combobox", name="Reading status", exact=True)).to_have_value("all")
    assert not any(".." in str(call[2].get("stop_id", [])) for call in sample_api["calls"])
    assert page.evaluate("window.navigationRecoveryDocument") == "same"


@pytest.mark.case("F011-COL01")
def test_scenario_failure_has_correct_retry_and_no_stale_success(page, dashboard_url, sample_api):
    page.goto(dashboard_url + "/?view=scenario&route=1")
    settled(page)
    page.get_by_role("button", name="Run scenario", exact=True).click()
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_be_visible()
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("20")
    sample_api["fail"].add("what-if")
    page.get_by_role("button", name="Run scenario", exact=True).click()
    error = page.get_by_role("alert").filter(has_text="Synthetic test: API unavailable")
    expect(error).to_contain_text("Run scenario")
    expect(error).not_to_contain_text("Use Refresh data")
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_have_count(0)
    sample_api["fail"].clear()
    before = len([call for call in sample_api["calls"] if call[0] == "POST"])
    page.get_by_role("button", name="Refresh data", exact=True).click()
    settled(page)
    assert len([call for call in sample_api["calls"] if call[0] == "POST"]) == before
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_have_count(0)
    page.get_by_role("button", name="Run scenario", exact=True).click()
    expect(page.get_by_role("table", name="Scenario results", exact=True)).to_contain_text("1,200")
    expect(error).to_have_count(0)


@pytest.mark.case("F011-COL01")
def test_pipeline_names_database_separately_from_processing(page, dashboard_url, sample_api):
    page.goto(dashboard_url + "/?view=pipeline")
    settled(page)
    expect(page.get_by_text("Serving database: postgresql", exact=False)).to_be_visible()
    expect(page.get_by_text("Engine: postgresql", exact=False)).to_have_count(0)
