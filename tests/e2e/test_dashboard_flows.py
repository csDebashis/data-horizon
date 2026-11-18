"""Controlled browser checks for F011-COL01/02/03. Fixtures are explicitly synthetic."""
from __future__ import annotations

import csv
import re

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F011"), pytest.mark.evidence("synthetic", "browser")]

PAGES = [("Network overview", "Network overview"), ("Route analysis", "Route analysis"),
         ("Graph and what-if", "Graph and what-if"), ("Pipeline", "Pipeline"),
         ("About", "About Data Horizon")]


def navigate(page, label):
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name=label, exact=True).click()


def wait_fetched(page):
    expect(page.get_by_test_id("loading-state")).to_have_count(0, timeout=30000)
    expect(page.get_by_test_id("page-last-fetched")).to_be_visible(timeout=30000)
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Updating data")


@pytest.mark.case("F011-COL01")
def test_five_pages_default_and_no_product_features(page, dashboard_url, sample_api, screenshot_dir):
    page.goto(dashboard_url)
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible()
    wait_fetched(page)
    page.evaluate("window.pageDocumentMarker = 'unchanged'")
    for label, heading in PAGES:
        navigate(page, label)
        expect(page.get_by_role("heading", name=heading, exact=True)).to_be_visible()
        if label != "About":
            wait_fetched(page)
        assert page.evaluate("window.pageDocumentMarker") == "unchanged"
        page.screenshot(path=str(screenshot_dir / f"synthetic-{label.lower().replace(' ', '-')}.png"), full_page=True)
    for text in ["Sign in", "Sign out", "Notifications", "Incident desk", "Administration", "Your commute", "Model experiment"]:
        expect(page.get_by_role("button", name=text, exact=True)).to_have_count(0)
        expect(page.get_by_role("link", name=text, exact=True)).to_have_count(0)
    assert not any(re.search(r"auth|notifications|subscriptions|analysis/|model/status|predictions", call[1]) for call in sample_api["calls"])


@pytest.mark.case("F011-COL01")
def test_route_station_chart_and_real_csv(page, dashboard_url, sample_api, tmp_path):
    page.goto(dashboard_url + "/?view=route&route=1")
    expect(page.get_by_test_id("selection-context")).to_contain_text("Route 1")
    control = page.get_by_role("combobox", name="Platform", exact=True)
    control.fill("102N")
    page.get_by_role("option", name="Second Station (102N)", exact=True).click()
    expect(page.get_by_test_id("selection-context")).to_contain_text("Second Station")
    expect(page.get_by_role("img", name=re.compile("Timetable difference \\(seconds\\) over Time"))).to_be_visible()
    with page.expect_download() as download:
        page.get_by_role("button", name="Download Route analytics CSV", exact=True).click()
    output = tmp_path / download.value.suggested_filename
    download.value.save_as(output)
    data = output.read_text()
    assert "102N" in data and "101N" not in data
    assert "2026-11-19 12:00:00 +05:30 [Asia/Kolkata]" in data
    page.get_by_role("combobox", name="Route", exact=True).select_option("2")
    expect(page.get_by_test_id("selection-context")).to_contain_text("Route 2")
    expect(page.get_by_test_id("selection-context")).not_to_contain_text("Second Station")


@pytest.mark.case("F011-COL01")
def test_graph_result_and_bounded_form(page, dashboard_url, sample_api, screenshot_dir):
    page.goto(dashboard_url + "/?view=scenario&route=1")
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("15")
    page.get_by_role("spinbutton", name=re.compile(r"^Time horizon \(minutes\)")).fill("20")
    page.get_by_role("button", name="Run scenario", exact=True).click()
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_be_visible()
    expect(page.get_by_role("img", name="Directed station connection path", exact=True)).to_be_visible()
    expect(page.get_by_role("table", name="Scenario results", exact=True)).to_contain_text("900")
    expect(page.get_by_role("table", name="Scenario results", exact=True)).to_contain_text("Second Station")
    page.screenshot(path=str(screenshot_dir / "synthetic-graph-result.png"), full_page=True)
    calls = len([item for item in sample_api["calls"] if item[1] == "what-if"])
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("121")
    assert not page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).evaluate("element => element.checkValidity()")
    page.get_by_role("button", name="Run scenario", exact=True).click()
    assert len([item for item in sample_api["calls"] if item[1] == "what-if"]) == calls


@pytest.mark.case("F011-COL02")
def test_manual_and_automatic_fetch_preserve_document_filters_and_draft(page, dashboard_url, sample_api):
    page.clock.install()
    page.goto(dashboard_url + "/?view=scenario&route=1")
    wait_fetched(page)
    page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)")).fill("17")
    page.evaluate("window.pageDocumentMarker = 'same'")
    first = page.get_by_test_id("page-last-fetched").get_attribute("datetime")
    before = len(sample_api["calls"])
    page.clock.fast_forward(31000)
    expect(page.get_by_test_id("page-last-fetched")).not_to_have_attribute("datetime", first)
    assert len(sample_api["calls"]) > before
    assert page.evaluate("window.pageDocumentMarker") == "same"
    expect(page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)"))).to_have_value("17")
    assert not any(item[1] in {"anomalies", "pipeline/status", "model/status"} for item in sample_api["calls"])
    before = len(sample_api["calls"])
    page.get_by_role("button", name="Refresh data", exact=True).click()
    wait_fetched(page)
    assert len(sample_api["calls"]) > before
    assert page.evaluate("window.pageDocumentMarker") == "same"
    expect(page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)"))).to_have_value("17")
    expect(page.get_by_test_id("local-zone")).to_have_text("Asia/Kolkata")
    assert "+05:30" in page.get_by_test_id("page-last-fetched").inner_text()


@pytest.mark.case("F011-COL02")
def test_api_outage_retains_same_query_and_reports_failed_fetch(page, dashboard_url, sample_api):
    page.goto(dashboard_url + "/?view=route&route=1")
    wait_fetched(page)
    first = page.get_by_test_id("page-last-fetched").get_attribute("datetime")
    sample_api["fail"].add("routes/1/metrics")
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(page.get_by_role("alert").filter(has_text="Synthetic test: API unavailable")).to_be_visible()
    expect(page.get_by_test_id("page-fetch-status")).to_contain_text("Some data could not be refreshed")
    expect(page.get_by_test_id("selection-context")).to_contain_text("Route 1")
    assert page.get_by_test_id("page-last-fetched").get_attribute("datetime") == first
    sample_api["fail"].clear()
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Some data could not be refreshed")


def choose_theme(page, label):
    page.get_by_role("combobox", name="Appearance", exact=True).click()
    page.get_by_role("option", name=label, exact=True).click()


@pytest.mark.case("F011-COL03")
def test_theme_matches_sidebar_persists_and_follows_device(page, dashboard_url, sample_api, screenshot_dir):
    page.goto(dashboard_url + "/?view=route&route=1")
    wait_fetched(page)
    for label, scheme, colour in [("Daylight", "light", "rgb(237, 244, 249)"), ("Midnight", "dark", "rgb(7, 19, 33)")]:
        choose_theme(page, label)
        expect(page.locator("html")).to_have_attribute("data-color-scheme", scheme)
        drawer = page.locator(".MuiDrawer-paper:visible")
        expect(drawer).to_have_css("background-color", colour)
        expected_primary = "rgb(0, 107, 98)" if scheme == "light" else "rgb(89, 218, 204)"
        expect(page.get_by_role("button", name="Download Route analytics CSV", exact=True)).to_have_css("color", expected_primary)
        page.screenshot(path=str(screenshot_dir / f"synthetic-{scheme}.png"), full_page=True)
    choose_theme(page, "Daylight")
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-color-scheme", "light")
    choose_theme(page, "Use device theme")
    page.emulate_media(color_scheme="dark")
    expect(page.locator("html")).to_have_attribute("data-color-scheme", "dark")
    page.emulate_media(color_scheme="light")
    expect(page.locator("html")).to_have_attribute("data-color-scheme", "light")


@pytest.mark.case("F011-COL03")
def test_mobile_drawer_route_focus_no_overlap_and_keyboard_skip(page, dashboard_url, sample_api, screenshot_dir):
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto(dashboard_url + "/?view=route&route=1")
    wait_fetched(page)
    page.get_by_role("button", name="Open navigation", exact=True).click()
    drawer = page.locator(".MuiDrawer-paper:visible")
    expect(drawer.get_by_role("navigation")).to_be_visible()
    expect(drawer).to_have_css("transform", "none")
    select = drawer.get_by_role("combobox", name="Route", exact=True)
    expect(select).to_be_visible()
    label_box = drawer.get_by_text("Route", exact=True).bounding_box()
    select_box = select.bounding_box()
    assert label_box["y"] + label_box["height"] <= select_box["y"]
    page.screenshot(path=str(screenshot_dir / "synthetic-mobile-drawer.png"), full_page=True)
    drawer.get_by_role("link", name="Graph and what-if", exact=True).click()
    expect(page.get_by_role("heading", name="Graph and what-if", exact=True)).to_be_visible()
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    page.keyboard.press("Tab")
    page.screenshot(path=str(screenshot_dir / "synthetic-mobile-graph.png"), full_page=True)


@pytest.mark.case("F011-COL02")
def test_initial_source_gate_and_later_scope_change(page, dashboard_url, sample_api):
    sample_api["hold_status"] = True
    page.goto(dashboard_url + "/?view=scenario&route=1")
    expect(page.get_by_test_id("loading-state")).to_contain_text("Loading the data source")
    expect(page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)"))).to_have_count(0)
    assert sample_api["held_status"], "The first status response must still be held"
    sample_api["release_status"]()
    wait_fetched(page)
    page.evaluate("window.sourceGateDocument = 'same'")
    delay = page.get_by_role("spinbutton", name=re.compile(r"^Delay \(minutes\)"))
    delay.fill("15")
    page.get_by_role("button", name="Run scenario", exact=True).click()
    expect(page.get_by_role("table", name="Scenario results", exact=True)).to_contain_text("900")
    # The fixture now simulates a real source-mode change, rather than another poll in the same mode.
    sample_api["mode"] = "recorded"
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(page.get_by_test_id("source-status")).to_contain_text("Data: recorded")
    wait_fetched(page)
    expect(delay).to_have_value("10")
    expect(page.get_by_role("heading", name="Scenario results", exact=True)).to_have_count(0)
    assert page.evaluate("window.sourceGateDocument") == "same"


@pytest.mark.case("F011-COL01")
def test_missing_exact_schedule_keeps_valid_arrival_gap(page, dashboard_url, sample_api, screenshot_dir):
    sample_api["unmatched_schedule"] = True
    page.goto(dashboard_url + "/?view=route&route=1")
    wait_fetched(page)
    cards = page.get_by_test_id("metric-card")
    expect(cards.filter(has_text="Average timetable difference")).to_contain_text("Unavailable")
    expect(cards.filter(has_text="Average recorded health")).to_contain_text("Unavailable")
    expect(cards.filter(has_text="Average train interval")).to_contain_text("190 seconds")
    expect(page.get_by_role("alert").filter(has_text="Timetable difference compares the arrival time with an exact timetable match")).to_be_visible()
    expect(page.get_by_role("alert").filter(has_text="Train intervals can still be shown")).to_be_visible()
    expect(page.get_by_role("img", name=re.compile("Headway \\(seconds\\) over Time"))).to_be_visible()
    table = page.get_by_role("table", name="Route analytics", exact=True)
    expect(table).to_contain_text("unknown")
    expect(table).not_to_contain_text("-17,280")
    page.screenshot(path=str(screenshot_dir / "synthetic-exact-schedule-required.png"), full_page=True)


@pytest.mark.case("F011-COL04")
def test_complete_platform_coverage_search_status_csv_and_quiet_history(page, dashboard_url, sample_api, screenshot_dir, tmp_path):
    page.clock.install()
    page.goto(dashboard_url + "/?view=route&route=1")
    wait_fetched(page)
    table = page.get_by_role("table", name="Platform coverage", exact=True)
    expect(table.get_by_role("row")).to_have_count(6)
    for identifier in ["101N", "102N", "103N", "104N", "105N"]:
        expect(table).to_contain_text(identifier)
    for label, value in [("Catalogue platforms", "5"), ("With fresh reading", "2"),
                         ("With old reading", "1"), ("No reading", "2"), ("With exact-match delay", "2")]:
        expect(page.get_by_test_id("metric-card").filter(has_text=label).get_by_test_id("metric-value")).to_have_text(value)
    expect(page.get_by_role("alert").filter(has_text="5-platform example network")).to_be_visible()
    page.get_by_label("Search platforms", exact=True).fill("Quiet")
    page.get_by_role("combobox", name="Reading status", exact=True).select_option("no_reading")
    expect(table.get_by_role("row")).to_have_count(3)
    expect(table).to_contain_text("Quiet Station")
    expect(table).to_contain_text("Unavailable")
    with page.expect_download() as download:
        page.get_by_role("button", name="Download Platform coverage CSV", exact=True).click()
    output = tmp_path / download.value.suggested_filename
    download.value.save_as(output)
    text = output.read_text()
    assert "104N" in text and "105N" in text and "101N" not in text
    assert '"no_reading"' in text
    assert all(row["data_mode"] == "synthetic" for row in csv.DictReader(text.splitlines()))
    platform = page.get_by_role("combobox", name="Platform", exact=True)
    platform.fill("104N")
    page.get_by_role("option", name="Quiet Station (104N)", exact=True).click()
    expect(page.get_by_test_id("selection-context")).to_contain_text("Quiet Station")
    expect(page.get_by_role("alert").filter(has_text="The platform remains in the catalogue above")).to_be_visible()
    # Keep both coverage controls, the selected history platform, document and scroll through a real poll.
    page.evaluate("window.coverageDocument = 'same'; window.scrollTo(0, 220)")
    scroll = page.evaluate("window.scrollY")
    before = page.get_by_test_id("page-last-fetched").get_attribute("datetime")
    page.clock.fast_forward(31000)
    expect(page.get_by_test_id("page-last-fetched")).not_to_have_attribute("datetime", before)
    expect(page.get_by_label("Search platforms", exact=True)).to_have_value("Quiet")
    expect(page.get_by_role("combobox", name="Reading status", exact=True)).to_have_value("no_reading")
    expect(page.get_by_test_id("selection-context")).to_contain_text("Quiet Station")
    assert page.evaluate("window.coverageDocument") == "same"
    assert abs(page.evaluate("window.scrollY") - scroll) <= 2
    page.get_by_label("Search platforms", exact=True).fill("")
    page.get_by_role("combobox", name="Reading status", exact=True).select_option("stale")
    expect(table.get_by_role("row")).to_have_count(2)
    expect(table).to_contain_text("Old Station")
    expect(table).to_contain_text("Unavailable")
    page.get_by_role("combobox", name="Reading status", exact=True).select_option("all")
    expect(table.get_by_role("row")).to_have_count(6)
    with page.expect_download() as full_download:
        page.get_by_role("button", name="Download Platform coverage CSV", exact=True).click()
    full_output = tmp_path / "all-platforms.csv"
    full_download.value.save_as(full_output)
    exported = list(csv.DictReader(full_output.read_text().splitlines()))
    assert len(exported) == 5
    assert all(row["data_mode"] == "synthetic" for row in exported)
    assert next(row for row in exported if row["stop_id"] == "101N")["time_basis"] == "observed"
    page.evaluate("window.scrollTo(0, 0)")
    page.screenshot(path=str(screenshot_dir / "synthetic-platform-coverage.png"), full_page=True)


@pytest.mark.case("F011-COL04")
def test_first_coverage_failure_does_not_claim_empty_filters(page, dashboard_url, sample_api):
    sample_api["fail"].add("routes/1/coverage")
    page.goto(dashboard_url + "/?view=route&route=1")
    expect(page.get_by_role("alert").filter(has_text="The platform catalogue could not be loaded")).to_be_visible()
    expect(page.get_by_text("No platforms match these filters.", exact=False)).to_have_count(0)
    expect(page.get_by_label("Search platforms", exact=True)).to_have_count(0)
    expect(page.get_by_role("combobox", name="Platform", exact=True)).to_be_disabled()
    sample_api["fail"].clear()
    page.get_by_role("button", name="Refresh data", exact=True).click()
    wait_fetched(page)
    expect(page.get_by_role("table", name="Platform coverage", exact=True).get_by_role("row")).to_have_count(6)
    expect(page.get_by_role("alert").filter(has_text="The platform catalogue could not be loaded")).to_have_count(0)
