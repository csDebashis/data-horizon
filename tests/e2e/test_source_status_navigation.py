"""A pending page request is not a source-mode outage."""
import re

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F011"),
              pytest.mark.case("F011-COL02"), pytest.mark.evidence("synthetic", "browser")]


def test_navigation_keeps_confirmed_source_clock_then_reports_failure_and_new_mode(page, dashboard_url, sample_api):
    page.clock.install()
    page.goto(dashboard_url)
    badge = page.get_by_test_id("source-status")
    expect(badge).to_contain_text("Data: synthetic")
    expect(page.get_by_test_id("page-fetch-status")).not_to_contain_text("Updating data")
    page.evaluate("window.sourceNavigationDocument = 'same'")
    sample_api["hold_status"] = True
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="Route analysis", exact=True).click()
    expect(badge).to_contain_text("Data: synthetic")
    expect(badge).not_to_contain_text("Unavailable")
    page.clock.fast_forward(5000)
    age = re.search(r"Age:\s*([\d,]+) seconds", badge.inner_text())
    assert age and int(age[1].replace(",", "")) >= 7
    sample_api["fail"].add("status")
    sample_api["release_status"]()
    expect(page.get_by_role("alert").filter(has_text="Synthetic test: API unavailable")).to_be_visible()
    expect(badge).to_contain_text("Data: synthetic")
    expect(badge).to_contain_text("Last confirmed")
    sample_api["fail"].clear()
    sample_api["mode"] = "recorded"
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(badge).to_contain_text("Data: recorded")
    expect(page.get_by_role("heading", name="Route analysis", exact=True)).to_be_visible()
    assert page.evaluate("window.sourceNavigationDocument") == "same"


def test_first_source_confirmation_loading_and_real_failure_are_distinct(page, dashboard_url, sample_api):
    sample_api["hold_status"] = True
    page.goto(dashboard_url)
    badge = page.get_by_test_id("source-status")
    expect(badge).to_contain_text("Data: Loading")
    expect(badge).not_to_contain_text("Unavailable")
    sample_api["fail"].add("status")
    sample_api["release_status"]()
    expect(badge).to_contain_text("Data: Unavailable")
    expect(page.get_by_role("alert").filter(has_text="Synthetic test: API unavailable")).to_be_visible()
