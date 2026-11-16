"""Model removal and requested About cleanup, using controlled page data."""
import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F009"),
              pytest.mark.case("F009-COL05"), pytest.mark.evidence("synthetic", "browser")]


def test_five_pages_and_about_cleanup(page, dashboard_url, sample_api, screenshot_dir):
    page.goto(dashboard_url + "/?view=about")
    expect(page.get_by_role("heading", name="About Data Horizon", exact=True)).to_be_visible()
    links = page.get_by_role("navigation", name="Main navigation").get_by_role("link")
    expect(links).to_have_count(5)
    expect(page.get_by_role("link", name="Model experiment", exact=True)).to_have_count(0)
    expect(page.get_by_test_id("model-readiness")).to_have_count(0)
    expect(page.get_by_text("Live, recorded and synthetic data are labelled separately.", exact=False)).to_have_count(0)
    expect(page.get_by_text("Data Horizon is not an official MTA travel service.", exact=False)).to_have_count(0)
    expect(page.get_by_role("table", name="Page guide")).not_to_contain_text("Model experiment")
    page.screenshot(path=str(screenshot_dir / "five-pages-about.png"), full_page=True)
    assert not any(call[1] in {"model/status", "predictions"} for call in sample_api["calls"])


def test_old_model_link_and_history_fall_back_without_model_reads(page, dashboard_url, sample_api):
    page.goto(dashboard_url + "/?view=model&route=2")
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible()
    page.evaluate("window.retirementDocument = 'same'")
    page.get_by_role("navigation", name="Main navigation").get_by_role("link", name="About", exact=True).click()
    expect(page.get_by_role("heading", name="About Data Horizon", exact=True)).to_be_visible()
    page.go_back()
    expect(page.get_by_role("heading", name="Network overview", exact=True)).to_be_visible()
    assert page.evaluate("window.retirementDocument") == "same"
    assert not any(call[1] in {"model/status", "predictions"} for call in sample_api["calls"])
