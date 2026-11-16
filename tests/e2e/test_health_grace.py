"""Clearly labelled health retention; fixtures are synthetic, not Live proof."""
import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F007"),
              pytest.mark.case("F007-COL02"), pytest.mark.evidence("synthetic", "browser")]


def retained_health(age=299, source="2026-11-19T06:25:03Z"):
    fallback = {"score": 73, "level": "watch", "source_as_of": source,
                "age_seconds": age, "retention_seconds": 300,
                "reason": "Waiting for current inputs."}
    return {"health_score": None, "anomaly_count": 0, "last_valid_health": fallback,
            "routes": [{"route_id": "1", "health_score": None, "health_level": "unknown", "last_valid_health": fallback},
                       {"route_id": "2", "health_score": None, "health_level": "unknown"}]}


def network_card(page):
    return page.get_by_test_id("metric-card").filter(has_text="Network health")


def test_last_valid_health_expires_without_a_new_response(page, dashboard_url, sample_api, screenshot_dir):
    page.clock.install()
    sample_api["health"] = retained_health()
    page.goto(dashboard_url)
    card = network_card(page)
    expect(card.get_by_test_id("metric-value")).to_have_text("73 / 100")
    expect(card).to_contain_text("Last valid")
    expect(card).to_contain_text("299 seconds old")
    expect(card).to_contain_text("2026-11-19 11:55:03 +05:30")
    expect(card).to_contain_text("Waiting for current inputs.")
    route = page.get_by_role("link", name="Analyse Route 1", exact=True)
    expect(route).to_contain_text("Last valid")
    expect(page.get_by_role("link", name="Analyse Route 2", exact=True)).to_contain_text("Unavailable")
    page.screenshot(path=str(screenshot_dir / "last-valid-health.png"), full_page=True)
    page.evaluate("window.healthGraceDocument = 'same'")
    sample_api["fail"].add("network/health")
    before = len([call for call in sample_api["calls"] if call[1] == "network/health"])
    page.clock.fast_forward(2000)
    expect(card.get_by_test_id("metric-value")).to_have_text("Unavailable")
    expect(card.get_by_text("Last valid", exact=False)).to_have_count(0)
    expect(route).to_contain_text("Unavailable")
    assert len([call for call in sample_api["calls"] if call[1] == "network/health"]) == before
    assert page.evaluate("window.healthGraceDocument") == "same"


def test_fetch_does_not_renew_retained_health_and_current_recovery_wins(page, dashboard_url, sample_api):
    page.clock.install()
    sample_api["health"] = retained_health()
    page.goto(dashboard_url)
    card = network_card(page)
    expect(card).to_contain_text("Last valid")
    sample_api["health"] = retained_health(age=301)
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(card.get_by_test_id("metric-value")).to_have_text("Unavailable")
    # A new current value takes priority even if a valid prior object is present.
    sample_api["health"] = retained_health()
    sample_api["health"]["health_score"] = 66
    sample_api["health"].update(health_source_as_of="2026-11-19T06:30:00Z", health_age_seconds=2,
                               health_freshness_seconds=120, health_retention_seconds=300)
    page.get_by_role("button", name="Refresh data", exact=True).click()
    expect(card.get_by_test_id("metric-value")).to_have_text("66 / 100")
    expect(card.get_by_text("Last valid", exact=False)).to_have_count(0)


def test_network_tab_remount_uses_server_last_valid_without_renewing_its_age(page, dashboard_url, sample_api):
    page.goto(dashboard_url)
    card = network_card(page)
    expect(card.get_by_test_id("metric-value")).to_have_text("80 / 100")
    page.evaluate("window.healthTabDocument = 'same'")
    nav = page.get_by_role("navigation", name="Main navigation")
    nav.get_by_role("link", name="Route analysis", exact=True).click()
    expect(page.get_by_role("heading", name="Route analysis", exact=True)).to_be_visible()
    sample_api["health"] = retained_health(age=240, source="2026-11-19T06:26:02Z")
    nav.get_by_role("link", name="Network overview", exact=True).click()
    expect(card.get_by_test_id("metric-value")).to_have_text("73 / 100")
    expect(card).to_contain_text("Last valid")
    expect(card).to_contain_text("240 seconds old")
    assert page.evaluate("window.healthTabDocument") == "same"


def test_current_health_ages_during_poll_failure_and_expires_on_return_to_tab(page, dashboard_url, sample_api):
    page.clock.install()
    page.goto(dashboard_url)
    card = network_card(page)
    expect(card.get_by_test_id("metric-value")).to_have_text("80 / 100")
    page.evaluate("window.healthOutageDocument = 'same'")
    sample_api["fail"].add("network/health")
    page.clock.fast_forward(121000)
    expect(card).to_contain_text("Last valid")
    expect(card.get_by_test_id("metric-value")).to_have_text("80 / 100")
    # The returning-tab handler must age the original receipt, not start a new allowance.
    page.evaluate("Object.defineProperty(document, 'visibilityState', {configurable: true, value: 'hidden'}); document.dispatchEvent(new Event('visibilitychange'))")
    page.clock.fast_forward(180000)
    page.evaluate("Object.defineProperty(document, 'visibilityState', {configurable: true, value: 'visible'}); document.dispatchEvent(new Event('visibilitychange'))")
    expect(card.get_by_test_id("metric-value")).to_have_text("Unavailable")
    assert page.evaluate("window.healthOutageDocument") == "same"
