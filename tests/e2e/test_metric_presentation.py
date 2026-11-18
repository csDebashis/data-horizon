"""Controlled UI evidence for signed timetable readings and missing-data boundaries."""
from __future__ import annotations

import csv

import pytest
from playwright.sync_api import expect

pytestmark = [pytest.mark.browser, pytest.mark.feature("F007"),
              pytest.mark.case("F007-COL03"), pytest.mark.evidence("synthetic", "browser")]


def test_history_preserves_signed_readings_and_explains_the_average(page, dashboard_url, sample_api, tmp_path):
    instant = "2026-11-19T06:30:00Z"
    rows = [{"window_start": instant, "stop_id": "101N", "delay_mean_seconds": -60,
             "headway_seconds": 100, "health_score": 80, "event_count": 100, "time_basis": "predicted"},
            {"window_start": "2026-11-19T06:31:00Z", "stop_id": "101N", "delay_mean_seconds": None,
             "headway_seconds": None, "health_score": None, "event_count": 2, "time_basis": "predicted"},
            {"window_start": "2026-11-19T06:32:00Z", "stop_id": "101N", "delay_mean_seconds": 20,
             "headway_seconds": 200, "health_score": 60, "event_count": 1, "time_basis": "predicted"}]
    page.route("**/bff/api/v1/routes/1/metrics?**", lambda route: route.fulfill(json={"data": rows,
               "meta": {"mode": "synthetic", "as_of": instant, "freshness_seconds": 2,
                        "stale": False, "stale_after_seconds": 120, "time_basis": "predicted"}}))
    page.goto(dashboard_url + "/?view=route&route=1")
    card = page.get_by_test_id("metric-card").filter(has_text="Average timetable difference")
    expect(card.get_by_test_id("metric-value")).to_have_text("-20 seconds")
    expect(page.get_by_test_id("metric-card").filter(has_text="Average train interval")).to_contain_text("150 seconds")
    expect(page.get_by_test_id("metric-card").filter(has_text="Average recorded health")).to_contain_text("70 / 100")
    expect(page.get_by_text("Negative means earlier than the timetable; positive means later.", exact=False)).to_be_visible()
    expect(page.get_by_text("Each displayed historical reading has equal weight", exact=False)).to_be_visible()
    chart = page.get_by_role("img", name="Timetable difference (seconds) over Time (Asia/Kolkata)", exact=True)
    expect(chart.locator("polyline")).to_have_count(2)
    expect(chart.locator("circle")).to_have_count(2)
    expect(page.get_by_role("table", name="Route analytics")).to_contain_text("Average timetable difference (seconds)")
    with page.expect_download() as download:
        page.get_by_role("button", name="Download Route analytics CSV", exact=True).click()
    output = tmp_path / download.value.suggested_filename
    download.value.save_as(output)
    exported = list(csv.DictReader(output.read_text().splitlines()))
    assert [row["delay_mean_seconds"] for row in exported] == ["-60", "", "20"]
    assert "+05:30" in exported[0]["window_start"]


def test_anomaly_count_and_list_expire_using_their_separate_source_clocks(page, dashboard_url, sample_api):
    page.clock.install()
    # The network summary is already old while the independently returned anomaly list is fresh.
    page.route("**/bff/api/v1/network/health", lambda route: route.fulfill(json={"data": {
        "health_score": None, "anomaly_count": 1, "routes": []}, "meta": {
        "mode": "synthetic", "as_of": "2026-11-19T06:25:00Z", "freshness_seconds": 301,
        "stale": True, "stale_after_seconds": 120, "time_basis": "predicted"}}))
    page.goto(dashboard_url)
    card = page.get_by_test_id("metric-card").filter(has_text="Unusual route-stop readings")
    expect(card.get_by_test_id("metric-value")).to_have_text("Unavailable")
    table = page.get_by_role("table", name="Unusual readings", exact=True)
    expect(table).to_contain_text("102N")
    expect(page.get_by_text("Unusual-reading data is old or unavailable.", exact=False)).to_have_count(0)
    page.evaluate("window.metricPresentationDocument = 'same'")
    # Polling fails; the original list receipt must age without a document reload or a fresh-data claim.
    sample_api["fail"].add("anomalies")
    page.clock.fast_forward(121000)
    expect(page.get_by_text("Unusual-reading data is old or unavailable. Any rows below are last known readings.", exact=True)).to_be_visible()
    expect(table).to_contain_text("102N")
    expect(card.get_by_test_id("metric-value")).to_have_text("Unavailable")
    assert page.evaluate("window.metricPresentationDocument") == "same"
