"""Keep new Live evidence separate and recognise retired fields without rejecting forecasts."""

import pytest

import test_live_api_contract as live_contract

pytestmark = [pytest.mark.case('F009-COL05', 'F013-COL02'),
              pytest.mark.evidence('synthetic', 'security')]


def test_live_report_uses_a_current_path_and_accepts_explicit_run_location(tmp_path, monkeypatch):
    monkeypatch.setattr(live_contract, 'ROOT', tmp_path)
    monkeypatch.delenv('LIVE_API_REPORT_FILE', raising=False)
    default = live_contract._report_output()
    assert default.is_relative_to(tmp_path / 'reports')
    assert default != tmp_path / 'reports/project-baseline/live-api-contract.json'
    monkeypatch.setenv('LIVE_API_REPORT_FILE', 'reports/new-run/read-only-api.json')
    assert live_contract._report_output() == tmp_path / 'reports/new-run/read-only-api.json'


def test_live_report_rejects_paths_and_links_outside_reports(tmp_path, monkeypatch):
    root = tmp_path / 'repo'
    root.mkdir()
    (root / 'reports').mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    (root / 'reports' / 'linked').symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(live_contract, 'ROOT', root)
    for value in ('README.md', 'reports/../README.md', str(outside / 'result.json'),
                  'reports/linked/result.json', 'reports'):
        monkeypatch.setenv('LIVE_API_REPORT_FILE', value)
        with pytest.raises(ValueError, match='reports'):
            live_contract._report_output()
    (root / 'reports' / 'linked').unlink()
    (root / 'reports').rmdir()
    (root / 'reports').symlink_to(outside, target_is_directory=True)
    monkeypatch.setenv('LIVE_API_REPORT_FILE', 'reports/result.json')
    with pytest.raises(ValueError, match='reports'):
        live_contract._report_output()


def test_retirement_scan_blocks_nested_model_fields_but_keeps_public_forecast_data():
    assert live_contract._retired_model_fields({'snapshots': [{'model_status': {'status': 'blocked'}}]})
    assert live_contract._retired_model_fields({'experiment': {}})
    assert not live_contract._retired_model_fields({'time_basis': 'predicted',
        'predicted_time': '2026-11-19T10:00:00Z', 'delay_mean_seconds': 30,
        'sources': {'transit': {'status': 'ok'}}})
