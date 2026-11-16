"""The retired model has no operational command or setup side effect."""
import json
from pathlib import Path
import re

import pytest

from data_horizon.cli import main
from scripts.prepare_runtime import MOUNTS, prepare

ROOT = Path(__file__).resolve().parents[3]
pytestmark = [pytest.mark.case('F009-COL05'), pytest.mark.evidence('synthetic', 'unit')]


@pytest.mark.parametrize('command', ['train', 'prepare-model'])
def test_retired_command_is_rejected_before_loading_state(command, monkeypatch):
    def unexpected_state():
        raise AssertionError('A retired command must not load application state')
    monkeypatch.setattr('data_horizon.cli.Settings.load', unexpected_state)
    with pytest.raises(SystemExit) as stopped:
        main([command, '--input', 'unused.jsonl'])
    assert stopped.value.code == 2


def test_operations_help_has_only_supported_commands(capsys):
    with pytest.raises(SystemExit) as stopped:
        main(['--help'])
    assert stopped.value.code == 0
    text = capsys.readouterr().out
    assert all(command in text for command in ('backfill', 'graph', 'source-check', 'evaluate'))
    assert all(command not in text for command in ('train', 'prepare-model', 'full-grid'))


def test_evaluate_reports_pipeline_without_a_model_accessor(monkeypatch, capsys):
    class PipelineOnly:
        def pipeline_status(self):
            return {'counts': {'events': 3}, 'streaming': {'query_active': True}}
        def __getattr__(self, name):
            raise AssertionError(f'Unexpected retired accessor: {name}')
    monkeypatch.setattr('data_horizon.cli.Store', PipelineOnly)
    main(['evaluate'])
    result = json.loads(capsys.readouterr().out)
    assert result['pipeline']['streaming']['query_active'] is True
    assert 'model' not in result and 'experiment' not in result


def runtime_payload():
    return {name: 'isolated-test-value-' + name for files in MOUNTS.values() for name in files}


def test_new_runtime_does_not_create_model_storage(tmp_path):
    data = tmp_path / 'data'
    prepare(runtime_payload(), tmp_path / 'runtime', data, tmp_path / 'monitoring', chown=lambda *_: None)
    assert not (data / 'models').exists()
    assert all((data / name).is_dir() for name in ('raw', 'parquet', 'curated', 'checkpoints', 'outbox'))


def test_runtime_setup_preserves_existing_model_files(tmp_path):
    data = tmp_path / 'data'
    old = data / 'models' / 'existing-file'
    old.parent.mkdir(parents=True)
    old.write_text('preserved user content')
    prepare(runtime_payload(), tmp_path / 'runtime', data, tmp_path / 'monitoring', chown=lambda *_: None)
    assert old.read_text() == 'preserved user content'


def test_make_and_compose_have_no_model_entry_points():
    make = (ROOT / 'Makefile').read_text()
    targets = set(re.findall(r'^([A-Za-z0-9_-]+):', make, flags=re.MULTILINE))
    assert not {'train', 'prepare-model'} & targets
    assert 'MODEL_REVIEW_PUBLIC_KEYS_JSON' not in (ROOT / 'compose.yaml').read_text()
    assert 'MODEL_REVIEW_PUBLIC_KEYS_JSON' not in (ROOT / '.env.example').read_text()
