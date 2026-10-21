"""The processor retains operational jobs without hidden training or inference."""
import ast

import pytest

pytestmark = [pytest.mark.case('F009-COL05'), pytest.mark.feature('F009'),
              pytest.mark.evidence('source', 'contract')]


def test_processor_rejects_retired_training_command_before_starting_spark():
    from data_horizon import processor
    with pytest.raises(SystemExit) as error:
        processor.main(['train', '--input', 'unused.jsonl'])
    assert error.value.code == 2


def test_model_modules_hooks_and_preparation_entry_are_absent(project_root):
    import tomllib

    retired = {'actual_arrivals', 'ml', 'model_dataset', 'model_provenance', 'model_tools', 'model_readiness'}
    roots = [project_root / 'services/processor/src', project_root / 'packages/core/src']
    for root in roots:
        for filename in root.rglob('*.py'):
            assert filename.stem not in retired
            for node in ast.walk(ast.parse(filename.read_text())):
                if isinstance(node, ast.ImportFrom):
                    assert not set((node.module or '').split('.')) & retired
                    assert not {name.name for name in node.names} & retired
    processor = ast.parse((roots[0] / 'data_horizon/processor.py').read_text())
    called = {node.func.id for node in ast.walk(processor)
              if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert not called & {'predict', 'refresh_active_model_state', 'publish_model_predictions', 'invalidate_model_if_current'}
    metadata = tomllib.loads((project_root / 'services/processor/pyproject.toml').read_text())
    assert 'data-horizon-prepare-model' not in metadata['project']['scripts']
    dependencies = metadata['project']['dependencies']
    assert not any(name.startswith('cryptography') for name in dependencies)
    assert 'numpy==1.26.4' in dependencies and 'pandas==2.2.3' in dependencies
    assert 'pyarrow==25.0.1' in dependencies
