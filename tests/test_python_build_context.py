"""Check actual Docker ignore semantics against stale generated Python modules."""
import os
from pathlib import Path
import subprocess

import pytest

from scripts.preflight import docker_binary

pytestmark = [pytest.mark.integration, pytest.mark.case('F009-COL05', 'F012-COL01'),
              pytest.mark.evidence('synthetic', 'integration')]
ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(os.getenv('RUN_DOCKER_CONTEXT') != '1', reason='Enable isolated real Docker context verification')
def test_real_docker_context_excludes_stale_python_builds_but_keeps_source(tmp_path):
    context, exported = tmp_path / 'context', tmp_path / 'exported'
    context.mkdir()
    (context / '.dockerignore').write_text((ROOT / '.dockerignore').read_text())
    (context / 'Dockerfile').write_text('FROM scratch\nCOPY . /\n')
    source = 'services/processor/src/data_horizon/processor.py'
    generated = [
        'services/processor/build/lib/data_horizon/ml.py',
        'packages/core/build/lib/data_horizon/model_readiness.py',
        'services/processor/dist/old.whl',
        'services/processor/src/old.egg-info/PKG-INFO',
    ]
    for name in [source, *generated]:
        path = context / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('isolated context marker\n')
    result = subprocess.run([docker_binary(), 'build', '--progress=plain', '--output',
                             f'type=local,dest={exported}', str(context)],
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr[-4000:]
    assert (exported / source).read_text() == 'isolated context marker\n'
    assert all(not (exported / name).exists() for name in generated), 'Generated Python output entered the image context'
