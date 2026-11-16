"""Check supplied VM settings; this does not certify a VM deployment."""
from pathlib import Path
import re

try:
    from scripts.bootstrap import effective_environ
except ModuleNotFoundError:
    from bootstrap import effective_environ


def validate(environ: dict[str, str], root: Path | None = None) -> list[str]:
    problems = []
    for name in ('API_IMAGE', 'COLLECTOR_IMAGE', 'SPARK_IMAGE', 'DASHBOARD_IMAGE', 'OPS_IMAGE'):
        if not re.search(r'@sha256:[a-f0-9]{64}$', environ.get(name, '')):
            problems.append(f'{name} must identify a tested immutable image digest')
    if not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{1,251})\.[a-z]{2,63}', environ.get('DOMAIN', '')):
        problems.append('DOMAIN must be a real DNS name')
    if environ.get('DATA_MODE', 'live') != 'live':
        problems.append('Production deployment must explicitly use live data')
    for name in ('BACKUP_DESTINATION', 'BACKUP_KEY_FILE'):
        if not environ.get(name):
            problems.append(f'{name} is required')
    return problems


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    errors = validate(effective_environ(root), root)
    if not (root / '.secrets/api_token').is_file():
        errors.append('Run setup to create a private API token')
    if errors:
        print('VM settings incomplete:\n- ' + '\n- '.join(errors))
        raise SystemExit(1)
    print('Settings passed. Actual VM, HTTPS and off-host restore tests are still required.')
