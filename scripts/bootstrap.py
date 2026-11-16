"""Create private database and service keys once. Existing files are preserved."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
from urllib.parse import quote

from dotenv import dotenv_values


def effective_environ(root: Path, environ=None) -> dict[str, str]:
    values = {key: value for key, value in dotenv_values(root / '.env').items() if value is not None}
    values.update(dict(os.environ if environ is None else environ))
    return values


def bootstrap(root: Path) -> list[str]:
    root = Path(root)
    folder = root / '.secrets'
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    folder.chmod(0o700)
    created = []

    def put(name: str, value: str) -> str:
        path = folder / name
        if not path.exists():
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, 'w') as stream:
                stream.write(value + '\n')
            created.append(name)
        path.chmod(0o600)
        return path.read_text().strip()

    passwords = {role: put(role + '_password', secrets.token_urlsafe(36))
                 for role in ('postgres', 'migration', 'writer', 'reader')}
    for filename, role, username in (
        ('database_url', 'writer', 'horizon_writer'),
        ('api_database_url', 'reader', 'horizon_reader'),
        ('migration_database_url', 'migration', 'horizon_migrator'),
    ):
        put(filename, f"postgresql://{username}:{quote(passwords[role], safe='')}@postgres:5432/horizon")
    put('test_database_url', f"postgresql://horizon_migrator:{quote(passwords['migration'], safe='')}@postgres:5432/horizon_test")
    put('api_token', secrets.token_urlsafe(48))
    put('public_api_token', secrets.token_urlsafe(48))
    put('backup_key', secrets.token_hex(32))
    environment = root / '.env'
    if not environment.exists():
        descriptor = os.open(environment, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w') as stream:
            stream.write('DATA_MODE=live\nAPP_ENV=development\n')
    return created


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    names = bootstrap(parser.parse_args().root)
    print(f'Prepared {len(names)} new private files. Existing credentials were preserved.')
