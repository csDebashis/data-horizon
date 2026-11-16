"""Encrypted PostgreSQL logical backup and explicit isolated restore support.

Production base backups/WAL archiving and off-host storage remain separate release gates.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

MAGIC = b"DHBACKUP1"
BLOCK = 1024 * 1024


def read_key(path: Path) -> bytes:
    if os.name == "posix" and path.stat().st_mode & 0o077:
        raise ValueError("Backup key permissions must be 0600 or stricter")
    key = bytes.fromhex(path.read_text().strip())
    if len(key) != 32:
        raise ValueError("Backup requires an independent 256-bit key")
    return key


def encrypt_stream(source, destination, key: bytes) -> None:
    nonce = secrets.token_bytes(12)
    encryptor = Cipher(algorithms.AES(key), modes.GCM(nonce)).encryptor()
    encryptor.authenticate_additional_data(MAGIC)
    destination.write(MAGIC + nonce)
    while chunk := source.read(BLOCK):
        destination.write(encryptor.update(chunk))
    destination.write(encryptor.finalize())
    destination.write(encryptor.tag)


def decrypt_stream(source, destination, key: bytes) -> None:
    if source.read(len(MAGIC)) != MAGIC:
        raise ValueError("Unsupported encrypted backup format")
    nonce = source.read(12)
    source.seek(0, io.SEEK_END)
    length = source.tell() - len(MAGIC) - 12 - 16
    if length < 0:
        raise ValueError("Truncated encrypted backup")
    source.seek(-16, io.SEEK_END)
    tag = source.read(16)
    decryptor = Cipher(algorithms.AES(key), modes.GCM(nonce, tag)).decryptor()
    decryptor.authenticate_additional_data(MAGIC)
    source.seek(len(MAGIC) + 12)
    remaining = length
    while remaining:
        chunk = source.read(min(BLOCK, remaining))
        if not chunk:
            raise ValueError("Truncated encrypted backup")
        destination.write(decryptor.update(chunk))
        remaining -= len(chunk)
    # Callers must not restore/use the plaintext until tag verification succeeds.
    destination.write(decryptor.finalize())


def docker_command() -> list[str]:
    binary = shutil.which("docker") or "/Applications/Docker.app/Contents/Resources/bin/docker"
    return [binary, "compose"]


@contextmanager
def private_plaintext():
    descriptor, filename = tempfile.mkstemp(prefix="horizon-restore-", suffix=".dump")
    os.close(descriptor)
    try:
        yield Path(filename)
    finally:
        Path(filename).unlink(missing_ok=True)


def backup(output: Path, key_file: Path, database: str = "horizon") -> dict:
    if database not in {"horizon", "horizon_test"}:
        raise ValueError("Unsupported database")
    key = read_key(key_file)
    if output.exists():
        raise FileExistsError("Backup destination already exists; choose a new filename")
    output.parent.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    process = subprocess.Popen(docker_command() + ["exec", "-T", "postgres", "pg_dump", "-U",
                               "horizon_admin", "-d", database, "-Fc"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    created = False
    try:
        descriptor = os.open(output, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        created = True
        with os.fdopen(descriptor, "wb") as destination:
            encrypt_stream(process.stdout, destination, key)
        _, error = process.communicate(timeout=300)
        if process.returncode:
            raise RuntimeError("PostgreSQL backup failed: " + error.decode(errors="replace")[:500])
    except BaseException:
        process.kill()
        process.wait()
        if created:
            output.unlink(missing_ok=True)
        raise
    result = {"database": database, "started_at": started.isoformat(),
              "completed_at": datetime.now(timezone.utc).isoformat(),
              "encryption": "AES-256-GCM", "bytes": output.stat().st_size,
              "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
              "off_host_copy_verified": False,
              "note": "Logical backup; this does not establish production WAL RPO or off-host recovery."}
    output.with_suffix(output.suffix + ".json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def restore(source: Path, key_file: Path, target: str, confirmation: str) -> dict:
    if target not in {"horizon", "horizon_test"} or confirmation != target:
        raise ValueError("Explicit --confirm-database must match the supported target database")
    started = datetime.now(timezone.utc)
    with private_plaintext() as plaintext:
        with source.open("rb") as encrypted, plaintext.open("wb") as destination:
            decrypt_stream(encrypted, destination, read_key(key_file))
        # Authentication completed before any database mutation is possible.
        with plaintext.open("rb") as stream:
            subprocess.run(docker_command() + ["exec", "-T", "postgres", "pg_restore", "-U", "horizon_admin",
                           "-d", target, "--clean", "--if-exists", "--no-owner", "--exit-on-error"],
                           stdin=stream, capture_output=True, check=True, timeout=300)
    return {"target": target, "started_at": started.isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "comparison_required": "Compare canonical table counts/checksums before accepting recovery."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["backup", "restore"])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument(
        "--key-file", type=Path,
        default=Path(os.getenv("BACKUP_KEY_FILE") or ".secrets/backup_key"),
        help="Encryption key: explicit option, then BACKUP_KEY_FILE, then the private local default",
    )
    parser.add_argument("--database", default="horizon")
    parser.add_argument("--target", default="horizon_test")
    parser.add_argument("--confirm-database", default="")
    args = parser.parse_args()
    if args.action == "backup":
        if not args.output:
            parser.error("backup requires --output")
        record = backup(args.output, args.key_file, args.database)
    else:
        if not args.input:
            parser.error("restore requires --input")
        record = restore(args.input, args.key_file, args.target, args.confirm_database)
    print(json.dumps(record, indent=2))
