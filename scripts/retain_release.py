"""Retain a Docker-qualified local component image set without replacing release evidence."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET

try:
    from scripts.preflight import docker_binary
except ModuleNotFoundError:
    from preflight import docker_binary

ROOT = Path(__file__).resolve().parents[1]
REVISION_LABEL = "org.opencontainers.image.revision"
IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}")
COMPONENTS = ("api", "collector", "processor", "dashboard", "ops")
IMAGE_ENV = {"api": "API_IMAGE", "collector": "COLLECTOR_IMAGE", "processor": "SPARK_IMAGE",
             "dashboard": "DASHBOARD_IMAGE", "ops": "OPS_IMAGE"}
BACKEND_INPUTS = ("packages", "services", "tools", "scripts", "configs", "docker", ".dockerignore", "Makefile",
                  "Dockerfile.postgres", "Dockerfile.proxy", "Dockerfile.monitoring")
DASHBOARD_INPUTS = ("frontend", ".dockerignore", "Makefile", "scripts/retain_release.py")
BUILD_INPUTS = tuple(dict.fromkeys((*BACKEND_INPUTS, *DASHBOARD_INPUTS)))
INSPECT_FORMAT = ('{"Id":{{json .Id}},"Architecture":{{json .Architecture}},'
                  '"Os":{{json .Os}},"Labels":{{json .Config.Labels}}}')


def _command(arguments, root, run):
    environment = dict(os.environ)
    if Path(arguments[0]).is_absolute():
        environment["PATH"] = str(Path(arguments[0]).parent) + os.pathsep + environment.get("PATH", "")
    return run(arguments, cwd=root, env=environment, text=True, capture_output=True, timeout=20)


def check_build_source(root, source_commit, run=subprocess.run, scope="all"):
    """Allow document-only commits, but require these runtime inputs to match the source."""
    root = Path(root).resolve()
    if not isinstance(source_commit, str) or not re.fullmatch(r"[a-f0-9]{40}", source_commit):
        raise ValueError("VCS_REF must be a full committed Git revision")
    revision = _command(["git", "rev-parse", "--verify", source_commit + "^{commit}"], root, run)
    if revision.returncode or revision.stdout.strip() != source_commit:
        raise ValueError("VCS_REF is not an available committed source revision")
    inputs = {"all": BUILD_INPUTS, "backend": BACKEND_INPUTS, "dashboard": DASHBOARD_INPUTS}.get(scope)
    if inputs is None:
        raise ValueError("Unknown build-source scope")
    # Test output is excluded from the Docker context and is not application source.
    source_inputs = (*inputs, ":(exclude)frontend/reports/**")
    tracked = _command(["git", "diff", "--quiet", source_commit, "--", *source_inputs], root, run)
    untracked = _command(["git", "ls-files", "--others", "--exclude-standard", "--", *source_inputs], root, run)
    if tracked.returncode or untracked.returncode or untracked.stdout.strip():
        raise ValueError("Runtime build inputs differ from VCS_REF; commit them before building or retaining")
    return source_commit


def _report_path(root, value):
    path = Path(value)
    path = (root / path).resolve() if not path.is_absolute() else path.resolve()
    reports = (root / "reports").resolve()
    if not reports.is_relative_to(root) or not path.is_relative_to(reports):
        raise ValueError("Release reports and manifests must stay under reports/")
    return path


def _inspect(binary, reference, root, run, missing_allowed=False):
    if not isinstance(reference, str) or not reference or len(reference) > 256 or any(c.isspace() for c in reference):
        raise ValueError("An explicit Docker image reference is required")
    result = _command([binary, "image", "inspect", "--format", INSPECT_FORMAT, reference], root, run)
    if result.returncode:
        if missing_allowed and "no such image" in result.stderr.lower():
            return None
        raise ValueError("A required local image is unavailable; no release was retained")
    try:
        if len(result.stdout) > 1024 * 1024:
            raise ValueError
        image = json.loads(result.stdout)
        if not IMAGE_ID.fullmatch(image["Id"]) or not image.get("Architecture") or not image.get("Os"):
            raise ValueError
        return image
    except (KeyError, TypeError, ValueError):
        raise ValueError("Docker did not return a valid immutable local image identity") from None


def _qualified_report(path, image_ids):
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("A bounded Docker regression report is required")
    with path.open("rb") as stream:
        content = stream.read(1024 * 1024 + 1)
    if len(content) > 1024 * 1024:
        raise ValueError("A bounded Docker regression report is required")
    if b"<!DOCTYPE" in content.upper() or b"<!ENTITY" in content.upper():
        raise ValueError("Docker regression XML must not declare document types or entities")
    try:
        report = ET.fromstring(content)
    except ET.ParseError:
        raise ValueError("Docker regression XML is invalid") from None
    if (not list(report.iter("testcase")) or any(list(report.iter(tag)) for tag in ("failure", "error", "skipped"))
            or any(element.get(name, "0") not in {"0", "0.0"}
                   for element in report.iter() if element.tag in {"testsuite", "testsuites"}
                   for name in ("failures", "errors", "skipped"))):
        raise ValueError("Docker regression evidence must contain passing tests without failures, errors or skips")
    for component, expected in image_ids.items():
        name = "docker_" + component + "_image"
        actual = {entry.get("value") for entry in report.iter("property") if entry.get("name") == name}
        if actual != {expected}:
            raise ValueError("Docker regression evidence does not identify this exact component image set")
    return hashlib.sha256(content).hexdigest()


def _write_new_manifest(output, manifest):
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=output.parent, prefix=".retained-", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(manifest, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # Atomic publication with no replace, including a concurrently created destination.
        os.link(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def retain_release(*, source_commit, component_images, regression_report, output,
                   database_compatible=False, checkpoint_compatible=False, compatibility_note="",
                   root=ROOT, docker=None, run=subprocess.run):
    root = Path(root).resolve()
    output = _report_path(root, output)
    if output.exists():
        raise FileExistsError("Release manifest already exists; choose a new evidence path")
    if not isinstance(component_images, dict) or set(component_images) != set(COMPONENTS):
        raise ValueError("All five explicit component images are required")
    report = _report_path(root, regression_report)
    check_build_source(root, source_commit, run)
    if (database_compatible or checkpoint_compatible) and not compatibility_note.strip():
        raise ValueError("Compatibility declarations require a reviewed explanation")
    if len(compatibility_note) > 2000:
        raise ValueError("Compatibility explanation exceeds its size limit")
    binary = docker or docker_binary()
    if not binary:
        raise ValueError("Docker CLI is unavailable")
    images = {kind: _inspect(binary, component_images[kind], root, run) for kind in COMPONENTS}
    for image in images.values():
        revision = (image.get("Labels") or {}).get(REVISION_LABEL, "")
        if not re.fullmatch(r"[a-f0-9]{40}", revision):
            raise ValueError("Image OCI source revision is missing or invalid")
        available = _command(["git", "rev-parse", "--verify", revision + "^{commit}"], root, run)
        ancestor = _command(["git", "merge-base", "--is-ancestor", revision, source_commit], root, run)
        if available.returncode or available.stdout.strip() != revision or ancestor.returncode:
            raise ValueError("Every image source must be an available committed ancestor of the assembly")
    image_ids = {kind: image["Id"] for kind, image in images.items()}
    report_hash = _qualified_report(report, image_ids)
    tags = {kind: f"data-horizon-{kind}:verified-{image['Labels'][REVISION_LABEL][:12]}-{image['Id'][7:19]}"
            for kind, image in images.items()}
    missing = []
    # Validate every destination before creating any tag.
    for kind, tag in tags.items():
        retained = _inspect(binary, tag, root, run, missing_allowed=True)
        if retained is None:
            missing.append(kind)
        elif retained["Id"] != images[kind]["Id"]:
            raise ValueError("A retention tag already names another image; it will not be replaced")
    manifest = {
        "schema_version": 2, "verified": True,
        "database_compatible": bool(database_compatible), "checkpoint_compatible": bool(checkpoint_compatible),
        "compatibility_note": compatibility_note, "source_commit": source_commit,
        "component_images": image_ids, "retained_tags": tags,
        "image_provenance": {kind: {"revision": image["Labels"][REVISION_LABEL],
                                   "architecture": image["Architecture"], "os": image["Os"]}
                             for kind, image in images.items()},
        "retained_at": datetime.now(timezone.utc).isoformat(),
        "configuration_sha256": hashlib.sha256((root / "compose.yaml").read_bytes()).hexdigest(),
        "regression_report": str(report.relative_to(root)), "regression_report_sha256": report_hash,
        "verification_scope": ["Passing Docker regression for the exact five retained local component images"],
        "known_limitations": ["Local retention does not certify target-VM deployment, state compatibility or production readiness."],
    }
    for kind in missing:
        result = _command([binary, "image", "tag", images[kind]["Id"], tags[kind]], root, run)
        if result.returncode:
            raise ValueError("Docker could not retain the qualified images; existing images were preserved")
    _write_new_manifest(output, manifest)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check-build")
    check.add_argument("--source-commit", required=True)
    check.add_argument("--scope", choices=("all", "backend", "dashboard"), default="all")
    retain = commands.add_parser("retain")
    for name in ("source-commit", "regression-report", "output", *(kind + "-image" for kind in COMPONENTS)):
        retain.add_argument("--" + name, required=True)
    retain.add_argument("--database-compatible", action="store_true")
    retain.add_argument("--checkpoint-compatible", action="store_true")
    retain.add_argument("--compatibility-note", default="")
    args = vars(parser.parse_args(argv))
    command = args.pop("command")
    if command == "check-build":
        check_build_source(ROOT, **args)
        print("Runtime source matches the committed build revision.")
    else:
        args["component_images"] = {kind: args.pop(kind + "_image") for kind in COMPONENTS}
        manifest = retain_release(**args)
        print(json.dumps({"status": "retained", "source_commit": manifest["source_commit"],
                          "component_images": manifest["component_images"]}))


if __name__ == "__main__":
    main()
