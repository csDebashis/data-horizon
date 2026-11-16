"""Supporting mechanics only; these mocks are not Docker rollback evidence."""

import hashlib
import json
from types import SimpleNamespace

import pytest

from scripts.retain_release import COMPONENTS, REVISION_LABEL, check_build_source, retain_release

pytestmark = [pytest.mark.feature("F012"), pytest.mark.feature("F013")]
SOURCE = "a" * 40
IMAGES = {kind: "sha256:" + digit * 64 for kind, digit in zip(COMPONENTS, "bcdef1")}
APP, SPARK = IMAGES["api"], IMAGES["processor"]


@pytest.fixture
def release(tmp_path):
    (tmp_path / "reports").mkdir()
    (tmp_path / "compose.yaml").write_text("services: {}\n")
    report = tmp_path / "reports/qualified.xml"
    report.write_text(
        '<testsuites tests="1" failures="0" errors="0" skipped="0"><testsuite><properties>'
        + "".join(f'<property name="docker_{kind}_image" value="{identity}"/>' for kind, identity in IMAGES.items())
        + '</properties><testcase name="actual_docker_flow"/></testsuite></testsuites>'
    )
    return {"root": tmp_path, "source_commit": SOURCE,
            "component_images": {kind: kind + ":local" for kind in COMPONENTS}, "regression_report": "reports/qualified.xml",
            "output": "reports/retained.json", "docker": "/usr/bin/docker"}


class DockerGit:
    def __init__(self):
        self.calls = []
        self.images = {kind + ":local": self.image(identity) for kind, identity in IMAGES.items()}
        self.known_revisions = {SOURCE}
        self.ancestors = {SOURCE}
        self.dirty = False
        self.untracked = ""
        self.unknown_revision = False
        self.tag_failure = False

    @staticmethod
    def image(identity, revision=SOURCE):
        return {"Id": identity, "Architecture": "arm64", "Os": "linux",
                "Labels": {REVISION_LABEL: revision} if revision else {}}

    def __call__(self, arguments, **kwargs):
        self.calls.append(arguments)
        assert kwargs["timeout"] == 20 and kwargs["capture_output"] is True
        code, output, error = 0, "", ""
        if arguments[:2] == ["git", "rev-parse"]:
            wanted = arguments[-1].removesuffix("^{commit}")
            code, output = (1, "") if self.unknown_revision or wanted not in self.known_revisions else (0, wanted + "\n")
        elif arguments[:2] == ["git", "merge-base"]:
            code = int(arguments[-2] not in self.ancestors)
        elif arguments[:2] == ["git", "diff"]:
            code = int(self.dirty)
        elif arguments[:2] == ["git", "ls-files"]:
            output = self.untracked
        elif arguments[1:3] == ["image", "inspect"]:
            image = self.images.get(arguments[-1])
            if image is None:
                code, error = 1, "Error response from daemon: No such image"
            else:
                output = json.dumps(image)
        elif arguments[1:3] == ["image", "tag"]:
            if self.tag_failure:
                code = 1
            else:
                self.images[arguments[-1]] = next(image.copy() for image in self.images.values() if image["Id"] == arguments[-2])
        else:
            raise AssertionError("Unexpected command")
        return SimpleNamespace(returncode=code, stdout=output, stderr=error)

    @property
    def mutations(self):
        return [arguments for arguments in self.calls if arguments[1:3] == ["image", "tag"]]


def test_retains_exact_qualified_pair_without_production_or_compatibility_inference(release):
    commands = DockerGit()
    result = retain_release(**release, run=commands)
    output = release["root"] / release["output"]
    assert json.loads(output.read_text()) == result
    assert result["verified"] is True
    assert result["database_compatible"] is False and result["checkpoint_compatible"] is False
    assert result["source_commit"] == SOURCE
    assert result["component_images"] == IMAGES
    assert result["configuration_sha256"] == hashlib.sha256((release["root"] / "compose.yaml").read_bytes()).hexdigest()
    assert result["regression_report_sha256"] == hashlib.sha256((release["root"] / "reports/qualified.xml").read_bytes()).hexdigest()
    assert result["image_provenance"]["processor"] == {"architecture": "arm64", "os": "linux", "revision": SOURCE}
    assert len(commands.mutations) == len(COMPONENTS)
    for kind, identity in IMAGES.items():
        tag = result["retained_tags"][kind]
        assert tag == f"data-horizon-{kind}:verified-{SOURCE[:12]}-{identity[7:19]}"
        assert commands.images[tag]["Id"] == identity
    # Rebuilding mutable tags cannot change the dedicated retained names.
    commands.images["api:local"] = commands.image("sha256:" + "d" * 64)
    assert commands.images[result["retained_tags"]["api"]]["Id"] == APP
    assert not list(output.parent.glob(".retained-*.tmp"))


@pytest.mark.parametrize("problem", ["missing", "no-label", "wrong-label", "conflicting-tag"])
def test_missing_unverified_or_conflicting_images_never_receive_retained_tags(release, problem):
    commands = DockerGit()
    if problem == "missing":
        del commands.images["api:local"]
    elif problem in {"no-label", "wrong-label"}:
        commands.images["api:local"] = commands.image(APP, None if problem == "no-label" else "d" * 40)
    else:
        commands.images[f"data-horizon-processor:verified-{SOURCE[:12]}-{SPARK[7:19]}"] = commands.image("sha256:" + "2" * 64)
    with pytest.raises(ValueError):
        retain_release(**release, run=commands)
    assert commands.mutations == []
    assert not (release["root"] / release["output"]).exists()


@pytest.mark.parametrize("problem", ["missing", "failed", "error", "skipped", "empty", "mismatched", "invalid-xml", "xml-entity", "too-large"])
def test_unqualified_regression_evidence_fails_before_tagging(release, problem):
    report = release["root"] / release["regression_report"]
    content = report.read_text()
    if problem == "missing":
        report.unlink()
    else:
        content = {"failed": content.replace('/></testsuite>', '><failure/></testcase></testsuite>'),
                   "error": content.replace('/></testsuite>', '><error/></testcase></testsuite>'),
                   "skipped": content.replace('skipped="0"', 'skipped="1"'),
                   "empty": "<testsuites/>", "mismatched": content.replace(APP, "sha256:" + "d" * 64),
                   "invalid-xml": "<not-complete",
                   "xml-entity": '<!DOCTYPE x [<!ENTITY x "untrusted">]><testsuites/>',
                   "too-large": " " * (1024 * 1024 + 1)}[problem]
        report.write_text(content)
    commands = DockerGit()
    with pytest.raises(ValueError):
        retain_release(**release, run=commands)
    assert commands.mutations == []
    assert not (release["root"] / release["output"]).exists()


def test_existing_manifest_and_outside_report_are_preserved_before_docker(release):
    path = release["root"] / release["output"]
    path.write_text("prior release evidence")
    commands = DockerGit()
    with pytest.raises(FileExistsError):
        retain_release(**release, run=commands)
    assert path.read_text() == "prior release evidence" and commands.calls == []
    with pytest.raises(ValueError, match="under reports"):
        retain_release(**{**release, "output": "outside.json"}, run=commands)
    assert commands.calls == []


@pytest.mark.parametrize("problem", ["unknown", "dirty", "untracked", "short-revision"])
def test_build_and_retention_require_available_clean_committed_runtime_inputs(release, problem):
    commands = DockerGit()
    commands.unknown_revision = problem == "unknown"
    commands.dirty = problem == "dirty"
    commands.untracked = "src/uncommitted.py\n" if problem == "untracked" else ""
    source = SOURCE[:8] if problem == "short-revision" else SOURCE
    with pytest.raises(ValueError):
        check_build_source(release["root"], source, run=commands)
    assert commands.mutations == []


def test_compatibility_is_explicit_and_existing_matching_tags_do_not_mutate(release):
    commands = DockerGit()
    with pytest.raises(ValueError, match="explanation"):
        retain_release(**release, run=commands, database_compatible=True)
    for kind, identity in IMAGES.items():
        commands.images[f"data-horizon-{kind}:verified-{SOURCE[:12]}-{identity[7:19]}"] = commands.image(identity)
    result = retain_release(**release, run=commands, database_compatible=True, checkpoint_compatible=True,
                            compatibility_note="Reviewed: image provenance tooling only; schema and checkpoints unchanged.")
    assert result["database_compatible"] is True and result["checkpoint_compatible"] is True
    assert commands.mutations == []


def test_tag_failure_never_publishes_a_verified_manifest_or_deletes_images(release):
    commands = DockerGit()
    commands.tag_failure = True
    with pytest.raises(ValueError, match="could not retain"):
        retain_release(**release, run=commands)
    assert not (release["root"] / release["output"]).exists()
    assert {kind + ":local" for kind in COMPONENTS} <= commands.images.keys()
    assert not any("rm" in command or "prune" in command or "push" in command for command in commands.calls)


def test_make_passes_explicit_revision_and_dockerfiles_label_both_images(project_root):
    makefile = (project_root / "Makefile").read_text()
    assert "check-build --source-commit" in makefile
    assert '--build-arg VCS_REF="$(VCS_REF)"' in makefile
    assert 'check-build --source-commit "$(VCS_REF)"' in makefile
    for folder in ("services/api", "services/collector", "services/processor", "frontend", "tools/operations"):
        dockerfile = (project_root / folder / "Dockerfile").read_text()
        assert "ARG VCS_REF" in dockerfile
        assert "LABEL org.opencontainers.image.revision=$VCS_REF" in dockerfile


def test_component_sources_may_be_committed_ancestors_of_the_assembly(release):
    commands = DockerGit()
    previous = "d" * 40
    commands.known_revisions.add(previous)
    commands.ancestors.add(previous)
    commands.images["api:local"] = commands.image(APP, previous)
    result = retain_release(**release, run=commands)
    assert result["source_commit"] == SOURCE
    assert result["image_provenance"]["api"]["revision"] == previous
    assert result["retained_tags"]["api"].startswith("data-horizon-api:verified-" + previous[:12])


def test_known_but_unrelated_image_source_and_incomplete_map_fail_closed(release):
    commands = DockerGit()
    unrelated = "d" * 40
    commands.known_revisions.add(unrelated)
    commands.images["api:local"] = commands.image(APP, unrelated)
    with pytest.raises(ValueError, match="ancestor"):
        retain_release(**release, run=commands)
    assert commands.mutations == []
    incomplete = {kind: value for kind, value in release["component_images"].items() if kind != "dashboard"}
    with pytest.raises(ValueError, match="five"):
        retain_release(**{**release, "component_images": incomplete}, run=commands)
    assert commands.mutations == []


def test_backend_source_scope_does_not_include_frontend_work_in_progress(release):
    commands = DockerGit()
    check_build_source(release["root"], SOURCE, run=commands, scope="backend")
    diff = next(command for command in commands.calls if command[:2] == ["git", "diff"])
    assert "services" in diff and "packages" in diff and "frontend" not in diff


def test_generated_ui_results_do_not_hide_real_source_changes(tmp_path):
    import subprocess

    root = tmp_path / 'repo'
    (root / 'frontend/app').mkdir(parents=True)
    (root / 'frontend/reports').mkdir()
    code = root / 'frontend/app/page.tsx'
    report = root / 'frontend/reports/unit.xml'
    code.write_text('export default function Page() { return null; }')
    report.write_text('<testsuites/>')

    def git(*args):
        return subprocess.check_output([
            'git', '-C', str(root), '-c', 'user.name=Test fixture',
            '-c', 'user.email=fixture@example.invalid', *args,
        ], text=True).strip()

    git('init', '-b', 'main')
    git('add', '.')
    git('commit', '-m', 'Fixture application')
    revision = git('rev-parse', 'HEAD')
    report.write_text('<testsuites tests="33"/>')
    assert check_build_source(root, revision) == revision
    code.write_text('export default function ChangedPage() { return null; }')
    with pytest.raises(ValueError, match='differ'):
        check_build_source(root, revision)
    code.write_text('export default function Page() { return null; }')
    (root / 'frontend/app/uncommitted.ts').write_text('export const changed = true;')
    with pytest.raises(ValueError, match='differ'):
        check_build_source(root, revision)
