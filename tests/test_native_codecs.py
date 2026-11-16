import zipfile

import pytest

from scripts.install_native_codecs import install


@pytest.mark.parametrize(
    "architecture,snappy,zstd", [("aarch64", "aarch64", "aarch64"), ("x86_64", "x86_64", "amd64")]
)
def test_codec_build_extracts_matching_immutable_libraries(tmp_path, architecture, snappy, zstd):
    jars = tmp_path / "jars"
    jars.mkdir()
    for filename, member in [
        ("snappy-java-1.1.10.5.jar", f"org/xerial/snappy/native/Linux/{snappy}/libsnappyjava.so"),
        ("zstd-jni-1.5.5-4.jar", f"linux/{zstd}/libzstd-jni-1.5.5-4.so"),
    ]:
        with zipfile.ZipFile(jars / filename, "w") as archive:
            archive.writestr(member.rsplit("/", 1)[0] + "/", b"")
            archive.writestr(member, b"\x7fELFtest-codec")
    target = tmp_path / "native"
    assert install(jars, target, architecture) == ["libsnappyjava.so", "libzstd-jni.so"]
    assert all(p.stat().st_mode & 0o777 == 0o555 for p in target.iterdir())


def test_unsupported_architecture_fails_before_packaging(tmp_path):
    with pytest.raises(ValueError, match="architecture"):
        install(tmp_path, tmp_path / "native", "unknown")
