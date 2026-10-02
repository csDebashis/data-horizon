"""Bake pinned JAR-native codecs into the immutable image, not executable /tmp."""

from pathlib import Path
import platform
import zipfile


def install(jars: Path, target: Path, architecture: str):
    arch = {"aarch64": "aarch64", "arm64": "aarch64", "x86_64": "x86_64", "amd64": "x86_64"}.get(architecture)
    if arch is None:
        raise ValueError("Unsupported native codec architecture")
    target.mkdir(parents=True, exist_ok=True)
    rules = [
        ("snappy-java-*.jar", f"org/xerial/snappy/native/Linux/{arch}/libsnappyjava.so", "libsnappyjava.so"),
        ("zstd-jni-*.jar", f"linux/{'amd64' if arch == 'x86_64' else arch}/", "libzstd-jni.so"),
    ]
    installed = []
    for pattern, member, destination in rules:
        files = sorted(jars.glob(pattern))
        if len(files) != 1:
            raise ValueError("Expected exactly one pinned codec JAR: " + pattern)
        with zipfile.ZipFile(files[0]) as jar:
            matches = [
                name
                for name in jar.namelist()
                if not name.endswith("/")
                and (
                    name == member
                    or (member.endswith("/") and name.startswith(member) and name.endswith(".so"))
                )
            ]
            if len(matches) != 1:
                raise ValueError("Missing architecture-specific codec library")
            binary = jar.read(matches[0])
        if not binary.startswith(b"\x7fELF"):
            raise ValueError("Codec library is not an ELF shared object")
        path = target / destination
        path.write_bytes(binary)
        path.chmod(0o555)
        installed.append(destination)
    target.chmod(0o755)
    return installed


if __name__ == "__main__":
    import pyspark

    print(
        "Installed immutable codecs:",
        install(Path(pyspark.__file__).parent / "jars", Path("/opt/horizon-native"), platform.machine()),
    )
