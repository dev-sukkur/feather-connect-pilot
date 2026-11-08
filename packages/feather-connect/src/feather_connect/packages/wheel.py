"""Reading a flow package's wheel back: its name, version and manifest, checked before a registry keeps it."""

import hashlib
import io
import json
import zipfile
from collections.abc import Mapping
from dataclasses import dataclass
from email.parser import HeaderParser
from typing import Any

from packaging.utils import InvalidWheelFilename, canonicalize_name, parse_wheel_filename

from feather_connect.packages import manifest as m
from feather_connect.packages.manifest import ManifestError

MAX_WHEEL = 50 * 1024 * 1024  # bytes: the most a registry takes, and an install fetches (design §5)


@dataclass(frozen=True)
class WheelInfo:
    filename: str
    name: str
    version: str
    sha256: str
    size: int
    manifest: Mapping[str, Any]


def read_wheel(filename: str, data: bytes) -> WheelInfo:
    """Check a flow package's wheel and say what it is. Raises `ManifestError` for anything that isn't a flow package:
    a name that isn't a wheel's, too many bytes, a zip that can't be read, metadata that disagrees with the name, or a
    manifest that is missing or wrong."""
    if len(data) > MAX_WHEEL:
        raise ManifestError(f"{filename} has {len(data)} bytes, more than the {MAX_WHEEL} a package may have")
    try:
        dist, version, _, _ = parse_wheel_filename(filename)
    except InvalidWheelFilename:
        raise ManifestError(f"{filename!r} is not a wheel's name: a flow package is a .whl file") from None
    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise ManifestError(f"{filename} is not a zip file") from None
    with archive:
        names = set(archive.namelist())
        module = str(dist).replace("-", "_")
        info = f"{module}-{version}.dist-info/METADATA"
        if info not in names:
            raise ManifestError(f"{filename} has no {info}")
        headers = HeaderParser().parsestr(archive.read(info).decode("utf-8", "replace"))
        if canonicalize_name(headers.get("Name", "")) != dist or headers.get("Version") != str(version):
            raise ManifestError(
                f"{filename}: its METADATA names {headers.get('Name')} {headers.get('Version')}"
            )
        where = f"{module}/{m.MANIFEST}"
        if where not in names:
            raise ManifestError(f"{filename} is a wheel but not a flow package: it has no {where}")
        try:
            found: Any = json.loads(archive.read(where))
        except ValueError:
            raise ManifestError(f"{where} is not JSON") from None
    manifest = m.validate(found)
    if canonicalize_name(manifest["name"]) != dist or manifest["version"] != str(version):
        raise ManifestError(f"{where} names {manifest['name']} {manifest['version']}, not the wheel's")
    return WheelInfo(
        filename, manifest["name"], manifest["version"], hashlib.sha256(data).hexdigest(), len(data), manifest
    )
