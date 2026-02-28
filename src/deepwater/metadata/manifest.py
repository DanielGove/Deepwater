"""
Manifest file management for version tracking and compatibility checks.
Prevents data corruption from version mismatches.
"""
from pathlib import Path
import orjson
from typing import Optional


def write_manifest(base_path: Path) -> None:
    """Create or update manifest.json with current Deepwater version."""
    from .. import __version__
    
    manifest_path = base_path / "manifest.json"
    manifest = {
        "deepwater_version": __version__,
        "format_version": 1,
    }
    manifest_path.write_bytes(orjson.dumps(manifest, option=orjson.OPT_INDENT_2))


def read_manifest(base_path: Path) -> Optional[dict]:
    """Read manifest.json if it exists, return None otherwise."""
    manifest_path = base_path / "manifest.json"
    if not manifest_path.exists():
        return None
    try:
        return orjson.loads(manifest_path.read_bytes())
    except Exception:
        return None
