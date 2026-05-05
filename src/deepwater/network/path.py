from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse


DEFAULT_PORT = 7447


@dataclass(frozen=True)
class NetworkTarget:
    raw: str
    is_remote: bool
    path: str
    host: str | None = None
    port: int = DEFAULT_PORT


def parse_target(value: str | Path, *, default_port: int = DEFAULT_PORT) -> NetworkTarget:
    """
    Parse a local Deepwater path or a Tailscale-reachable remote path.

    Remote forms:
      - host:/absolute/path
      - dw://host/absolute/path
      - dw://host:7447/absolute/path

    Plain absolute/relative paths are local.
    """
    raw = str(value)
    if not raw:
        raise ValueError("target path is required")

    if raw.startswith("dw://"):
        parsed = urlparse(raw)
        if parsed.scheme != "dw" or not parsed.netloc:
            raise ValueError(f"invalid Deepwater URL: {raw!r}")
        host = parsed.hostname
        if not host:
            raise ValueError(f"missing host in Deepwater URL: {raw!r}")
        path = unquote(parsed.path or "")
        if not path.startswith("/"):
            raise ValueError(f"remote Deepwater URL must include an absolute path: {raw!r}")
        return NetworkTarget(
            raw=raw,
            is_remote=True,
            host=host,
            port=int(parsed.port or default_port),
            path=path,
        )

    colon = raw.find(":")
    slash = raw.find("/")
    if colon > 0 and (slash == -1 or colon < slash):
        host = raw[:colon]
        path = raw[colon + 1 :]
        if not host:
            raise ValueError(f"missing host in remote Deepwater target: {raw!r}")
        if not path.startswith("/"):
            raise ValueError(
                "host:/path remote targets require an absolute path; "
                "use dw://host:port/path when specifying a port"
            )
        return NetworkTarget(
            raw=raw,
            is_remote=True,
            host=host,
            port=int(default_port),
            path=path,
        )

    return NetworkTarget(raw=raw, is_remote=False, path=raw, port=int(default_port))


def resolve_agent_path(root: str | Path, requested: str | Path) -> Path:
    """
    Resolve a client-requested path and reject anything outside the agent root.
    """
    root_path = Path(root).expanduser().resolve()
    requested_path = Path(requested).expanduser()
    if not requested_path.is_absolute():
        requested_path = root_path / requested_path
    resolved = requested_path.resolve()
    if resolved != root_path and root_path not in resolved.parents:
        raise ValueError(f"remote path rejected outside agent root: {requested}")
    return resolved
