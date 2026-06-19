#!/usr/bin/env python
"""Self-correcting launcher for the Neo4j MCP server.

The Neo4j EC2 box gets a new public IP on every start, so a hard-coded
NEO4J_URI in .mcp.json goes stale. This launcher runs *before* the real MCP
server: it resolves the instance's current public IP, exports
NEO4J_URI=bolt://<ip>:7687 (and rewrites .mcp.json to match), then hands off
to `uvx mcp-neo4j-cypher`. If resolution fails (instance stopped, no AWS
creds, ...) it falls back to whatever NEO4J_URI is already in the environment,
so the server still starts with the last-known IP.

The MCP transport is stdio JSON-RPC over fd 1, so this launcher must keep
stdout pristine: all diagnostics go to stderr, and the noisy stdout prints
from the infra helpers are redirected to stderr during IP resolution.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:  # make `chilecompra_er` importable
    sys.path.insert(0, str(REPO_ROOT))


def _log(msg: str) -> None:
    print(f"[neo4j-mcp-launch] {msg}", file=sys.stderr, flush=True)


def _resolve_uri() -> str | None:
    """Current bolt URI for the running instance, or None to fall back."""
    try:
        from chilecompra_er import graphdb as g

        with contextlib.redirect_stdout(sys.stderr):  # infra helpers print to stdout
            _, state, ip = g.instance_status()
            if state == "running" and ip:
                g._update_mcp_bolt_ip(ip)  # keep .mcp.json in sync on disk
                return f"bolt://{ip}:{g.BOLT_PORT}"
        _log(f"instance is '{state}', not running — using existing NEO4J_URI")
    except Exception as exc:  # AWS creds missing, boto3 absent, etc.
        _log(f"IP resolution failed ({exc!r}) — using existing NEO4J_URI")
    return None


def main() -> int:
    uri = _resolve_uri()
    if uri:
        os.environ["NEO4J_URI"] = uri
        _log(f"NEO4J_URI = {uri}")
    elif os.environ.get("NEO4J_URI"):
        _log(f"NEO4J_URI = {os.environ['NEO4J_URI']} (fallback)")
    else:
        _log("no NEO4J_URI resolved and none in environment — server will likely fail")

    uvx = shutil.which("uvx") or "uvx"
    cmd = [uvx, "mcp-neo4j-cypher@latest", "--transport", "stdio", *sys.argv[1:]]
    # Inherit stdio so the child speaks JSON-RPC directly to the MCP client;
    # this launcher stays as a thin parent and forwards the exit code.
    return subprocess.run(cmd).returncode


if __name__ == "__main__":
    raise SystemExit(main())
