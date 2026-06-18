"""Neo4j connection factory — reuses the existing infra modules.

Resolution order for the bolt URI:
  1. NEO4J_URI env var (secrets.env or shell), or
  2. lookup of the EC2 instance tagged NEO4J_EC2_NAME (default "Neo4j-EC2")
     via boto3, requiring it to be running.

start_neo4j_instance() brings the box up in three steps: (1) check whether the
instance is already running; (2) if not, start it and read its new public IP,
registering that IP into .mcp.json for the Neo4j MCP server; (3) wait until
Neo4j answers on the bolt port (it auto-starts on boot).
"""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
if str(REPO_ROOT) not in sys.path:  # funcionesNeo4j / funcionesNeo4jEC2 live at repo root
    sys.path.insert(0, str(REPO_ROOT))

import funcionesNeo4j as fn  # noqa: E402
import funcionesNeo4jEC2 as fne  # noqa: E402

from .config import env  # noqa: E402

DEFAULT_INSTANCE_NAME = "Neo4j-EC2"
BOLT_PORT = 7687
MCP_CONFIG_PATH = REPO_ROOT / ".mcp.json"
# NEO4J_URI value inside .mcp.json — captured so only the host:port is rewritten.
_MCP_BOLT_RE = re.compile(r'("NEO4J_URI"\s*:\s*")bolt://[^"]*(")')


def _ec2_client():
    import boto3

    return boto3.client("ec2", region_name=env("AWS_REGION", "us-east-1"))


def get_connection() -> fn.Neo4jConnection:
    uri = env("NEO4J_URI")
    if not uri:
        ec2 = _ec2_client()
        name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)
        instance_id, public_ip, state = fne.find_instance_by_name(ec2, name)
        if state != "running" or not public_ip:
            raise RuntimeError(
                f"EC2 instance '{name}' is {state}; start it first "
                f"(chilecompra_er.graphdb.start_neo4j_instance())"
            )
        uri = f"bolt://{public_ip}:{BOLT_PORT}"
    return fn.Neo4jConnection(
        uri=uri,
        user=env("NEO4J_USER", "neo4j"),
        pwd=env("NEO4J_PASSWORD"),
    )


def instance_status() -> tuple[str, str, str | None]:
    """(instance_id, state, public_ip) of the Neo4j EC2 instance."""
    ec2 = _ec2_client()
    name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)
    instance_id, public_ip, state = fne.find_instance_by_name(ec2, name)
    return instance_id, state, public_ip


def stop_neo4j_instance() -> str:
    """Stop the Neo4j EC2 instance (data persists; public IP changes on next start)."""
    ec2 = _ec2_client()
    name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)
    instance_id, _, state = fne.find_instance_by_name(ec2, name)
    if state in ("stopped", "stopping"):
        return state
    ec2.stop_instances(InstanceIds=[instance_id])
    return "stopping"


def _update_mcp_bolt_ip(public_ip: str | None,
                        path: Path = MCP_CONFIG_PATH) -> str | None:
    """Point the Neo4j MCP server at `public_ip` by rewriting NEO4J_URI in
    .mcp.json (the instance's public IP changes on every start). A targeted
    text substitution, so the rest of the file's formatting is untouched.
    Returns the new bolt URI, or None if there was nothing to update (IP
    unknown, file missing, or no NEO4J_URI present)."""
    if not public_ip or not path.exists():
        return None
    new_uri = f"bolt://{public_ip}:{BOLT_PORT}"
    text = path.read_text(encoding="utf-8")
    new_text, n = _MCP_BOLT_RE.subn(rf"\g<1>{new_uri}\g<2>", text)
    if n == 0:
        return None
    if new_text != text:
        path.write_text(new_text, encoding="utf-8")
    return new_uri


def start_neo4j_instance(wait_for_bolt: int = 300) -> str:
    """Bring the Neo4j EC2 box up and return its public IP (see module docstring
    for the three steps)."""
    ec2 = _ec2_client()
    name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)

    # 1) is the instance already running?
    instance_id, public_ip, state = fne.find_instance_by_name(ec2, name)

    # 2) if not, start it and read its (new) public IP
    if state != "running":
        fne.start_instance(ec2, instance_id)
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        _, public_ip, _ = fne.find_instance_by_name(ec2, name)

    # register the IP so the Neo4j MCP server points at the right box
    new_uri = _update_mcp_bolt_ip(public_ip)
    if new_uri:
        print(f"registered {new_uri} in {MCP_CONFIG_PATH.name}")

    # 3) Neo4j auto-starts on boot — wait until it answers on bolt
    deadline = time.time() + wait_for_bolt
    import socket

    while time.time() < deadline:
        try:
            with socket.create_connection((public_ip, BOLT_PORT), timeout=5):
                return public_ip
        except OSError:
            time.sleep(5)
    raise TimeoutError(f"bolt port {BOLT_PORT} on {public_ip} not reachable after {wait_for_bolt}s")
