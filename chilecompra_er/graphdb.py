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


def _bolt_host() -> str | None:
    """Host the python driver would dial: NEO4J_URI's host if set, else the running
    instance's public IP. None when neither is available."""
    uri = env("NEO4J_URI")
    if uri:
        m = re.search(r"bolt://([^:/]+)", uri)
        if m:
            return m.group(1)
    try:
        _iid, state, ip = instance_status()
        return ip if state == "running" else None
    except Exception:  # noqa: BLE001 — a probe must never raise
        return None


def graph_reachable(timeout: float = 4.0) -> bool:
    """True if the bolt port answers a TCP connect right now (a quick liveness
    probe — it does NOT verify auth). Lets the pipeline decide whether its starter
    must run: reachable -> skip; not reachable (stopped, or blocked by the security
    group from here) -> run the starter."""
    import socket

    host = _bolt_host()
    if not host:
        return False
    try:
        with socket.create_connection((host, BOLT_PORT), timeout=timeout):
            return True
    except OSError:
        return False


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


def _client_public_ip() -> str | None:
    """This machine's public IPv4, as the security group sees it. None on failure."""
    import urllib.request

    try:
        return urllib.request.urlopen(
            "https://checkip.amazonaws.com", timeout=5).read().decode().strip()
    except Exception:  # noqa: BLE001 — best-effort; caller degrades gracefully
        return None


SG_NAME = "neo4j-sg"           # the security group the allowlist rule lives in
SG_RULE_DESC = "chilecompra_app"  # the named ingress rule the starter manages


def _authorize_bolt_ingress(ec2, ports: tuple[int, ...] = (BOLT_PORT,)) -> str | None:
    """Allow THIS machine's public IP to reach Neo4j on `ports` by adding an ingress
    rule named `chilecompra_app` (its Description) to the `neo4j-sg` security group.
    The public IP changes with your network, so a running box can still be
    unreachable (bolt times out) until the IP is allowlisted — this closes that gap.
    Keeps a SINGLE `chilecompra_app` rule per port: any prior one pointing at a
    different IP is revoked first, then the current /32 is added. Returns the
    authorized CIDR, or None if it couldn't (no client IP, group missing, or no
    ec2 perms) — connectivity then relies on whatever the SG already permits.
    The group name is overridable via NEO4J_SG_NAME."""
    client_ip = _client_public_ip()
    if not client_ip:
        return None
    cidr = f"{client_ip}/32"
    sg_name = env("NEO4J_SG_NAME", SG_NAME)
    try:
        groups = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [sg_name]}]).get(
                "SecurityGroups", [])
    except Exception as exc:  # noqa: BLE001
        print(f"  (security-group lookup failed: {exc})")
        return None
    if not groups:
        print(f"  (security group {sg_name!r} not found — allowlist skipped)")
        return None
    sg = groups[0]
    gid = sg["GroupId"]

    # Prune stale `chilecompra_app` rules on these ports (a previous IP), so the
    # named permission stays a single rule tracking the current address.
    stale = []
    for perm in sg.get("IpPermissions", []):
        if perm.get("IpProtocol") != "tcp" or perm.get("FromPort") not in ports:
            continue
        for rng in perm.get("IpRanges", []):
            if rng.get("Description") == SG_RULE_DESC and rng.get("CidrIp") != cidr:
                stale.append({"IpProtocol": "tcp", "FromPort": perm["FromPort"],
                              "ToPort": perm["ToPort"],
                              "IpRanges": [{"CidrIp": rng["CidrIp"]}]})
    if stale:
        try:
            ec2.revoke_security_group_ingress(GroupId=gid, IpPermissions=stale)
        except Exception as exc:  # noqa: BLE001
            print(f"  (could not prune stale {SG_RULE_DESC} rules: {exc})")

    authorized = False
    for port in ports:
        try:
            ec2.authorize_security_group_ingress(
                GroupId=gid,
                IpPermissions=[{
                    "IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                    "IpRanges": [{"CidrIp": cidr, "Description": SG_RULE_DESC}],
                }])
            authorized = True
        except Exception as exc:  # noqa: BLE001 — duplicate rule or missing perms
            if "Duplicate" in str(exc):
                authorized = True
            else:
                print(f"  (could not authorize {cidr} on {sg_name}:{port}: {exc})")
    return cidr if authorized else None


def start_neo4j_instance(wait_for_bolt: int = 300) -> str:
    """Bring the Neo4j EC2 box up + reachable and return its public IP: boot it if
    stopped, allowlist this machine's IP on the bolt port, point .mcp.json at it,
    and wait until bolt answers."""
    ec2 = _ec2_client()
    name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)

    # 1) is the instance already running?
    instance_id, public_ip, state = fne.find_instance_by_name(ec2, name)

    # 2) if not, start it and read its (new) public IP
    if state != "running":
        fne.start_instance(ec2, instance_id)
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        _, public_ip, _ = fne.find_instance_by_name(ec2, name)

    # allowlist THIS machine's public IP on the bolt port — a running box is still
    # unreachable from here until the security group permits the current IP.
    cidr = _authorize_bolt_ingress(ec2)
    if cidr:
        print(f"allowlisted {cidr} as {SG_RULE_DESC} on {SG_NAME} (bolt {BOLT_PORT})")

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
