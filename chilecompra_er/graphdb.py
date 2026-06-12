"""Neo4j connection factory — reuses the existing infra modules.

Resolution order for the bolt URI:
  1. NEO4J_URI env var (secrets.env or shell), or
  2. lookup of the EC2 instance tagged NEO4J_EC2_NAME (default "Neo4j-EC2")
     via boto3, requiring it to be running.

start_neo4j_instance() starts the instance and waits until Neo4j answers.
"""

from __future__ import annotations

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


def start_neo4j_instance(wait_for_bolt: int = 300) -> str:
    """Start the Neo4j EC2 instance, wait for it, return its public IP."""
    ec2 = _ec2_client()
    name = env("NEO4J_EC2_NAME", DEFAULT_INSTANCE_NAME)
    instance_id, public_ip, state = fne.find_instance_by_name(ec2, name)
    if state != "running":
        fne.start_instance(ec2, instance_id)
        ec2.get_waiter("instance_running").wait(InstanceIds=[instance_id])
        _, public_ip, _ = fne.find_instance_by_name(ec2, name)

    deadline = time.time() + wait_for_bolt
    import socket

    while time.time() < deadline:
        try:
            with socket.create_connection((public_ip, BOLT_PORT), timeout=5):
                return public_ip
        except OSError:
            time.sleep(5)
    raise TimeoutError(f"bolt port {BOLT_PORT} on {public_ip} not reachable after {wait_for_bolt}s")
