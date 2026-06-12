"""Apply Cypher migrations in filename order, recording each in the graph.

Usage:
    python -m chilecompra_er.migrations.migrate            # connect via graphdb
    python -m chilecompra_er.migrations.migrate --dry-run  # print, don't run
"""

from __future__ import annotations

import argparse
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent


def _statements(path: Path) -> list[str]:
    text = "\n".join(
        line for line in path.read_text(encoding="utf-8").splitlines()
        if not line.strip().startswith("//")
    )
    return [s.strip() for s in text.split(";") if s.strip()]


def applied_migrations(conn) -> set[str]:
    records = conn.query("MATCH (m:SchemaMigration) RETURN m.file AS file")
    return {r["file"] for r in records}


def migrate(conn, dry_run: bool = False) -> list[str]:
    done = set() if dry_run else applied_migrations(conn)
    ran: list[str] = []
    for path in sorted(MIGRATIONS_DIR.glob("[0-9]*.cypher")):
        if path.name in done:
            continue
        for stmt in _statements(path):
            if dry_run:
                print(stmt, end="\n\n")
            else:
                conn.query(stmt)
        if not dry_run:
            conn.query(
                "MERGE (m:SchemaMigration {file: $file}) ON CREATE SET m.applied_at = datetime()",
                parameters={"file": path.name},
            )
        ran.append(path.name)
    return ran


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        ran = migrate(None, dry_run=True)
    else:
        from ..graphdb import get_connection

        conn = get_connection()
        try:
            ran = migrate(conn)
        finally:
            conn.close()
    print(f"migrations {'printed' if args.dry_run else 'applied'}: {ran or 'none (up to date)'}")


if __name__ == "__main__":
    main()
