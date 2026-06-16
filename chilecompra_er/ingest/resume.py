"""Partial-save + resume support for long `resolve` runs.

A streamed run writes its resoluciones CSV row-by-row and, at every progress
tick, flushes the CSV, (for dry runs) rewrites the products CSV, and writes a
small checkpoint JSON. A killed run can then be resumed: the checkpoint says
how many records are done, the resoluciones CSV is truncated to exactly that
many rows (so kill timing can never duplicate rows), the in-memory catalog is
reseeded from the products CSV, and the fetch continues from `skip +
processed` (the fetch order is stable, see neo4j_source.fetch_tender_items).

Durable state by mode:
  dry run  -> the two CSVs (products CSV reseeds the in-memory catalog)
  persist  -> the graph itself; only the resoluciones CSV + checkpoint here
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path

from ..resolve.assignment import InMemoryCatalog, NodeSpec, NodeView
from .export import RESOLUTION_HEADER, resolution_row
from .runner import ResolutionStats


def checkpoint_path(prefix: Path) -> Path:
    return prefix.with_name(prefix.name + ".checkpoint.json")


def resolutions_path(prefix: Path) -> Path:
    return prefix.with_name(prefix.name + "_resoluciones.csv")


def products_path(prefix: Path) -> Path:
    return prefix.with_name(prefix.name + "_productos_genericos.csv")


# --- checkpoint ---------------------------------------------------------------

@dataclass
class Checkpoint:
    kind: str
    contains: str | None
    segment: int | None
    persist: bool
    limit: int | None
    start_skip: int        # the original --skip the run began at
    processed: int         # cumulative records resolved so far
    done: bool
    stats_dict: dict       # serialized cumulative ResolutionStats

    def stats(self) -> ResolutionStats:
        return ResolutionStats.from_dict(self.stats_dict)

    def to_json(self) -> dict:
        return {
            "kind": self.kind, "contains": self.contains, "segment": self.segment,
            "persist": self.persist, "limit": self.limit,
            "start_skip": self.start_skip, "processed": self.processed,
            "done": self.done, "stats": self.stats_dict,
        }

    def mismatches(self, *, kind, contains, segment, persist, limit) -> list[str]:
        """Reasons the current invocation is incompatible with this checkpoint
        (resuming with different params would corrupt the output)."""
        out = []
        if self.kind != kind:
            out.append(f"kind: checkpoint={self.kind!r} now={kind!r}")
        if self.contains != contains:
            out.append(f"contains: checkpoint={self.contains!r} now={contains!r}")
        if self.segment != segment:
            out.append(f"segment: checkpoint={self.segment!r} now={segment!r}")
        if self.persist != persist:
            out.append(f"persist: checkpoint={self.persist!r} now={persist!r}")
        if self.limit != limit:
            out.append(f"limit: checkpoint={self.limit!r} now={limit!r}")
        return out


def save_checkpoint(path: Path, cp: Checkpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file then replace, so a kill mid-write can't leave a
    # truncated/garbage checkpoint that would block resume.
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(cp.to_json(), ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(path)


def load_checkpoint(path: Path) -> Checkpoint | None:
    if not path.exists():
        return None
    d = json.loads(path.read_text(encoding="utf-8"))
    return Checkpoint(
        kind=d["kind"], contains=d.get("contains"), segment=d.get("segment"),
        persist=d.get("persist", False), limit=d.get("limit"),
        start_skip=d.get("start_skip", 0), processed=d.get("processed", 0),
        done=d.get("done", False), stats_dict=d.get("stats", {}),
    )


# --- streaming resoluciones writer -------------------------------------------

class StreamingResolutionWriter:
    """Append-or-create writer for the resoluciones CSV. utf-8-sig (BOM) for a
    fresh file so Excel renders accents; plain utf-8 when appending so the BOM
    isn't re-inserted mid-file."""

    def __init__(self, path: Path, append: bool):
        path.parent.mkdir(parents=True, exist_ok=True)
        encoding = "utf-8" if append else "utf-8-sig"
        self._f = open(path, "a" if append else "w", newline="", encoding=encoding)
        self._w = csv.writer(self._f)
        if not append:
            self._w.writerow(RESOLUTION_HEADER)

    def write(self, report) -> None:
        self._w.writerow(resolution_row(report))

    def flush(self) -> None:
        if not self._f.closed:
            self._f.flush()

    def close(self) -> None:
        if not self._f.closed:
            self._f.close()


def truncate_resolutions(path: Path, keep_rows: int) -> int:
    """Trim the resoluciones CSV to exactly `keep_rows` data rows (header
    preserved). Robust to embedded newlines in cells (re-parses via csv).
    Returns the number of rows kept."""
    if not path.exists():
        return 0
    with open(path, encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        header = next(r, None)
        rows = []
        for i, row in enumerate(r):
            if i >= keep_rows:
                break
            rows.append(row)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        w.writerows(rows)
    return len(rows)


# --- dry-run catalog reseed ---------------------------------------------------

def _identity_values_from_key(identity_key: str, category_id: str) -> dict[str, str]:
    """Inverse of assignment.identity_key: 'cat|k=v|k=v' -> {k: v}."""
    parts = identity_key.split("|")
    vals: dict[str, str] = {}
    for p in parts[1:]:  # parts[0] is the category id
        if "=" in p:
            k, v = p.split("=", 1)
            vals[k] = v
    return vals


def seed_inmemory_catalog(catalog: InMemoryCatalog, path: Path) -> int:
    """Reconstruct catalog nodes from a products CSV so a resumed dry run keeps
    deduping/parenting against products created before the kill. Returns the
    number of products loaded."""
    if not path.exists():
        return 0
    with open(path, encoding="utf-8-sig", newline="") as f:
        n = 0
        for row in csv.DictReader(f):
            nid = row["node_id"]
            props = json.loads(row["attributes"]) if row["attributes"] else {}
            catalog.specs[nid] = NodeSpec(
                id=nid, category_id=row["category_id"],
                identity_key=row["identity_key"], properties=props,
                specificity=int(row["specificity"] or 0),
                is_complete=row["is_complete"] == "True",
            )
            catalog.nodes[nid] = NodeView(
                id=nid,
                identity_values=_identity_values_from_key(row["identity_key"],
                                                          row["category_id"]),
                parent_id=row["parent_id"] or None,
            )
            n += 1
    return n
