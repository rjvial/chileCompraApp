"""L0 dedup -> L1 canonicalize driver (design: the L0->L3 redesign).

Turns raw `descripcion_proveedor` strings into persisted canonical Profiles.
Canonicalize ONCE per distinct normalized text, cache by text-hash, reuse — so
the LLM step is a deterministic pure function and a re-run is nearly free.

SCAFFOLD: the pure core (dedup, skip-cached, batch, persist) is implemented; the
graph source fetch and the choice of persistent store backend are marked TODO —
both are small and decided in the Phase-1 build, not here.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from ..categories.schema import load_register
from ..normalize import Normalizer
from . import profile as P


# --- profile store (the cache; JSONL keyed by text_hash) ----------------------
# Phase-1 scaffold uses a local JSONL file. TODO(phase-2): decide whether the
# canonical store is JSONL/parquet under data/ or a :Profile node in the graph.

class ProfileStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._cache: dict[str, dict] = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    rec = json.loads(line)
                    self._cache[rec["text_hash"]] = rec["profile"]

    def has(self, text_hash: str) -> bool:
        return text_hash in self._cache

    def get(self, text_hash: str) -> P.Profile | None:
        d = self._cache.get(text_hash)
        return P.parse_profile(d) if d else None

    def put_many(self, items: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            for text_hash, prof in items.items():
                self._cache[text_hash] = prof
                f.write(json.dumps({"text_hash": text_hash, "profile": prof},
                                   ensure_ascii=False) + "\n")

    def profiles(self) -> list[P.Profile]:
        """Every stored profile, for the L2 matcher."""
        return [P.parse_profile(d) for d in self._cache.values()]

    def items(self) -> list[tuple[str, P.Profile]]:
        """(text_hash, profile) pairs — the matcher works on the profiles and
        persistence maps each cluster member back to its text-hash (and so to its
        offers) via this parallel order."""
        return [(h, P.parse_profile(d)) for h, d in self._cache.items()]

    def __len__(self) -> int:
        return len(self._cache)


@dataclass
class CanonStats:
    total_inputs: int = 0
    distinct: int = 0
    cached: int = 0
    canonicalized: int = 0
    failed: int = 0


_BATCH_SIZE = 50_000  # under the API's 100k-requests / 256MB per-batch limit


def canonicalize(records, store: ProfileStore, *,
                 register: dict | None = None,
                 normalizer: Normalizer | None = None,
                 model: str = "claude-haiku-4-5",
                 batch_size: int = _BATCH_SIZE,
                 dry_run: bool = False,
                 log=lambda _m: None) -> CanonStats:
    """Canonicalize an iterable of records into the store.

    Each record is either a raw description string, or a `(description, unspsc)`
    tuple (the graph source supplies the item's UNSPSC as a category hint).
    L0 dedup: distinct *normalized* texts only. Skips any text already in the
    store (the persisted cache), so re-runs only pay for genuinely new strings.
    """
    register = register or load_register()
    normalizer = normalizer or Normalizer()
    system = P.system_prompt(register)
    stats = CanonStats()

    # L0: dedup by normalized text-hash; keep one raw exemplar + UNSPSC per hash.
    by_hash: dict[str, str] = {}
    unspsc_by_hash: dict[str, int] = {}
    for rec in records:
        raw, unspsc = rec if isinstance(rec, tuple) else (rec, None)
        stats.total_inputs += 1
        norm = normalizer(raw) if raw else ""
        if not norm:
            continue
        h = P.text_hash(norm)
        by_hash.setdefault(h, raw)
        if unspsc is not None:
            unspsc_by_hash.setdefault(h, unspsc)
    stats.distinct = len(by_hash)

    todo = {h: raw for h, raw in by_hash.items() if not store.has(h)}
    stats.cached = stats.distinct - len(todo)
    log(f"L0: {stats.total_inputs:,} inputs -> {stats.distinct:,} distinct; "
        f"{stats.cached:,} cached, {len(todo):,} to canonicalize")

    if dry_run:
        log("dry run — L0 only, no LLM calls (no API credits spent)")
        return stats

    items = list(todo.items())
    for start in range(0, len(items), batch_size):
        chunk = items[start:start + batch_size]
        from ..llm import complete_json_batch
        requests = [
            (h, P.build_user_message(raw, unspsc=unspsc_by_hash.get(h)))
            for h, raw in chunk
        ]
        results = complete_json_batch(requests, P.PROFILE_SCHEMA, system,
                                      model=model, log=log)
        to_store: dict[str, dict] = {}
        for h, _raw in chunk:
            d = results.get(h)
            if d is None:
                stats.failed += 1
                continue
            try:
                to_store[h] = P.profile_to_dict(P.parse_profile(d))
                stats.canonicalized += 1
            except (KeyError, TypeError) as exc:
                stats.failed += 1
                log(f"  parse failed for {h[:12]}: {exc}")
        store.put_many(to_store)
        log(f"  persisted batch {start // batch_size + 1}: "
            f"{len(to_store)} profiles (store now {len(store):,})")
    return stats


def fetch_distinct_descriptions(conn, *, unspsc_segment: int | None = None,
                                limit: int | None = None):
    """Stream `(description, unspsc)` records from the graph for L0/L1.

    A thin generator over ingest.neo4j_source.fetch_offer_descriptions —
    dedup is done by `canonicalize()` (by normalized text-hash), so this stays a
    plain streamed scan. The connection must stay open while the result is
    consumed (it's lazy). Scope a run with `unspsc_segment`."""
    from ..ingest.neo4j_source import fetch_offer_descriptions

    yield from fetch_offer_descriptions(conn, unspsc_segment=unspsc_segment,
                                        limit=limit)
