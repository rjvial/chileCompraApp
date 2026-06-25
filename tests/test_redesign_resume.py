"""Kill/resume guarantees for the redesign stages: L1 persists incrementally and
resumes by skipping cached profiles; the L2 edge write has a stream-offset
checkpoint. Monkeypatched — no LLM, no graph."""
from __future__ import annotations

import re

from chilecompra_er.normalize import Normalizer
from chilecompra_er.resolve import canonicalize as CZ
from chilecompra_er.resolve import profile as P


def _raw():
    return {"is_product": True, "category": "x", "identity_attributes": [],
            "brand": None, "model_token": None,
            "packaging": {"pack_size": None, "pack_unit": None, "evidence": None},
            "base_unit": "unidad", "confidence": "high", "flags": []}


def test_canonicalize_resumes_by_skipping_cached(tmp_path, monkeypatch):
    norm = Normalizer()
    store = CZ.ProfileStore(tmp_path / "p.jsonl")
    recs = ["Sonda Foley 16", "Aguja 21G", "Guante M", "Gasa 10x10"]

    # simulate a prior run that already canonicalized the first two
    store.put_many({P.text_hash(norm(r)): _raw() for r in recs[:2]})

    asked_ids: list[str] = []

    def fake_many(requests, schema, system, **kw):
        # each request is a GROUP; echo a profile per item id in the prompt
        on_result = kw["on_result"]
        for cid, user in requests:
            ids = re.findall(r"id=(\d+)", user)
            asked_ids.extend(ids)
            on_result(cid, {"profiles": [{**_raw(), "id": i} for i in ids]})
        return {}

    monkeypatch.setattr("chilecompra_er.llm.complete_json_many", fake_many)
    stats = CZ.canonicalize(recs, store, normalizer=norm)

    assert stats.distinct == 4 and stats.cached == 2 and stats.canonicalized == 2
    assert len(asked_ids) == 2           # only the two uncached items were sent
    assert len(store) == 4               # all now persisted (durable)

    asked_ids.clear()
    again = CZ.canonicalize(recs, store, normalizer=norm)   # full no-op
    assert again.cached == 4 and asked_ids == []


def test_priced_in_checkpoint_roundtrip(tmp_path):
    from chilecompra_er.ingest.clusters import (
        _write_priced_in_checkpoint,
        read_priced_in_checkpoint,
    )
    p = tmp_path / "match.checkpoint.json"
    assert read_priced_in_checkpoint(p) == 0       # absent → start from 0
    _write_priced_in_checkpoint(p, 1234)
    assert read_priced_in_checkpoint(p) == 1234    # resume offset
