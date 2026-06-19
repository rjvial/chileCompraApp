"""Schema generation builds OVER an established register: a schema already on
disk is never re-drafted (and so never clobbered) unless overwrite is asked."""

from chilecompra_er import register as reg
from chilecompra_er import strawman
from chilecompra_er.register import Candidate

_FAM = {"category_id": "jeringas", "name": "Jeringas",
        "corpus_regex": "(?i).*jering.*"}


def _stub_draft_chain(monkeypatch):
    """Replace the LLM + corpus calls so generate() can write without a graph."""
    monkeypatch.setattr(strawman, "fetch_samples",
                        lambda conn, regex, n: ["jeringa 5ml"] * 6)
    monkeypatch.setattr(strawman, "complete_json", lambda *a, **k: {"drafted": True})
    monkeypatch.setattr(strawman, "curate",
                        lambda draft, cid, name: ({"category_id": cid,
                                                   "attribute_defs": [],
                                                   "curation_log": []}, []))
    monkeypatch.setattr(strawman, "dry_run",
                        lambda path, normalized: {"samples": 6, "coverage": 1.0,
                                                  "illegal": 0})


def test_generate_skips_existing_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(strawman, "families", lambda *a, **k: [_FAM])
    monkeypatch.setattr(strawman, "SCHEMAS_DIR", tmp_path)
    existing = tmp_path / "jeringas.json"
    existing.write_text('{"hand": "edited"}', encoding="utf-8")
    # if generate tried to draft, it would call these — make that an error
    monkeypatch.setattr(strawman, "complete_json",
                        lambda *a, **k: (_ for _ in ()).throw(
                            AssertionError("re-drafted an existing schema")))

    written = strawman.generate(conn=None, only="jeringas", log=lambda *_: None)

    assert written == []
    assert existing.read_text(encoding="utf-8") == '{"hand": "edited"}'  # untouched


def test_generate_drafts_missing_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(strawman, "families", lambda *a, **k: [_FAM])
    monkeypatch.setattr(strawman, "SCHEMAS_DIR", tmp_path)
    _stub_draft_chain(monkeypatch)

    written = strawman.generate(conn=None, only="jeringas", log=lambda *_: None)

    assert written == [tmp_path / "jeringas.json"]
    assert (tmp_path / "jeringas.json").exists()


def test_generate_overwrite_redrafts(tmp_path, monkeypatch):
    monkeypatch.setattr(strawman, "families", lambda *a, **k: [_FAM])
    monkeypatch.setattr(strawman, "SCHEMAS_DIR", tmp_path)
    (tmp_path / "jeringas.json").write_text('{"old": true}', encoding="utf-8")
    _stub_draft_chain(monkeypatch)

    written = strawman.generate(conn=None, only="jeringas", overwrite=True,
                                log=lambda *_: None)

    assert written == [tmp_path / "jeringas.json"]
    assert '"old"' not in (tmp_path / "jeringas.json").read_text(encoding="utf-8")


# --- apply() builds over the existing register -------------------------------

def _cand(cid):
    return Candidate(token=cid, records=99, spend_share=0.01, viable=True,
                     category_id=cid, name=cid.title(), include=[f"\\b{cid}\\b"],
                     corpus_regex=f"(?i).*{cid}.*", canonical_example=f"{cid} x")


def test_apply_skips_already_registered_category(monkeypatch):
    # the candidate is already in the register
    monkeypatch.setattr(reg, "load_register",
                        lambda *a, **k: {"categories": [{"category_id": "jeringas"}]})
    monkeypatch.setattr("chilecompra_er.strawman.fetch_samples",
                        lambda conn, regex, n: ["x"] * 10)  # passes the yield gate
    added, generated = [], []
    monkeypatch.setattr(reg, "add_category",
                        lambda **kw: added.append(kw["category_id"])
                        or {"category_id": kw["category_id"]})
    monkeypatch.setattr("chilecompra_er.strawman.generate",
                        lambda conn, only, **kw: generated.append(only) or [])

    reg.apply(conn=None, candidates=[_cand("jeringas")], log=lambda *_: None)

    assert added == []                 # NOT re-added (no crash, no version bump)
    assert generated == ["jeringas"]   # schema still gap-filled (generate skips if present)


def test_apply_registers_a_new_category(monkeypatch):
    monkeypatch.setattr(reg, "load_register",
                        lambda *a, **k: {"categories": [{"category_id": "jeringas"}]})
    monkeypatch.setattr("chilecompra_er.strawman.fetch_samples",
                        lambda conn, regex, n: ["x"] * 10)
    added, generated = [], []
    monkeypatch.setattr(reg, "add_category",
                        lambda **kw: added.append(kw["category_id"])
                        or {"category_id": kw["category_id"]})
    monkeypatch.setattr("chilecompra_er.strawman.generate",
                        lambda conn, only, **kw: generated.append(only) or [])

    reg.apply(conn=None, candidates=[_cand("mascarillas")], log=lambda *_: None)

    assert added == ["mascarillas"]        # new family added
    assert generated == ["mascarillas"]
