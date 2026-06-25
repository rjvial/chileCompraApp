"""Unit tests for the L1 profile contract (resolve/profile.py) and the L0 dedup
in the canonicalization driver (resolve/canonicalize.py). Pure / offline — no
graph, no LLM."""
from __future__ import annotations

from chilecompra_er.normalize import Normalizer
from chilecompra_er.resolve import profile as P
from chilecompra_er.resolve.canonicalize import ProfileStore, canonicalize


def _profile(**kw) -> P.Profile:
    base = dict(is_product=True, category="sondas_foley",
                identity_attributes=(P.IdentityAttr("calibre", "16fr", "16 fr"),
                                     P.IdentityAttr("material", "latex", "latex")),
                brand="bbraun", model_token=None,
                packaging=P.Packaging(2, "caja", "caja x2"),
                base_unit="unidad", confidence="high", flags=())
    base.update(kw)
    return P.Profile(**base)


# --- signature ---------------------------------------------------------------

def test_signature_is_sorted_and_brand_packaging_free():
    p = _profile()
    # sorted by (name, value); brand 'bbraun' and packaging must not appear
    assert p.signature() == "sondas_foley|calibre=16fr|material=latex"
    assert "bbraun" not in p.signature() and "caja" not in p.signature()


def test_signature_attribute_order_independent():
    a = _profile(identity_attributes=(P.IdentityAttr("material", "latex", "x"),
                                      P.IdentityAttr("calibre", "16fr", "y")))
    b = _profile(identity_attributes=(P.IdentityAttr("calibre", "16fr", "y"),
                                      P.IdentityAttr("material", "latex", "x")))
    assert a.signature() == b.signature()


def test_signature_empty_attrs_is_bare_category():
    assert _profile(identity_attributes=()).signature() == "sondas_foley"


# --- text_hash ---------------------------------------------------------------

def test_text_hash_deterministic_and_distinct():
    assert P.text_hash("sonda foley 16") == P.text_hash("sonda foley 16")
    assert P.text_hash("sonda foley 16") != P.text_hash("sonda foley 18")


# --- parse / round-trip ------------------------------------------------------

def test_parse_profile_round_trip():
    p = _profile()
    d = P.profile_to_dict(p)
    assert P.parse_profile(d) == p           # frozen dataclasses compare by value


def test_batch_message_and_schema():
    items = [("0", "sonda foley 16", 42142500), ("1", "aguja 21g", None)]
    msg = P.build_batch_message(items)
    assert "id=0" in msg and "id=1" in msg and "42142500" in msg
    assert "profiles" in P.BATCH_SCHEMA["properties"]
    # the per-item schema gained an id, keeping the profile fields
    item = P.BATCH_SCHEMA["properties"]["profiles"]["items"]
    assert "id" in item["properties"] and "category" in item["properties"]
    assert "id" in item["required"]


def test_category_normalized_for_stable_blocking():
    d = {"is_product": True, "category": "Cordón eléctrico",
         "identity_attributes": [], "brand": None, "model_token": None,
         "packaging": {"pack_size": None, "pack_unit": None, "evidence": None},
         "base_unit": None, "confidence": "low", "flags": []}
    assert P.parse_profile(d).category == "cordon_electrico"
    # spaces / casing / accents all collapse to the same block key
    assert P.normalize_category("cordon electrico") == "cordon_electrico"
    assert P.normalize_category("sondas") == "sondas"


def test_parse_profile_from_raw_llm_dict():
    d = {"is_product": True, "category": "concentrado_acido",
         "identity_attributes": [{"name": "calcio", "value": "2.5meq_l",
                                  "evidence": "2,5 mEqL de Ca"}],
         "brand": "renalyte", "model_token": "326",
         "packaging": {"pack_size": None, "pack_unit": None, "evidence": None},
         "base_unit": "unidad", "confidence": "high", "flags": []}
    p = P.parse_profile(d)
    assert p.identity_attributes[0].evidence == "2,5 mEqL de Ca"  # evidence preserved
    assert p.signature() == "concentrado_acido|calcio=2.5meq_l"


# --- user message + system prompt --------------------------------------------

def test_build_user_message_includes_optional_context():
    msg = P.build_user_message("sonda foley", unspsc=42142500, buyer_line="SONDA")
    assert "DESCRIPTION" in msg and "42142500" in msg and "BUYER_LINE" in msg
    bare = P.build_user_message("sonda foley")
    assert "UNSPSC" not in bare and "BUYER_LINE" not in bare


def test_system_prompt_carries_families_and_cardinal_rule():
    register = {"categories": [{"category_id": "sondas_foley"},
                               {"category_id": "agujas"}]}
    sp = P.system_prompt(register)
    assert "sondas_foley" in sp and "agujas" in sp
    assert "EVIDENCE OR NOTHING" in sp        # the cardinal rule is present


# --- L0 dedup + cache (driver, dry-run, no LLM) ------------------------------

def test_canonicalize_l0_dedup_and_cache(tmp_path):
    store = ProfileStore(tmp_path / "profiles.jsonl")
    records = ["Sonda Foley 16", "SONDA  FOLEY 16",          # same after normalize
               ("Aguja 21G", 42182200)]                       # tuple form (with unspsc)
    stats = canonicalize(records, store, dry_run=True)
    assert stats.total_inputs == 3
    assert stats.distinct == 2                                 # the two foley lines collapse
    assert stats.cached == 0

    # Pre-seed one profile; a re-run must skip it (the persisted pure-function cache).
    h = P.text_hash(Normalizer()("Aguja 21G"))
    store.put_many({h: P.profile_to_dict(_profile(category="agujas"))})
    stats2 = canonicalize(records, store, dry_run=True)
    assert stats2.cached == 1
