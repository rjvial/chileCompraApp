from pathlib import Path

from chilecompra_er.categories.schema import load_schema
from chilecompra_er.normalize import Normalizer
from chilecompra_er.resolve import extract
from chilecompra_er.resolve.extractor import canonicalize_measure

norm = Normalizer()
# Fixed fixture schema (original sondas_foley draft) so extraction assertions
# run against a stable schema, not the evolving auto-generated catalog.
schema = load_schema(Path(__file__).parent / "fixtures" / "sondas_foley.json")


def values(raw: str) -> dict:
    return extract(norm(raw), schema).values


def test_same_product_different_wording():
    a = values("SONDA FOLEY CH16 SILICONA 2 VIAS")
    b = values("Cateter foley n°16 100% silicona dos vías")
    assert a == b == {"calibre": "16Fr", "material": "silicona", "vias": "2_vias"}


def test_one_identity_value_differs():
    assert values("SONDA FOLEY 16 LATEX 2 VIAS") == \
        {"calibre": "16Fr", "material": "latex", "vias": "2_vias"}


def test_partial_stays_partial():
    assert values("SONDA FOLEY 16") == {"calibre": "16Fr"}  # unknowns never guessed


def test_latex_siliconado_beats_components():
    assert values("SONDA FOLEY 18 LATEX SILICONADO")["material"] == "latex_siliconado"


def test_balloon_capacity_context_anchored():
    v = values("SONDA FOLEY CH16 SILICONA 2 VIAS BALON 30 CC")
    assert v["capacidad_balon"] == "30cc"
    # a syringe-like volume with no 'balon' context must not leak in
    assert "capacidad_balon" not in values("SONDA FOLEY 16 SILICONA 10 ML")


def test_out_of_domain_dropped_and_recorded():
    ex = extract(norm("SONDA FOLEY 99 FR"), schema)
    assert "calibre" not in ex.values
    assert any(i["attribute"] == "calibre" for i in ex.illegal)


def test_negated_keyword_does_not_fire():
    # "libre de latex" / "sin latex" must NOT stamp material=latex (a latex-FREE
    # product); silicona present still wins.
    assert values("SONDA FOLEY 16 SILICONA LIBRE DE LATEX 2 VIAS")["material"] == "silicona"
    assert "material" not in values("SONDA FOLEY 16 SIN LATEX 2 VIAS")


def test_negation_scope_is_local():
    # a negator bound to a different noun must not suppress a later plain mention:
    # "sin aguja ... con latex" -> latex SHOULD fire.
    assert values("SET SONDA 14 SIN AGUJA CON LATEX 2 VIAS")["material"] == "latex"
    # plain "con latex" unaffected
    assert values("SONDA FOLEY 16 LATEX 2 VIAS")["material"] == "latex"


def test_unit_canonicalization_metric():
    # magnitude conversion within a family (rule templates can't do arithmetic)
    assert canonicalize_measure("8cm", "mm") == "80mm"
    assert canonicalize_measure("2.5cm", "mm") == "25mm"   # fragmentation collapse
    assert canonicalize_measure("1l", "ml") == "1000ml"
    # bare number assumed already in target unit
    assert canonicalize_measure("150", "mm") == "150mm"


def test_unit_canonicalization_leaves_nonconvertible():
    # Fr / gauge are not linearly convertible -> untouched (enum-completion job)
    assert canonicalize_measure("14.5fr", "fr") == "14.5fr"
    assert canonicalize_measure("22g", "fr") == "22g"
    assert canonicalize_measure("20fr", None) == "20fr"
    # a foreign unit for a metric target is left as-is (still drops as illegal)
    assert canonicalize_measure("5kg", "mm") == "5kg"


def test_provenance_recorded():
    ex = extract(norm("SONDA FOLEY 16 LATEX"), schema)
    assert ex.provenance["material"]["matched"] == "latex"
    assert ex.provenance["calibre"]["layer"] == "L1_regex"
