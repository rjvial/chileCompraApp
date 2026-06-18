from chilecompra_er.resolve.brand_lexicon import BrandLexicon
from chilecompra_er.resolve.classifier import AMBIGUOUS, CLASSIFIED, UNCLASSIFIED

REGISTER = {"register_version": "t", "categories": [
    {"category_id": "cementos_dentales"},
    {"category_id": "materiales_temporales"},
]}


def _lex(mapping):
    return BrandLexicon(mapping, register=REGISTER)


def test_brand_hit_classifies_to_its_category():
    lex = _lex({"relyx": "cementos_dentales", "cavit": "materiales_temporales"})
    c = lex.classify("kit relyx temp a 3.5")
    assert c.status == CLASSIFIED and c.category_id == "cementos_dentales"
    assert c.tier == "brand"


def test_unknown_brand_is_unclassified():
    lex = _lex({"relyx": "cementos_dentales"})
    assert lex.classify("turbina alta velocidad").status == UNCLASSIFIED


def test_two_brands_different_categories_is_ambiguous():
    lex = _lex({"relyx": "cementos_dentales", "cavit": "materiales_temporales"})
    assert lex.classify("relyx y cavit").status == AMBIGUOUS


def test_brand_to_missing_category_is_dropped():
    # category not in the register -> entry ignored, never matches
    lex = _lex({"ghostbrand": "no_such_category"})
    assert lex.brands == {}
    assert lex.classify("ghostbrand").status == UNCLASSIFIED
