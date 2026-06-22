from chilecompra_er.resolve.brand import SIN_MARCA, extract_brand


def test_explicit_marca_label_wins():
    bid, name, src = extract_brand("APOSITO TRANSPARENTE MARCA: HEALFLEX 10X12")
    assert (bid, name, src) == ("healflex", "HEALFLEX", "marca")


def test_alphanumeric_brand_survives_from_raw_text():
    # "3M" must not be shredded to "3" (which is what normalized text would do)
    bid, name, src = extract_brand("CINTA QUIRURGICA Marca 3M MICROPORE")
    assert bid == "3m" and name == "3M" and src == "marca"


def test_marca_filler_words_are_not_brands():
    # "marca registrada" / "marca propia" name no brand -> falls through to sentinel
    assert extract_brand("PRODUCTO MARCA REGISTRADA")[0] == SIN_MARCA
    assert extract_brand("ARTICULO marca propia del hospital")[0] == SIN_MARCA


def test_lexicon_token_as_fallback():
    # no "marca" label, but a known brand token appears anywhere in the text
    bmap = {"relyx": "cementos_dentales"}
    bid, name, src = extract_brand("CEMENTO RESINOSO RELYX U200 AUTOMIX", bmap)
    assert bid == "relyx" and src == "lexicon"


def test_marca_label_beats_lexicon():
    bmap = {"relyx": "cementos_dentales"}
    # explicit label takes priority over an incidental lexicon token
    bid, _, src = extract_brand("RELYX COMPATIBLE, MARCA: medline", bmap)
    assert bid == "medline" and src == "marca"


def test_no_brand_is_sentinel():
    assert extract_brand("GUANTE NITRILO TALLA L") == (SIN_MARCA, SIN_MARCA, "none")
    assert extract_brand(None) == (SIN_MARCA, SIN_MARCA, "none")


def test_accents_stripped_in_id_casing_kept_in_name():
    bid, name, _ = extract_brand("SUTURA marca Médica")
    assert bid == "medica" and name == "Médica"
