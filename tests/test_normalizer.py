from chilecompra_er.normalize import Normalizer

norm = Normalizer()


def test_units_and_abbreviations():
    assert norm("JERINGA 10ML C/AGUJA 21G") == "jeringa 10 ml con aguja 21 g"


def test_accents_degree_sign_and_number_words():
    assert norm("Cateter foley n°16 100% silicona dos vías") == \
        "cateter foley n 16 100 % silicona 2 vias"


def test_digit_letter_split():
    assert norm("SONDA FOLEY CH16") == "sonda foley ch 16"


def test_decimal_survives():
    assert norm("aguja 5.33 mm") == "aguja 5.33 mm"


def test_version_exposed():
    assert norm.version == "abbreviations_v1"
