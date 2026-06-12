from chilecompra_er.price import (
    BASIS_PER_BASE_UNIT,
    BASIS_PER_PACK,
    BASIS_UNKNOWN,
    cross_check,
    infer_basis,
)
from chilecompra_er.normalize import Normalizer

norm = Normalizer()


def test_pack_evidence_yields_per_pack():
    b = infer_basis(norm("JERINGA 10ML CAJA 100 UN"))
    assert b.basis == BASIS_PER_PACK and b.pack_size == 100
    assert b.evidence  # provenance always travels with the basis


def test_caja_x_form():
    b = infer_basis(norm("SONDA FOLEY 16 LATEX CAJA X 10"))
    assert b.basis == BASIS_PER_PACK and b.pack_size == 10


def test_no_evidence_is_unknown_never_per_unit():
    assert infer_basis(norm("JERINGA 10ML")).basis == BASIS_UNKNOWN


def test_cross_check_promotes_per_base_unit():
    # unit price 45, 12 boxes of 100: total = 12 * 100 * 45
    assert cross_check(54000, 12, 45, pack_size=100) == BASIS_PER_BASE_UNIT


def test_cross_check_confirms_per_pack():
    # pack price 4200, 12 boxes: total = 12 * 4200
    assert cross_check(50400, 12, 4200, pack_size=100) == BASIS_PER_PACK


def test_cross_check_never_promotes_without_pack_size():
    assert cross_check(540, 12, 45) is None
