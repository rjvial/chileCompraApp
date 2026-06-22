from chilecompra_er.price import (
    BASIS_PER_BASE_UNIT,
    BASIS_PER_PACK,
    BASIS_UNKNOWN,
    cross_check,
    infer_basis,
    normalize_unit_prices,
    pack_size_from_text,
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


# --- pack_size_from_text ------------------------------------------------------

def test_pack_size_forms():
    assert pack_size_from_text(norm("TELA MICROPORE CAJA X 12 UNIDADES")) == 12
    assert pack_size_from_text(norm("SET DE 2 JERINGAS DE 2 G")) == 2
    assert pack_size_from_text(norm("RESINA FLOW A2 JERINGA 2 GR")) is None  # no pack
    assert pack_size_from_text(norm("CINTA 2,5 CM X 9,1 MT")) is None        # dims, not a pack
    assert pack_size_from_text(None) is None


# --- normalize_unit_prices (distribution-aware) -------------------------------

def test_cluster_keeps_per_unit_and_divides_per_pack():
    # micropore tape: per-roll cluster ~230..753, one per-box-of-12 quote at 6192
    prices = [230, 280, 320, 400, 500, 506, 753, 6192]
    packs  = [12,  None, 12, None, 12, None, 12, 12]   # the 6192 row says "caja x 12"
    out = normalize_unit_prices(prices, packs, min_n=5)
    bases = [b for _, b in out]
    # the seven cluster prices stay per base unit (kept as-is)...
    assert bases[:7] == [BASIS_PER_BASE_UNIT] * 7
    assert [round(p) for p, _ in out[:7]] == [230, 280, 320, 400, 500, 506, 753]
    # ...and the 6192 outlier is recognized as per_pack -> 6192/12 = 516
    assert bases[7] == BASIS_PER_PACK and round(out[7][0]) == 516


def test_outlier_without_pack_size_is_flagged_not_guessed():
    prices = [230, 280, 320, 400, 500, 506, 9999]   # last is a far outlier...
    packs  = [None] * 7                              # ...with NO pack size to divide by
    out = normalize_unit_prices(prices, packs, min_n=5)
    assert out[-1] == (None, BASIS_UNKNOWN)          # flagged, never assumed
    assert all(b == BASIS_PER_BASE_UNIT for _, b in out[:6])


def test_sparse_product_is_all_unknown():
    # too few offers to define a cluster -> nothing normalized
    out = normalize_unit_prices([230, 250], [None, None], min_n=5)
    assert out == [(None, BASIS_UNKNOWN), (None, BASIS_UNKNOWN)]


def test_spurious_pack_size_rejected_by_cluster():
    # a wrong pack size (e.g. parsed from a volume) must not normalize a good price:
    # 250/300 would fall far below the ~230..506 cluster, so it's left as-is.
    prices = [230, 280, 320, 400, 500, 250]
    packs  = [None, None, None, None, None, 300]   # bogus size on the last
    out = normalize_unit_prices(prices, packs, min_n=5)
    assert out[-1][1] == BASIS_PER_BASE_UNIT and round(out[-1][0]) == 250
