from chilecompra_er.fallback import bucket_ranking, residue_ranking, _is_rubric
from chilecompra_er.profiling import RESIDUE

# rows as fetch_fallback_items() returns them: per-item, with the bucket's
# attributed spend and the UNSPSC code.
ROWS = [
    {"code": "unspsc_42151601", "text": "TURBINA ALTA VELOCIDAD MIDWEST", "spend_clp": 100},
    {"code": "unspsc_42151601", "text": "TURBINA DENTAL ALTA VELOCIDAD", "spend_clp": 100},
    {"code": "unspsc_42151601", "text": "PIEZA DE MANO RECTA BAJA VELOCIDAD", "spend_clp": 100},
    {"code": "unspsc_42151601",
     "text": "Equipamiento y suministros medicos / Equipos odontologicos / Instrumentos",
     "spend_clp": 100},  # rubric
    {"code": "unspsc_42132205", "text": "GUANTE NITRILO TALLA L", "spend_clp": 50},
    {"code": "unspsc_42132205", "text": "GUANTE LATEX TALLA M", "spend_clp": 50},
]


def test_is_rubric():
    assert _is_rubric("a / b / c")
    assert not _is_rubric("TURBINA ALTA VELOCIDAD")


def test_bucket_ranking_orders_by_item_volume_and_counts_rubric():
    buckets = bucket_ranking(ROWS, min_count=1)
    assert [b.code for b in buckets] == ["unspsc_42151601", "unspsc_42132205"]
    top = buckets[0]
    assert top.items == 4
    assert top.rubric_items == 1                 # the boilerplate line
    fams = dict(top.top_families)
    assert fams.get("turbina") == 2              # head-noun grouping within bucket
    assert RESIDUE not in fams                   # residue is excluded from top families


def test_residue_ranking_surfaces_families():
    fams = {s.group: s for s in residue_ranking(ROWS, min_count=2)}
    assert "turbina" in fams and fams["turbina"].records == 2
    assert "guante" in fams and fams["guante"].records == 2
