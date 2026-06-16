from chilecompra_er.profiling import (
    RESIDUE,
    candidate_family_nouns,
    head_noun,
    profile,
)

ROWS = [
    {"text": "SONDA FOLEY 16 SILICONA", "spend_clp": 100},
    {"text": "CATETER FOLEY N°18 LATEX", "spend_clp": 50},   # merges into sonda
    {"text": "JERINGA 10ML DESECHABLE", "spend_clp": 300},
    {"text": "JERINGA 5ML", "spend_clp": 200},
    {"text": "GUANTE NITRILO TALLA L", "spend_clp": 25},
    {"text": "ZZZZQX", "spend_clp": 10},                      # residue
]


def test_synonym_merge_and_spend_ranking():
    stats = profile(ROWS, min_count=2)
    by_group = {s.group: s for s in stats}
    assert by_group["sonda"].records == 2          # cateter -> sonda
    assert stats[0].group == "jeringa"             # highest spend first
    assert by_group["jeringa"].spend_clp == 500
    assert RESIDUE in by_group                     # singletons land here, visible
    assert "zzzzqx" not in by_group                # below min_count: never a family


def test_cumulative_share_monotonic_to_one():
    stats = profile(ROWS, min_count=2)
    shares = [s.cum_share for s in stats]
    assert shares == sorted(shares)
    assert abs(shares[-1] - 1.0) < 1e-9


def test_qualifiers_never_become_families():
    fams = candidate_family_nouns(["jeringa desechable esteril", "guante esteril"])
    assert "esteril" not in fams and "desechable" not in fams


def test_head_noun_first_family_token_wins():
    assert head_noun("sonda foley 16", {"sonda", "foley"}) == "sonda"
    assert head_noun("xyz abc", {"sonda"}) == RESIDUE


def test_tender_context_anchors_a_terse_line():
    # the item line names no family; its tender title does -> it inherits it,
    # and contributes its spend to that group instead of falling into residue.
    rows = [
        {"text": "JERINGA 10ML", "spend_clp": 100},
        {"text": "JERINGA 5ML", "spend_clp": 100},
        {"text": "ITEM 5 SEGUN ANEXO", "context": "ADQUISICION DE JERINGA HOSPITAL",
         "spend_clp": 400},
    ]
    by_group = {s.group: s for s in profile(rows, min_count=1)}
    assert by_group["jeringa"].records == 3
    assert by_group["jeringa"].spend_clp == 600
    assert RESIDUE not in by_group


def test_item_family_wins_over_tender_context():
    # the item already names its family; a different tender title must NOT
    # override it (one tender fans out over many different-product lines).
    rows = [
        {"text": "JERINGA 10ML", "context": "ADQUISICION INSUMOS SONDA FOLEY",
         "spend_clp": 100},
    ]
    by_group = {s.group: s for s in profile(rows, min_count=1)}
    assert "jeringa" in by_group
    assert "sonda" not in by_group
