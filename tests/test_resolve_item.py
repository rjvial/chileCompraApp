"""Item-centric resolution (resolve_item): one ItemLicitacion -> one
GenericProduct, classified by pooling buyer line + offer consensus + title."""

from chilecompra_er.ingest import SourceItem, resolve_items
from chilecompra_er.resolve import InMemoryCatalog, Resolver, SourceRef

RUBRIC = "Equipamiento y suministros médicos / Productos quirúrgicos / Sondas"


def r():
    return Resolver(InMemoryCatalog())


def offer(text, awarded=False):
    return {"text": text, "awarded": awarded}


def test_buyer_line_wins_when_it_classifies():
    rep = r().resolve_item("SONDA FOLEY CH16 SILICONA 2 VIAS", tender_text=None,
                           offers=[offer("MARCA X COD 123")])
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas_foley"
    assert rep.evidence["category_source"] == "buyer"


def test_offer_consensus_recovers_rubric_buyer_line():
    # buyer text is a useless rubric path; the offers carry the product.
    rep = r().resolve_item(RUBRIC, tender_text=None,
                           offers=[offer("SONDA FOLEY CH16 2 VIAS"),
                                   offer("SONDA FOLEY CH18 SILICONA")])
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas_foley"
    assert rep.evidence["category_source"] == "offer"


def test_offer_majority_breaks_a_split_vote():
    # two foley offers, one off-topic — majority wins.
    rep = r().resolve_item(RUBRIC, tender_text=None,
                           offers=[offer("SONDA FOLEY CH16 2 VIAS"),
                                   offer("SONDA FOLEY CH18 LATEX"),
                                   offer("GUANTE QUIRURGICO ESTERIL N 7")])
    assert rep.classification.category_id == "sondas_foley"
    assert rep.evidence["offer_votes"]["sondas_foley"] == 2


def test_tender_title_is_the_last_resort():
    rep = r().resolve_item("ITEM 1", tender_text="ADQUISICION DE SONDA FOLEY",
                           offers=[])
    assert rep.classification.category_id == "sondas_foley"
    assert rep.evidence["category_source"] == "tender"


def test_zero_signal_item_stays_unresolved():
    # rubric buyer line, no offers, no usable title -> visible debt (step 1 has
    # no UNSPSC fallback yet).
    rep = r().resolve_item(RUBRIC, tender_text=None, offers=[])
    assert rep.status == "unresolved"
    assert rep.unresolved_reason == "boilerplate_rubric"


def test_all_offers_share_the_items_single_node():
    # The item resolves once; both offers belong to that one generic product.
    rep = r().resolve_item(RUBRIC, tender_text=None,
                           offers=[offer("SONDA FOLEY CH16 2 VIAS"),
                                   offer("SONDA FOLEY CH16 2 VIAS MARCA Y")])
    assert rep.node_id is not None
    assert rep.evidence["n_offers"] == 2


def test_runner_dispatches_item_mode():
    catalog = InMemoryCatalog()
    items = [SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC,
        extra={"tender_text": None,
               "offers": [offer("SONDA FOLEY CH16 2 VIAS")]},
    )]
    stats, _ = resolve_items(Resolver(catalog), items, persist=False,
                             item_mode=True)
    assert stats.total == 1
    assert stats.by_status["resolved_generic"] == 1
    assert stats.by_category["sondas_foley"] == 1
