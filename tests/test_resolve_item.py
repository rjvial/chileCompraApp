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


def test_unspsc_fallback_links_unmatched_item():
    rep = r().resolve_item(RUBRIC, tender_text=None, offers=[],
                           unspsc=42182200, fallback="unspsc")
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "unspsc_42182200"
    assert rep.evidence["category_source"] == "unspsc_fallback"
    assert rep.node_id is not None


def test_fallback_none_leaves_item_unresolved():
    rep = r().resolve_item(RUBRIC, tender_text=None, offers=[],
                           unspsc=42182200, fallback="none")
    assert rep.status == "unresolved"


def test_curated_match_beats_fallback():
    # a classifiable buyer line never falls through to the UNSPSC bucket
    rep = r().resolve_item("SONDA FOLEY CH16 2 VIAS", tender_text=None, offers=[],
                           unspsc=42182200, fallback="unspsc")
    assert rep.classification.category_id == "sondas_foley"


def test_same_unspsc_code_shares_one_fallback_node():
    res = r()
    a = res.resolve_item(RUBRIC, tender_text=None, offers=[],
                         unspsc=42182200, fallback="unspsc")
    b = res.resolve_item("OTRA COSA RARA SIN FAMILIA", tender_text=None, offers=[],
                         unspsc=42182200, fallback="unspsc")
    assert a.node_id == b.node_id  # one bucket node per code


def test_fallback_counts_in_stats():
    catalog = InMemoryCatalog()
    items = [SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC, unspsc=42182200,
        extra={"tender_text": None, "offers": []},
    )]
    stats, _ = resolve_items(Resolver(catalog), items, persist=False,
                             item_mode=True, fallback="unspsc")
    assert stats.by_status["resolved_generic"] == 1
    assert stats.resolved_via_fallback == 1


def _item_with_offers():
    return SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC, unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "SONDA FOLEY CH16 2 VIAS",
             "unit_price": 1000.0, "awarded": True},
            {"offer_id": "o2", "text": "SONDA FOLEY CH18 SILICONA",
             "unit_price": 1200.0, "awarded": False}]},
    )


def test_offers_counted_but_not_written_in_dry_run():
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [_item_with_offers()],
                             persist=False, item_mode=True)
    assert stats.offers_bound == 2
    assert catalog.products == {}  # dry run writes nothing


def test_offers_bound_as_products_share_one_generic_node():
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [_item_with_offers()],
                             persist=True, item_mode=True)
    assert stats.offers_bound == 2
    assert len(catalog.products) == 2
    # the intra-item invariant: both offers' Products point to ONE generic node
    assert len({p["generic_id"] for p in catalog.products.values()}) == 1
    assert all(p["unit_price"] for p in catalog.products.values())


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
