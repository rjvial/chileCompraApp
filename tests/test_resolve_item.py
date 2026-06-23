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
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["category_source"] == "buyer"


def test_records_winning_tier_tier1():
    # a Tier-1 (regex) win is tagged tier1 on both the report and the evidence
    rep = r().resolve_item("SONDA FOLEY CH16 SILICONA 2 VIAS", tender_text=None, offers=[])
    assert rep.classification.tier == "tier1"
    assert rep.evidence["classifier"]["tier"] == "tier1"


def test_records_winning_tier_tier2():
    # when a lower tier (here a stubbed Tier-2) produces the category, the item-mode
    # evidence/report carry that tier — the audit hook for Tier-2's contribution
    from chilecompra_er.resolve.classifier import CLASSIFIED, Classification

    class StubTier2:
        register_version = "t"

        def classify(self, _text):
            return Classification("sondas", CLASSIFIED, matched=("p=0.91",), tier="tier2")

    resolver = Resolver(InMemoryCatalog(), classifier=StubTier2())
    rep = resolver.resolve_item("una sonda de descripcion atipica", tender_text=None, offers=[])
    assert rep.classification.tier == "tier2"
    assert rep.evidence["classifier"]["tier"] == "tier2"


def test_by_tier_stat_credits_curated_resolution():
    item = SourceItem(ref=SourceRef("mp", "T", "1", "SONDA FOLEY CH16 2 VIAS"),
                      kind="item", raw_text="SONDA FOLEY CH16 2 VIAS",
                      extra={"offers": []})
    stats, _ = resolve_items(r(), [item], persist=False, item_mode=True, fallback="none")
    assert stats.by_tier["tier1"] == 1


def test_offer_consensus_recovers_rubric_buyer_line():
    # buyer text is a useless rubric path; the offers carry the product.
    rep = r().resolve_item(RUBRIC, tender_text=None,
                           offers=[offer("SONDA FOLEY CH16 2 VIAS"),
                                   offer("SONDA FOLEY CH18 SILICONA")])
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["category_source"] == "offer"


def test_offer_majority_breaks_a_split_vote():
    # two foley offers, one off-topic — majority wins.
    rep = r().resolve_item(RUBRIC, tender_text=None,
                           offers=[offer("SONDA FOLEY CH16 2 VIAS"),
                                   offer("SONDA FOLEY CH18 LATEX"),
                                   offer("GUANTE QUIRURGICO ESTERIL N 7")])
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["offer_votes"]["sondas"] == 2


def test_tender_title_is_the_last_resort():
    rep = r().resolve_item("ITEM 1", tender_text="ADQUISICION DE SONDA FOLEY",
                           offers=[])
    assert rep.classification.category_id == "sondas"
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
    assert rep.classification.category_id == "sondas"


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
    # two offers describing the SAME product (foley 16Fr 2-vias), no brand —
    # they share one generic and one (sin marca) Product.
    return SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC, unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "SONDA FOLEY CH16 2 VIAS",
             "unit_price": 1000.0, "awarded": True},
            {"offer_id": "o2", "text": "SONDA FOLEY CH16 2 VIAS",
             "unit_price": 1200.0, "awarded": False}]},
    )


def test_offers_counted_but_not_written_in_dry_run():
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [_item_with_offers()],
                             persist=False, item_mode=True)
    assert stats.offers_bound == 2
    assert catalog.products == {}  # dry run writes nothing


def test_offers_same_brand_dedup_to_one_product():
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [_item_with_offers()],
                             persist=True, item_mode=True)
    assert stats.offers_bound == 2
    # both offers carry no brand -> the (sin marca) Brand -> ONE branded Product
    assert len(catalog.products) == 1
    prod = next(iter(catalog.products.values()))
    assert prod["brand_id"] == "(sin marca)"
    assert "(sin marca)" in catalog.brands
    # price is NOT on the Product; it rides each (:Oferta)-[:OFFERS]->(:Product) edge
    assert "unit_price" not in prod
    assert len(catalog.offers) == 2
    assert {o["unit_price"] for o in catalog.offers} == {1000.0, 1200.0}
    # the explicit (:ItemLicitacion)-[:HAS_RECORD]->(:SourceRecord) edge for the item
    assert ("mp_item_licitacion|L1|1", "L1|1") in catalog.has_record


def _item_with_branded_offers():
    return SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC, unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "SONDA FOLEY CH16 2 VIAS MARCA RUSCH",
             "unit_price": 1000.0, "awarded": True},
            {"offer_id": "o2", "text": "SONDA FOLEY CH16 2 VIAS MARCA COLOPLAST",
             "unit_price": 1200.0, "awarded": False}]},
    )


def test_distinct_brands_make_distinct_products_one_generic():
    catalog = InMemoryCatalog()
    resolve_items(Resolver(catalog), [_item_with_branded_offers()],
                  persist=True, item_mode=True)
    # Product = Brand × GenericProduct: two brands -> two Products, ONE shared generic
    assert len(catalog.products) == 2
    assert len({p["generic_id"] for p in catalog.products.values()}) == 1
    assert {p["brand_id"] for p in catalog.products.values()} == {"rusch", "coloplast"}
    assert set(catalog.brands) == {"rusch", "coloplast"}
    assert len(catalog.offers) == 2


# --- offer-aware resolution (Oferta↔Product↔Generic consistency) -------------

def test_offer_more_specific_refines_to_finer_generic():
    # buyer/consensus generic = foley 16Fr 2-vias; a second offer is a DIFFERENT,
    # more-specific spec (18Fr silicona) -> it earns its own (finer) generic
    # instead of being force-merged onto the item's node.
    item = SourceItem(
        ref=SourceRef("mp_item_licitacion", "L1", "1", RUBRIC),
        kind="item", raw_text=RUBRIC, unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "SONDA FOLEY CH16 2 VIAS",
             "unit_price": 1000.0, "awarded": True},
            {"offer_id": "o2", "text": "SONDA FOLEY CH18 SILICONA",
             "unit_price": 1200.0, "awarded": False}]},
    )
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [item], persist=True, item_mode=True)
    assert len({p["generic_id"] for p in catalog.products.values()}) == 2
    assert stats.offer_routing["refined"] == 1
    assert stats.offer_routing["same"] == 1
    # both stay in-family -> conforming
    assert all(o["conforming"] for o in catalog.offers)


def test_offer_in_different_family_recategorized_nonconforming():
    # buyer line is a sonda; one offer is actually an aguja (different family) ->
    # it binds to an agujas generic, flagged conforming=False, not forced onto sondas.
    item = SourceItem(
        ref=SourceRef("mp_item_licitacion", "L2", "1", "SONDA FOLEY CH16"),
        kind="item", raw_text="SONDA FOLEY CH16", unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "SONDA FOLEY CH16 2 VIAS",
             "unit_price": 1000.0, "awarded": True},
            {"offer_id": "o2", "text": "AGUJA HIPODERMICA 21G",
             "unit_price": 50.0, "awarded": False}]},
    )
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [item], persist=True, item_mode=True)
    cats = {catalog.specs[p["generic_id"]].category_id for p in catalog.products.values()}
    assert "sondas" in cats and "agujas" in cats
    conforming = {o["oferta_id"]: o["conforming"] for o in catalog.offers}
    assert conforming["o1"] is True and conforming["o2"] is False
    assert stats.offer_routing["recategorized"] == 1


def test_vague_offer_stays_on_item_generic():
    # an offer too terse to classify falls back to the item's node (conservative).
    item = SourceItem(
        ref=SourceRef("mp_item_licitacion", "L3", "1", "SONDA FOLEY CH16 2 VIAS"),
        kind="item", raw_text="SONDA FOLEY CH16 2 VIAS", unspsc=42182200,
        extra={"tender_text": None, "offers": [
            {"offer_id": "o1", "text": "segun bases", "unit_price": 999.0}]},
    )
    catalog = InMemoryCatalog()
    stats, _ = resolve_items(Resolver(catalog), [item], persist=True, item_mode=True)
    assert stats.offer_routing["conservative"] == 1
    assert catalog.offers[0]["conforming"] is True


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
    assert stats.by_category["sondas"] == 1


def test_offer_rebinding_is_idempotent():
    # re-binding the same offer to a different Product replaces its edge — no
    # stale duplicate (the bug a re-resolve with retargeting would otherwise hit).
    catalog = InMemoryCatalog()
    catalog.link_offer("o1", "pr_old", {"unit_price": 100.0})
    catalog.link_offer("o1", "pr_new", {"unit_price": 100.0})
    edges = [o for o in catalog.offers if o["oferta_id"] == "o1"]
    assert len(edges) == 1 and edges[0]["product_id"] == "pr_new"
