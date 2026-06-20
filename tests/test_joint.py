"""Joint tender + offer resolution (resolve_joint)."""

from pathlib import Path

from chilecompra_er.categories.schema import load_schema
from chilecompra_er.ingest import SourceItem, resolve_items
from chilecompra_er.resolve import InMemoryCatalog, Resolver, SourceRef
from chilecompra_er.resolve.resolver import REASON_CONFLICT

RUBRIC = "Equipamiento y suministros médicos / Productos quirúrgicos / Sondas"

# Fixed fixture catalog so resolve_joint runs against a stable register+schema
# (calibre/material/vias extraction; a jeringas family for the conflict case),
# independent of the evolving live catalog.
_SCHEMA = load_schema(Path(__file__).parent / "fixtures" / "sondas_foley.json")
_REG = {"register_version": "test", "categories": [
    {"category_id": "sondas", "status": "candidate", "include": ["\\bsonda\\w*"],
     "exclude": [], "schema_file": "x", "corpus_regex": "(?i).*sonda.*"},
    {"category_id": "jeringas", "status": "candidate", "include": ["\\bjeringa\\w*"],
     "exclude": [], "schema_file": "x", "corpus_regex": "(?i).*jeringa.*"},
]}


def r():
    res = Resolver(InMemoryCatalog(), register=_REG)
    res._schemas["sondas"] = _SCHEMA      # fixture schema drives extraction
    res._schemas["jeringas"] = _SCHEMA    # placeholder (jeringas is only ever a conflict)
    return res


def test_offer_recovers_boilerplate_buyer_line():
    # buyer text is a useless rubric path; the offer text carries the product.
    rep = r().resolve_joint(offer_text="SONDA FOLEY CH16 SILICONA 2 VIAS",
                            buyer_text=RUBRIC)
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["category_source"] == "offer"
    assert rep.extraction.values.get("calibre") == "16Fr"


def test_buyer_used_when_offer_does_not_classify():
    # offer text has no family word; buyer text classifies.
    rep = r().resolve_joint(offer_text="producto segun bases tecnicas",
                            buyer_text="SONDA FOLEY CH18 LATEX 2 VIAS")
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["category_source"] == "buyer"


def test_conflict_when_texts_disagree_on_category():
    rep = r().resolve_joint(offer_text="SONDA FOLEY CH16 SILICONA",
                            buyer_text="JERINGA 10ML LUER SLIP")
    assert rep.status == "unresolved"
    assert rep.unresolved_reason == REASON_CONFLICT
    assert set(rep.classification.matched) == {"sondas", "jeringas"}


def test_attributes_merge_offer_wins():
    # both classify to foley; buyer says latex, offer says silicona -> offer wins,
    # and the offer adds the calibre the buyer omitted.
    rep = r().resolve_joint(
        offer_text="SONDA FOLEY CH16 SILICONA 2 VIAS",
        buyer_text="SONDA FOLEY LATEX")
    assert rep.classification.category_id == "sondas"
    assert rep.extraction.values["material"] == "silicona"   # offer beat buyer's latex
    assert rep.extraction.values["calibre"] == "16Fr"        # only the offer had it
    assert rep.extraction.provenance["material"]["text"] == "offer"


def test_both_boilerplate_stays_unresolved():
    rep = r().resolve_joint(offer_text=RUBRIC, buyer_text=RUBRIC)
    assert rep.status == "unresolved"
    assert rep.unresolved_reason == "boilerplate_rubric"


def test_runner_joint_flag_drives_resolve_joint():
    cat = InMemoryCatalog()
    items = [
        SourceItem(ref=SourceRef("mp", "T", "1:9", "SONDA FOLEY CH16 SILICONA 2 VIAS"),
                   kind="offer", raw_text="SONDA FOLEY CH16 SILICONA 2 VIAS",
                   extra={"buyer_text": RUBRIC, "awarded": True}),
    ]
    stats, reports = resolve_items(Resolver(cat), items, persist=False,
                                   collect_reports=True, joint=True)
    assert stats.by_status["resolved_generic"] == 1
    assert reports[0].evidence["awarded"] is True


# --- tender line + tender-title resolution (resolve(context_text=...)) --------

def test_tender_title_recovers_terse_item_line():
    # the item line names no family; the tender title classifies it.
    rep = r().resolve("producto segun bases tecnicas",
                      context_text="ADQUISICION SONDA FOLEY CH18 LATEX")
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas"
    assert rep.evidence["category_source"] == "tender"


def test_tender_title_fills_attribute_item_omits():
    # both classify to foley; the item omits the calibre the tender states.
    rep = r().resolve("SONDA FOLEY SILICONA",
                      context_text="SONDA FOLEY CH16 2 VIAS")
    assert rep.classification.category_id == "sondas"
    assert rep.extraction.values["material"] == "silicona"   # only the item had it
    assert rep.extraction.values["calibre"] == "16Fr"        # filled from the tender
    assert rep.evidence["category_source"] == "item"


def test_item_wins_and_no_conflict_against_tender_title():
    # a broad tender title naming a different family is context, not a conflict:
    # the item is authoritative, so it resolves to the item's category.
    rep = r().resolve("SONDA FOLEY CH16 SILICONA",
                      context_text="ADQUISICION INSUMOS JERINGAS Y SONDAS")
    assert rep.status == "resolved_generic"
    assert rep.classification.category_id == "sondas"


def test_runner_passes_tender_text_as_context():
    cat = InMemoryCatalog()
    items = [
        SourceItem(ref=SourceRef("mp", "T", "1", "producto segun anexo"),
                   kind="tender_item", raw_text="producto segun anexo",
                   extra={"tender_text": "ADQUISICION SONDA FOLEY CH18 LATEX"}),
    ]
    stats, reports = resolve_items(Resolver(cat), items, persist=False,
                                   collect_reports=True, joint=False)
    assert stats.by_status["resolved_generic"] == 1
    assert reports[0].classification.category_id == "sondas"
