import pytest

from chilecompra_er.categories import schema_for
from chilecompra_er.resolve import (
    InMemoryCatalog,
    identity_key,
    plan_assignment,
    subsumes,
)

schema = schema_for("sondas_foley")


def assign(catalog: InMemoryCatalog, values: dict):
    plan = plan_assignment(schema, values, catalog.load("sondas_foley", schema))
    catalog.apply(plan, schema)
    return plan


def test_identity_key_distinguishes_absence_from_value():
    assert identity_key("c", {"calibre": "16Fr"}) != \
        identity_key("c", {"calibre": "16Fr", "material": "latex"})


def test_subsumption_strict():
    assert subsumes({"calibre": "16Fr"}, {"calibre": "16Fr", "material": "latex"})
    assert not subsumes({"calibre": "16Fr"}, {"calibre": "16Fr"})  # equal: not strict subset
    assert not subsumes({"calibre": "18Fr"}, {"calibre": "16Fr", "material": "latex"})


def test_same_identity_values_resolve_to_same_node():
    cat = InMemoryCatalog()
    p1 = assign(cat, {"calibre": "16Fr", "material": "silicona", "vias": "2_vias"})
    p2 = assign(cat, {"calibre": "16Fr", "material": "silicona", "vias": "2_vias"})
    assert p1.created is not None and p2.created is None
    assert p1.home_id == p2.home_id


def test_parent_assigned_on_insert():
    cat = InMemoryCatalog()
    partial = assign(cat, {"calibre": "16Fr"})
    child = assign(cat, {"calibre": "16Fr", "material": "silicona"})
    assert child.parent_id == partial.home_id


def test_inserting_partial_repoints_existing_children():
    cat = InMemoryCatalog()
    g1 = assign(cat, {"calibre": "16Fr", "material": "silicona", "vias": "2_vias"})
    mid = assign(cat, {"calibre": "16Fr", "material": "silicona"})
    assert (g1.home_id, mid.home_id) in mid.repoint
    root = assign(cat, {"calibre": "16Fr"})
    # G1's best parent is still the mid node, not the new coarser root
    assert cat.nodes[g1.home_id].parent_id == mid.home_id
    assert cat.nodes[mid.home_id].parent_id == root.home_id


def test_descriptive_attributes_never_enter_identity():
    cat = InMemoryCatalog()
    a = assign(cat, {"calibre": "16Fr", "color_envase": "azul"})
    b = assign(cat, {"calibre": "16Fr", "color_envase": "verde"})
    assert a.home_id == b.home_id  # two catheters differing only here are the same product


def test_is_complete_requires_all_identity_attrs():
    cat = InMemoryCatalog()
    full = assign(cat, {"calibre": "16Fr", "material": "silicona",
                        "vias": "2_vias", "capacidad_balon": "30cc"})
    part = assign(cat, {"calibre": "16Fr", "material": "silicona", "vias": "2_vias"})
    assert full.created.is_complete is True
    assert part.created.is_complete is False


def test_write_path_rejects_illegal_values():
    with pytest.raises(ValueError):
        plan_assignment(schema, {"calibre": "99Fr"}, {})
    with pytest.raises(KeyError):
        plan_assignment(schema, {"not_an_attr": "x"}, {})


def test_versioned_resolutions_never_overwrite():
    from chilecompra_er.resolve import SourceRef

    cat = InMemoryCatalog()
    src = SourceRef("mp", "T-1", "1", "SONDA FOLEY 16")
    cat.persist_resolution(src, "resolved_generic", "gp_x")
    cat.persist_resolution(src, "resolved_generic", "gp_y")  # promotion = new version
    versions = [r for r in cat.resolutions if r["record_key"] == src.record_key]
    assert [r["version"] for r in versions] == [1, 2]
    assert [r["current"] for r in versions] == [False, True]
