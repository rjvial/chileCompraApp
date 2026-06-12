import json

import pytest

from chilecompra_er.categories.schema import add_category, load_register
from chilecompra_er.strawman import families


@pytest.fixture
def register_path(tmp_path):
    path = tmp_path / "register.json"
    path.write_text(json.dumps({
        "register_version": "0.2.0",
        "categories": [
            {"category_id": "jeringas", "name": "Jeringas", "status": "candidate",
             "include": ["\\bjering\\w*"], "exclude": [],
             "schema_file": "schemas/jeringas.json", "corpus_regex": "(?i).*jering.*"},
        ],
    }), encoding="utf-8")
    return path


def test_add_category_appends_and_bumps_version(register_path):
    entry = add_category("mascarillas", "Mascarillas",
                         include=["\\bmascarilla\\w*"], path=register_path)
    register = load_register(register_path)
    assert register["register_version"] == "0.3.0"
    ids = [c["category_id"] for c in register["categories"]]
    assert ids == ["jeringas", "mascarillas"]
    assert entry["schema_file"] == "schemas/mascarillas.json"
    assert entry["status"] == "candidate"


def test_default_corpus_regex_derived_from_include(register_path):
    entry = add_category("mascarillas", "Mascarillas",
                         include=["\\bmascarilla\\w*", "\\bbarbijo\\b"],
                         path=register_path)
    assert entry["corpus_regex"] == "(?i).*(mascarilla|barbijo).*"


def test_explicit_corpus_regex_wins(register_path):
    entry = add_category("oxigeno", "Oxígeno", include=["\\bnaricera\\w*"],
                         corpus_regex="(?i).*(naricera|oxigeno).*",
                         path=register_path)
    assert entry["corpus_regex"] == "(?i).*(naricera|oxigeno).*"


def test_duplicate_id_rejected(register_path):
    with pytest.raises(ValueError, match="already in register"):
        add_category("jeringas", "Jeringas", include=["x"], path=register_path)


def test_invalid_inputs_rejected(register_path):
    with pytest.raises(ValueError, match="snake_case"):
        add_category("Máscarillas!", "x", include=["x"], path=register_path)
    with pytest.raises(Exception):  # re.error
        add_category("mascarillas", "x", include=["(unclosed"], path=register_path)
    with pytest.raises(ValueError, match="at least one"):
        add_category("mascarillas", "x", include=[], path=register_path)


def test_families_picks_up_new_category(register_path):
    add_category("mascarillas", "Mascarillas", include=["\\bmascarilla\\w*"],
                 path=register_path)
    fams = families(load_register(register_path))
    assert {"category_id": "mascarillas", "name": "Mascarillas",
            "corpus_regex": "(?i).*(mascarilla).*"} in fams