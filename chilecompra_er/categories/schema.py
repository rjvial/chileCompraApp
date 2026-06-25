"""Category schemas (design note §3).

A schema is a list of attribute definitions, each a 4-tuple: name, value
domain (closed set), role (identity | descriptive), canonicalization rules.
Schemas live as versioned JSON files under schemas/ and are frozen onto the
:Category node as its attribute_defs property. Every edit after freeze is a
migration.

Extraction rules operate on *normalized* text (see normalize/) and come in
two kinds:
  - "regex":   pattern with capture groups + a template ("{1}Fr") that builds
               the canonical value from the match.
  - "keyword": pattern whose presence maps to a fixed canonical value.
Rules are ordered; for one attribute the first rule that yields an in-domain
value wins. Out-of-domain yields are dropped and counted (illegal-value rate,
schema dry-run metric).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

CATEGORIES_DIR = Path(__file__).parent
SCHEMAS_DIR = CATEGORIES_DIR / "schemas"
REGISTER_PATH = CATEGORIES_DIR / "register.json"

IDENTITY = "identity"
DESCRIPTIVE = "descriptive"

# Negation guard for keyword rules: a material/feature term mentioned in a
# negated context ("libre de latex", "sin balon", "no esteril") must NOT fire —
# otherwise a *latex-free* product is wrongly stamped material=latex (measured:
# ~1,925 such records, an identity error). We look only at the 2 word-tokens
# immediately before the match: tight enough to skip "sin aguja con latex"
# (latex is NOT negated there) while catching the dominant "libre de X" / "sin X"
# adjacency. Lists ("libre de ftalatos y latex") past the window are accepted
# loss for now.
_NEGATORS = frozenset({
    "sin", "no", "libre", "libres", "exento", "exenta", "exentos", "exentas",
})
_WORD = re.compile(r"\w+")


def _is_negated(text: str, start: int) -> bool:
    """True if the keyword match at `start` is preceded by a negator within the
    last two word-tokens."""
    return any(t.lower() in _NEGATORS for t in _WORD.findall(text[:start])[-2:])


@dataclass(frozen=True)
class Rule:
    kind: str  # "regex" | "keyword"
    pattern: re.Pattern
    template: str | None = None  # regex kind
    value: str | None = None     # keyword kind
    # Optional sentence-level context guard. The rule fires only if `requires`
    # also matches somewhere in the text — the fix for "anchorless" numeric rules
    # that would otherwise stamp an identity value from a bare number (e.g. the
    # "2,5" in "Ca 2,5 mEq" or "cable 3x2,5 mm" wrongly read as dextrose 2.5%).
    # The guard names the attribute's concept (dextrosa|glucosa, calibre, talla…),
    # so a number with no such concept word present can no longer fire. A guard
    # can only *reduce* matches, never widen them — so it never creates a false
    # merge, at worst it trades a little recall for precision.
    requires: re.Pattern | None = None

    def apply(self, text: str) -> tuple[str, re.Match] | None:
        # Context guard: if present and absent from the text, the rule is inert.
        if self.requires is not None and not self.requires.search(text):
            return None
        if self.kind == "keyword":
            # Return the first NON-negated occurrence (a negated mention like
            # "libre de latex" must not fire; a later plain mention still can).
            for m in self.pattern.finditer(text):
                if not _is_negated(text, m.start()):
                    return self.value, m
            return None
        m = self.pattern.search(text)
        if not m:
            return None
        out = self.template or ""
        for i, group in enumerate(m.groups(), start=1):
            out = out.replace("{%d}" % i, group or "")
        return out, m


@dataclass(frozen=True)
class AttributeDef:
    name: str
    role: str
    domain: tuple[str, ...]
    rules: tuple[Rule, ...]
    unit: str | None = None
    curation_notes: str = ""

    @property
    def is_identity(self) -> bool:
        return self.role == IDENTITY


@dataclass(frozen=True)
class CategorySchema:
    category_id: str
    name: str
    schema_version: str
    status: str  # draft | launched
    base_unit: str  # per-category base pricing unit (design §6)
    attribute_defs: tuple[AttributeDef, ...]
    raw_json: str  # original file content, frozen onto :Category.attribute_defs

    @property
    def identity_names(self) -> tuple[str, ...]:
        return tuple(a.name for a in self.attribute_defs if a.is_identity)

    def attr(self, name: str) -> AttributeDef:
        for a in self.attribute_defs:
            if a.name == name:
                return a
        raise KeyError(f"no attribute '{name}' in schema {self.category_id}")

    def validate_values(self, values: dict[str, str]) -> None:
        """Write-time validator (design §9 #6): every value must be in-domain.

        Neo4j has no CHECK constraints, so the write path is the only gate —
        raise on any violation rather than persisting garbage.
        """
        for name, value in values.items():
            attr = self.attr(name)  # KeyError if attr not in schema
            if value not in attr.domain:
                raise ValueError(
                    f"illegal value {name}={value!r} for {self.category_id} "
                    f"(domain: {list(attr.domain)})"
                )


def _parse_rule(d: dict) -> Rule:
    return Rule(
        kind=d["kind"],
        pattern=re.compile(d["pattern"]),
        template=d.get("template"),
        value=d.get("value"),
        requires=re.compile(d["requires"]) if d.get("requires") else None,
    )


# --- anchorless-numeric lint (Phase 0 coherence guard) ------------------------
# An identity rule is "anchorless" when its pattern, stripped of pure regex
# syntax, retains no letter and no '%' — i.e. it can fire on a bare number with
# nothing tying it to the attribute's concept. That is the exact failure that
# collapsed cable gauge and calcium onto a "dextrose 2.5%" product. Such a rule
# MUST carry a `requires` guard; the lint flags the ones that don't.

# Unit symbols that anchor a number to a concept (degrees, percent, micro, ohm),
# so evidence like "360°" or "5µm" counts as anchored, not bare.
_UNIT_SYMBOLS = "%°ºµμΩ"


def _is_anchorless_pattern(pattern: str) -> bool:
    if any(u in pattern for u in _UNIT_SYMBOLS):
        return False
    s = pattern
    for tok in (r"\b", r"\s", r"\d", r"\w", r"\.", r"\-"):
        s = s.replace(tok, " ")
    s = re.sub(r"[*+?(){}\[\]|^$0-9.,\\\- ]", " ", s)
    # any Unicode letter (incl. accented Spanish) anchors it; bare numbers don't
    return re.search(r"[^\W\d_]", s) is None


def anchorless_identity_rules(schema: CategorySchema) -> list[tuple[str, str]]:
    """(attribute_name, pattern) for every identity rule that can fire on a bare
    number and carries no `requires` guard. Empty list == clean."""
    out: list[tuple[str, str]] = []
    for a in schema.attribute_defs:
        if not a.is_identity:
            continue
        for r in a.rules:
            if r.requires is None and _is_anchorless_pattern(r.pattern.pattern):
                out.append((a.name, r.pattern.pattern))
    return out


def load_schema(path: Path | str) -> CategorySchema:
    path = Path(path)
    raw = path.read_text(encoding="utf-8")
    doc = json.loads(raw)
    defs = tuple(
        AttributeDef(
            name=a["name"],
            role=a["role"],
            domain=tuple(a["domain"]),
            rules=tuple(_parse_rule(r) for r in a.get("rules", [])),
            unit=a.get("unit"),
            curation_notes=a.get("curation_notes", ""),
        )
        for a in doc["attribute_defs"]
    )
    for a in defs:
        if a.role not in (IDENTITY, DESCRIPTIVE):
            raise ValueError(f"{path}: attribute {a.name} has invalid role {a.role!r}")
    return CategorySchema(
        category_id=doc["category_id"],
        name=doc["name"],
        schema_version=doc["schema_version"],
        status=doc.get("status", "draft"),
        base_unit=doc["base_unit"],
        attribute_defs=defs,
        raw_json=raw,
    )


def load_register(path: Path | str = REGISTER_PATH) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _bump_minor(version: str) -> str:
    major, minor, _ = (version.split(".") + ["0", "0"])[:3]
    return f"{major}.{int(minor) + 1}.0"


def _literal_tokens(patterns: list[str]) -> list[str]:
    """Best-effort literal stems from include regexes ("\\bmascarilla\\w*" ->
    "mascarilla"), used to build a default corpus-sampling regex."""
    tokens: list[str] = []
    for pattern in patterns:
        for part in pattern.split("|"):
            part = part.replace("\\b", "").replace("\\w*", "").replace("\\w+", "")
            part = re.sub(r"[\\^$()\[\]?*+|.{}]", "", part).strip()
            if part:
                tokens.append(part)
    return tokens


def add_category(category_id: str, name: str, include: list[str],
                 exclude: list[str] | None = None, corpus_regex: str | None = None,
                 canonical_example: str | None = None,
                 status: str = "candidate", path: Path | str = REGISTER_PATH) -> dict:
    """Append a category to the register (design §3 step 7: every boundary
    change is a new register version). Validates regexes; the schema file is
    expected to be drafted afterwards via generate-schemas."""
    exclude = exclude or []
    if not re.fullmatch(r"[a-z0-9_]+", category_id):
        raise ValueError("category_id must be lowercase snake_case ASCII")
    if not include:
        raise ValueError("at least one --include pattern is required")
    for pattern in [*include, *exclude, *([corpus_regex] if corpus_regex else [])]:
        re.compile(pattern)  # raises re.error on invalid input

    register = load_register(path)
    if any(c["category_id"] == category_id for c in register["categories"]):
        raise ValueError(f"category '{category_id}' already in register")

    if corpus_regex is None:
        tokens = _literal_tokens(include)
        if not tokens:
            raise ValueError("could not derive corpus regex from include patterns; "
                             "pass --corpus explicitly")
        corpus_regex = "(?i).*(" + "|".join(tokens) + ").*"

    entry = {
        "category_id": category_id,
        "name": name,
        "status": status,
        "include": include,
        "exclude": exclude,
        "spend_share": None,
        "schema_file": f"schemas/{category_id}.json",
        "corpus_regex": corpus_regex,
    }
    if canonical_example:
        entry["canonical_example"] = canonical_example
    register["categories"].append(entry)
    register["register_version"] = _bump_minor(register["register_version"])
    Path(path).write_text(json.dumps(register, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")
    return entry


def schema_for(category_id: str, register: dict | None = None) -> CategorySchema:
    register = register or load_register()
    for cat in register["categories"]:
        if cat["category_id"] == category_id:
            return load_schema(CATEGORIES_DIR / cat["schema_file"])
    raise KeyError(f"category '{category_id}' not in register")
