"""
Group id_item values whose product_generico strings describe the same product.

Pipeline:
  1. Normalize each product_generico (lowercase, fold accents, strip "°",
     split digit/letter boundaries, drop Spanish stopwords).
  2. Extract a spec signature — split into "hard" specs (FR, dimensions,
     talla, channel counts, materials) that must match exactly, and "soft"
     specs (length in cm/mm) that must only match if BOTH rows specify
     them. Present-vs-absent on a soft spec stays compatible.
  3. Build a "content" string by removing spec tokens; fuzzy similarity scores
     run on this so shared sizes don't inflate matches.
  4. Block candidate pairs via an inverted token index; fall back to the
     rarest token when every token of a row is common.
  5. Score candidates with rapidfuzz.token_set_ratio.
  6. Agglomerate with complete-linkage — every pair inside a cluster must
     clear the threshold — to prevent transitive chaining.
  7. Force rows sharing the same id_item into the same cluster.
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process


INPUT_PATH = Path("data/productos_genericos.jsonl")
OUTPUT_PATH = Path("data/grupos_productos_genericos.jsonl")
DEFAULT_THRESHOLD = 85
DEFAULT_MAX_BLOCK_SIZE = 300


# --- Vocabulary --------------------------------------------------------------

STOPWORDS = frozenset({
    "a", "al", "con", "de", "del", "e", "el", "en", "la", "las", "los",
    "o", "para", "por", "que", "sin", "un", "una", "unas", "unos", "y",
})

# Spanish number words → digit form, so "dos vías" and "2 vías" share a sig.
# ("un"/"una" stay as stopwords; they're far more often articles than counts.)
NUMBER_WORDS = {
    "uno": "1", "dos": "2", "tres": "3", "cuatro": "4", "cinco": "5",
    "seis": "6", "siete": "7", "ocho": "8", "nueve": "9", "diez": "10",
    "once": "11", "doce": "12",
}

# Multi-char units: safe to strip standalone in content (won't collide with
# size letters like "M" or "L").
MULTI_UNITS = r"inches|inch|pulgadas|pulg|mm|cm|ml|cc|kg|mg|mcg|ug"
# Short units: only consumed when adjacent to a number; never stripped alone
# because "m", "l", "g" double as size letters / initials.
SHORT_UNITS = r"fr|in|%|m|l|g"
ALL_UNITS = f"{MULTI_UNITS}|{SHORT_UNITS}"

# Unit synonyms — rows written with either spelling share the same sig key.
# Unlisted units pass through unchanged.
UNIT_CANON = {
    "cc": "ml",                                                      # 1 cc = 1 ml
    "ug": "mcg",                                                     # microgram
    "inch": "in", "inches": "in", "pulg": "in", "pulgadas": "in",    # inch
}

# Soft spec units: length descriptors that products often omit. A row with
# "40 cm" and one without still merge; "40 cm" and "30 cm" do not. Canonical
# unit names (post-UNIT_CANON).
SOFT_UNITS = frozenset({"cm", "mm"})

# Count markers — only meaningful in "N vías" (number-before-mark) order.
# Kept OUT of ALL_UNITS so "2 vias 18 fr" doesn't also fire the unit-number
# pattern on "vias 18" (which would invent a spurious "18vias" spec and
# split "FR 18" from "18 FR" into different clusters).
COUNT_MARKS = r"vias|via"

SIZE_MARKS = r"numero|tamanio|tamano|talla|size|num|no|n"
SIZE_LETTERS = r"xxxl|xxl|xl|xs|s|m|l"

# Material tokens get folded into the signature so rows with different
# specified materials (silicona vs latex) — or one specified and one not —
# never merge. Synonyms collapse to a canonical form. Extend as needed.
MATERIAL_CANON = {
    "silicona": "silicona", "silicone": "silicona", "silicon": "silicona",
    "latex": "latex",
    "pvc": "pvc",
    "poliuretano": "poliuretano", "polyurethane": "poliuretano",
    "polietileno": "polietileno",
    "polipropileno": "polipropileno",
    "nitrilo": "nitrilo", "nitrile": "nitrilo",
    "caucho": "caucho", "goma": "caucho",
    "plastico": "plastico",
    "vidrio": "vidrio",
    "acero": "acero",
    "inoxidable": "inoxidable",
    "aluminio": "aluminio",
    "algodon": "algodon",
    "nylon": "nylon",
}

# Decimal: 12, 12.5, 12,5 (Spanish uses comma; canonicalized below).
_DEC = r"\d+(?:[.,]\d+)?"

# Measurement patterns. `(?![a-z])` at the end is stricter than `\b` — it also
# fires at end-of-string after "%" (where \b would fail) and rejects "mms".
_RE_NUM_UNIT   = re.compile(rf"({_DEC})\s+({ALL_UNITS})(?![a-z])")
_RE_UNIT_NUM   = re.compile(rf"\b({ALL_UNITS})\s+({_DEC})(?![a-z0-9])")
_RE_NUM_COUNT  = re.compile(rf"({_DEC})\s+({COUNT_MARKS})(?![a-z])")
_RE_SIZE_NUM   = re.compile(rf"\b({SIZE_MARKS})\s+({_DEC})(?![a-z0-9])")
_RE_SIZE_ALPHA = re.compile(rf"\b({SIZE_MARKS})\s+({SIZE_LETTERS})(?![a-z])")
# Dim with trailing unit ("10x10 cm") binds the unit to the dim so it stays
# hard — otherwise the cm would fall through to the soft-length pattern and
# "10x10 cm" would wrongly merge with "10x10 mm".
_RE_DIM_UNIT   = re.compile(rf"({_DEC})\s*x\s*({_DEC})(?:\s*x\s*({_DEC}))?\s+({ALL_UNITS})(?![a-z])")
_RE_DIMS       = re.compile(rf"({_DEC})\s*x\s*({_DEC})(?:\s*x\s*({_DEC}))?")
_RE_BARE_NUM   = re.compile(rf"\b{_DEC}\b")
_RE_MULTI_UNIT = re.compile(rf"\b({MULTI_UNITS}|{COUNT_MARKS})\b")


def _mask_with(rx: re.Pattern[str], text: str) -> str:
    return rx.sub(lambda m: " " * (m.end() - m.start()), text)


def _canon_num(s: str) -> str:
    return f"{float(s.replace(',', '.')):g}"


# --- Normalization & feature extraction --------------------------------------

def normalize(text: str) -> str:
    # NFKD decomposes accented letters and ordinal indicators (º → o, ª → a).
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().replace("°", " ")
    # "#N" is a size indicator (e.g. "máscara #3"); rewrite to "n N" so
    # _RE_SIZE_NUM captures it like any other "talla/numero N" form.
    text = re.sub(r"#\s*(?=\d)", "n ", text)
    # Split "12FR" → "12 fr" and "FR12" → "fr 12" so tokens separate cleanly.
    text = re.sub(r"(\d)([a-z])", r"\1 \2", text)
    text = re.sub(r"([a-z])(\d)", r"\1 \2", text)
    text = re.sub(r"\s+", " ", text).strip()
    return " ".join(
        NUMBER_WORDS.get(t, t)
        for t in text.split(" ")
        if t and t not in STOPWORDS
    )


# (hard, soft) — hard tokens must match exactly between two rows; soft
# tokens are stored as (attr, value) pairs and must only agree on attrs
# that both rows specify.
Sig = tuple[frozenset[str], frozenset[tuple[str, str]]]


def measurement_signature(normalized: str) -> Sig:
    """Canonical spec. Hard tokens must match exactly; soft tokens must only
    match on attributes specified by both rows."""
    hard: set[str] = set()
    soft: set[tuple[str, str]] = set()

    def _add_unit_spec(num: str, unit: str) -> None:
        canon = UNIT_CANON.get(unit, unit)
        tok = f"{_canon_num(num)}{canon}"
        (soft.add((canon, tok)) if canon in SOFT_UNITS else hard.add(tok))

    # Process patterns in order of specificity, masking each match so a
    # later pattern can't re-parse the same characters. Rationale for each
    # masking step is noted inline.
    #
    # "N vías" — prevents _RE_UNIT_NUM from matching "vias 18" in
    # "2 vias 18 fr" (which would invent a bogus "18vias" spec).
    for num, mark in _RE_NUM_COUNT.findall(normalized):
        hard.add(f"{_canon_num(num)}{mark}")
    masked = _mask_with(_RE_NUM_COUNT, normalized)

    # Dim-with-unit ("10x10 cm") — unit stays bound to the dim; prevents it
    # from falling through to the soft-length pattern.
    for num1, num2, num3, unit in _RE_DIM_UNIT.findall(masked):
        dim = "x".join(_canon_num(g) for g in (num1, num2, num3) if g)
        hard.add(f"{dim}{UNIT_CANON.get(unit, unit)}")
    masked = _mask_with(_RE_DIM_UNIT, masked)

    # Plain dim ("10x10").
    for num1, num2, num3 in _RE_DIMS.findall(masked):
        hard.add("x".join(_canon_num(g) for g in (num1, num2, num3) if g))
    masked = _mask_with(_RE_DIMS, masked)

    # Num-unit ("12 fr") — mask before unit-num so "12 fr 40 cm" doesn't
    # also parse as "fr 40" (bogus "40fr").
    for num, unit in _RE_NUM_UNIT.findall(masked):
        _add_unit_spec(num, unit)
    masked = _mask_with(_RE_NUM_UNIT, masked)

    # Unit-num ("FR 12") on what remains.
    for unit, num in _RE_UNIT_NUM.findall(masked):
        _add_unit_spec(num, unit)

    # Size marks ("talla M" / "número 5").
    for _mark, num in _RE_SIZE_NUM.findall(masked):
        hard.add(f"{_canon_num(num)}n")
    for _mark, letter in _RE_SIZE_ALPHA.findall(masked):
        hard.add(f"{letter}n")

    for tok in set(normalized.split()) & MATERIAL_CANON.keys():
        hard.add(f"mat:{MATERIAL_CANON[tok]}")
    return frozenset(hard), frozenset(soft)


def compatible_sigs(a: Sig, b: Sig) -> bool:
    if a[0] != b[0]:
        return False
    if a[1] == b[1]:
        return True
    sa = dict(a[1])
    sb = dict(b[1])
    return all(sa[k] == sb[k] for k in sa.keys() & sb.keys())


def content_tokens(normalized: str) -> str:
    """Normalized text with spec tokens removed, for similarity scoring."""
    t = normalized
    # Same ordering as measurement_signature — strip specific composites
    # (count marks, dim-with-unit, dim) before the generic unit patterns.
    for rx in (_RE_NUM_COUNT, _RE_DIM_UNIT, _RE_DIMS, _RE_NUM_UNIT, _RE_UNIT_NUM, _RE_SIZE_NUM, _RE_SIZE_ALPHA):
        t = rx.sub(" ", t)
    t = _RE_BARE_NUM.sub(" ", t)
    t = _RE_MULTI_UNIT.sub(" ", t)  # SHORT_UNITS kept: "m"/"l" may be size letters
    return re.sub(r"\s+", " ", t).strip()


# --- Clustering --------------------------------------------------------------

class UnionFind:
    __slots__ = ("parent",)

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        p = self.parent
        while p[x] != x:
            p[x] = p[p[x]]
            x = p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def build_postings(contents: list[str]) -> dict[str, list[int]]:
    postings: dict[str, list[int]] = {}
    for i, text in enumerate(contents):
        for tok in set(text.split(" ")):
            if tok:
                postings.setdefault(tok, []).append(i)
    return postings


def collect_edges(
    contents: list[str],
    sigs: list[Sig],
    postings: dict[str, list[int]],
    hot_tokens: set[str],
    threshold: int,
) -> tuple[list[tuple[float, int, int]], dict[tuple[int, int], float], int]:
    edges: list[tuple[float, int, int]] = []
    edge_score: dict[tuple[int, int], float] = {}
    scored = 0
    for i, text in enumerate(contents):
        all_toks = [t for t in text.split(" ") if t]
        tokens = {t for t in all_toks if t not in hot_tokens}
        if not tokens and all_toks:
            # Every token is hot (e.g. a row made entirely of common medical
            # terms — "sonda foley silicona vias"). Fall back to the rarest
            # token so identical rows can still find each other.
            tokens = {min(all_toks, key=lambda t: len(postings[t]))}
        cand: set[int] = set()
        for tok in tokens:
            cand.update(postings[tok])
        cand = {j for j in cand if j > i and compatible_sigs(sigs[i], sigs[j])}
        if not cand:
            continue
        cand_list = sorted(cand)
        cand_texts = [contents[j] for j in cand_list]
        row = process.cdist(
            [text], cand_texts,
            scorer=fuzz.token_set_ratio,
            score_cutoff=threshold,
        )[0]
        scored += len(cand_list)
        for j, score in zip(cand_list, row):
            if score >= threshold:
                s = float(score)
                edges.append((s, i, j))
                edge_score[(i, j)] = s
    return edges, edge_score, scored


def complete_linkage(
    n: int,
    edges: list[tuple[float, int, int]],
    edge_score: dict[tuple[int, int], float],
    threshold: int,
) -> tuple[UnionFind, int]:
    """Agglomerate so every pair inside a cluster has a scored edge >= threshold.

    This prevents chaining: if A--B and B--C both score high but A--C does not,
    A and C never end up in the same cluster.
    """
    uf = UnionFind(n)
    members: dict[int, list[int]] = {i: [i] for i in range(n)}
    rejected = 0
    edges.sort(key=lambda e: -e[0])
    for _score, i, j in edges:
        ri, rj = uf.find(i), uf.find(j)
        if ri == rj:
            continue
        mi, mj = members[ri], members[rj]
        ok = True
        for a in mi:
            for b in mj:
                key = (a, b) if a < b else (b, a)
                if edge_score.get(key, 0.0) < threshold:
                    ok = False
                    break
            if not ok:
                break
        if ok:
            uf.union(ri, rj)  # our UF impl sets parent[rj] = ri
            members[ri] = mi + mj
            del members[rj]
        else:
            rejected += 1
    return uf, rejected


def enforce_shared_id(uf: UnionFind, id_items: list[str]) -> int:
    id_to_rows: dict[str, list[int]] = {}
    for i, iid in enumerate(id_items):
        id_to_rows.setdefault(iid, []).append(i)
    merged = 0
    for rows in id_to_rows.values():
        if len(rows) > 1:
            anchor = rows[0]
            for r in rows[1:]:
                if uf.find(anchor) != uf.find(r):
                    uf.union(anchor, r)
                    merged += 1
    return merged


def cluster(df: pd.DataFrame, threshold: int, max_block_size: int) -> pd.DataFrame:
    contents = df["content"].tolist()
    sigs = df["sig"].tolist()
    n = len(contents)

    postings = build_postings(contents)
    hot_tokens = {t for t, ids in postings.items() if len(ids) > max_block_size}

    edges, edge_score, scored = collect_edges(
        contents, sigs, postings, hot_tokens, threshold
    )
    full = n * (n - 1) // 2 or 1
    print(
        f"Blocking: scored {scored} pairs (vs N(N-1)/2 = {full}, "
        f"ratio = {scored / full:.2%}); hot tokens: {len(hot_tokens)}; "
        f"edges >= {threshold}: {len(edges)}"
    )

    uf, rejected = complete_linkage(n, edges, edge_score, threshold)
    if rejected:
        print(f"Complete-linkage rejected {rejected} merges (would break complete-link)")

    merged = enforce_shared_id(uf, df["id_item"].astype(str).tolist())
    if merged:
        print(f"Merged {merged} cross-cluster rows via shared id_item")

    raw = [uf.find(i) for i in range(n)]
    remap: dict[int, int] = {}
    for cid in raw:
        if cid not in remap:
            remap[cid] = len(remap)
    out = df.copy()
    out["cluster_id"] = [remap[c] for c in raw]
    return out


# --- I/O ---------------------------------------------------------------------

def load_df(path: Path) -> pd.DataFrame:
    rows = [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    df = pd.DataFrame(rows)
    df["norm"] = df["product_generico"].astype(str).map(normalize)
    df["sig"] = df["norm"].map(measurement_signature)
    df["content"] = df["norm"].map(content_tokens)
    return df


def _pick_canonical(series: pd.Series) -> str:
    # Shortest distinct string, ties broken alphabetically — deterministic across runs.
    return min(set(series.astype(str)), key=lambda s: (len(s), s))


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby("cluster_id", sort=False)
        .agg(
            id_items=("id_item", lambda s: sorted({str(x) for x in s})),
            products=("product_generico", lambda s: sorted({str(x) for x in s})),
            canonical=("product_generico", _pick_canonical),
        )
        .reset_index()
    )
    grouped["size"] = grouped["id_items"].map(len)
    grouped = grouped[["cluster_id", "size", "id_items", "products", "canonical"]]
    return grouped.sort_values(["size", "cluster_id"], ascending=[False, True])


def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in df.to_dict(orient="records"):
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# --- Main --------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=INPUT_PATH)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    parser.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD,
                        help="rapidfuzz token_set_ratio cutoff (0-100)")
    parser.add_argument("--max-block-size", type=int, default=DEFAULT_MAX_BLOCK_SIZE,
                        help="Tokens in more than this many rows are excluded "
                             "from candidate generation (still used by the scorer).")
    parser.add_argument("--only-duplicates", action="store_true",
                        help="Write only clusters with > 1 id_item")
    args = parser.parse_args()

    df = load_df(args.input)
    print(f"Loaded {len(df)} rows from {args.input}")

    clustered = cluster(df, args.threshold, args.max_block_size)
    summary = summarize(clustered)

    dup = summary[summary["size"] > 1]
    print(
        f"Clusters: {len(summary)} total; "
        f"{len(dup)} with duplicates ({int(dup['size'].sum())} rows grouped)"
    )

    out = dup if args.only_duplicates else summary
    write_jsonl(out, args.output)
    print(f"Wrote {len(out)} clusters to {args.output}")

    print("\nTop 10 duplicate clusters:")
    for rec in dup.head(10).to_dict(orient="records"):
        print(f"  [{rec['size']}] {rec['canonical']}")
        for pid, prod in zip(rec["id_items"], rec["products"]):
            print(f"      - {pid}: {prod}")


if __name__ == "__main__":
    main()
