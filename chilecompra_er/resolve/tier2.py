"""Tier-2 statistical classifier (improvement loop, step 3b; design §8 Tier 2).

Tier-1 is deterministic include/exclude regex — precise, but it only fires when
the family word is literally present. Tier-2 is a TF-IDF + logistic-regression
model trained on the labels Tier-1 produces: it generalizes to wording the regex
misses (synonyms, word order, partial phrasings) and assigns a category with a
probability. Below a confidence threshold it abstains (UNCLASSIFIED), so the item
still falls through to brand lookup / UNSPSC fallback — Tier-2 only ever *adds*
coverage, never overrides a confident upstream tier.

Training labels come from a resolved graph (items whose RESOLVED_TO target is a
curated, non-fallback GenericProduct). The model is a sklearn Pipeline persisted
with joblib. classify() takes NORMALIZED text, like every other tier.
"""

from __future__ import annotations

from pathlib import Path

from .classifier import CLASSIFIED, UNCLASSIFIED, Classification
from .resolver import UNSPSC_PREFIX

DEFAULT_THRESHOLD = 0.60
TIER2_MODEL_PATH = Path("data/tier2_model.joblib")


def unspsc_tokens(code) -> str:
    """The buyer's UNSPSC commodity code -> hierarchical tokens (segment / family /
    class) to append to the text, so the linear model can use the code as a coarse
    category prior. Empty when the code is missing or unparseable — the model then
    just sees the text (a missing feature is fine for a linear model)."""
    if code is None or code == "":
        return ""
    try:
        s = str(int(code)).zfill(8)
    except (TypeError, ValueError):
        return ""
    return f" unspscseg{s[:2]} unspscfam{s[:4]} unspsccls{s[:6]}"


def augment(text: str, code=None) -> str:
    """Text the Tier-2 vectorizer actually sees: the normalized description plus the
    UNSPSC tokens. Used identically at train and eval/inference time so the feature
    space matches. `code=None` => plain text (backward-compatible)."""
    return (text or "") + unspsc_tokens(code)


def train_pipeline(texts: list[str], labels: list[str]):
    """Fit a TF-IDF + logistic-regression pipeline on NORMALIZED texts -> labels.
    word 1-2 grams + char 3-5 grams (char grams catch terse/typo'd lines and
    shared morphology); balanced classes so rare families aren't drowned out.

    The vectorizers are CAPPED (max_features): uncapped, char 3-5 grams over a
    large corpus (300k+ rows) explode to millions of features, and the per-class
    coefficient matrix then blows past available RAM (one untuned run hit ~9 GB
    and swapped for an hour). The caps bound memory and per-iteration cost and
    reduce overfit, with negligible accuracy cost since dropped grams are rare.
    min_df=2 stays (small training sets must keep a non-empty vocabulary).

    The fit is HARD-bounded for time, not just memory: saga with max_iter=200 and
    a loose tol stops in known time even if not fully converged — fine for a
    fallback classifier that only fires above a confidence threshold. verbose=1
    logs per-epoch convergence so a long fit is observable, not a black box."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import FeatureUnion, Pipeline

    features = FeatureUnion([
        ("word", TfidfVectorizer(ngram_range=(1, 2), min_df=2,
                                 max_features=30_000, sublinear_tf=True)),
        ("char", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5),
                                 min_df=2, max_features=50_000, sublinear_tf=True)),
    ])
    pipe = Pipeline([
        ("features", features),
        ("clf", LogisticRegression(max_iter=200, C=10.0, class_weight="balanced",
                                   solver="lbfgs", tol=1e-3, verbose=1)),
    ])
    pipe.fit(texts, labels)
    return pipe


class Tier2Classifier:
    """Tier interface twin of Tier1Classifier: .classify(normalized) -> Classification."""

    def __init__(self, pipeline, threshold: float = DEFAULT_THRESHOLD,
                 register_version: str = "tier2"):
        self.pipeline = pipeline
        self.threshold = threshold
        self.register_version = register_version
        # Batch-prediction cache (see prime()). predict_proba has heavy per-call
        # overhead — a per-item classify() is ~150x slower than predicting a whole
        # chunk at once. The runner primes this cache per chunk so classify() is a
        # dict lookup.
        self._cache: dict[str, Classification] = {}

    def _verdict(self, proba) -> Classification:
        i = int(proba.argmax())
        p = float(proba[i])
        if p >= self.threshold:
            return Classification(self.pipeline.classes_[i], CLASSIFIED,
                                  matched=(f"p={p:.2f}",), tier="tier2")
        return Classification(None, UNCLASSIFIED, tier="tier2")

    def prime(self, normalized_texts) -> None:
        """Predict a whole chunk of texts in ONE predict_proba call and cache the
        verdicts, so the following per-item classify() calls are dict lookups.
        Replaces the cache each call, so it stays chunk-sized."""
        uniq = {t for t in normalized_texts if t and t.strip()}
        self._cache = {}
        if uniq:
            keys = list(uniq)
            for t, proba in zip(keys, self.pipeline.predict_proba(keys)):
                self._cache[t] = self._verdict(proba)

    def classify(self, normalized_text: str) -> Classification:
        if not normalized_text or not normalized_text.strip():
            return Classification(None, UNCLASSIFIED, tier="tier2")
        hit = self._cache.get(normalized_text)
        if hit is not None:
            return hit
        return self._verdict(self.pipeline.predict_proba([normalized_text])[0])

    def save(self, path: Path = TIER2_MODEL_PATH) -> None:
        import joblib

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"pipeline": self.pipeline, "threshold": self.threshold}, path)

    @classmethod
    def load(cls, path: Path = TIER2_MODEL_PATH,
             threshold: float | None = None) -> "Tier2Classifier":
        import joblib

        d = joblib.load(path)
        return cls(d["pipeline"],
                   threshold=threshold if threshold is not None else d["threshold"])


def train(texts: list[str], labels: list[str],
          threshold: float = DEFAULT_THRESHOLD) -> Tier2Classifier:
    return Tier2Classifier(train_pipeline(texts, labels), threshold=threshold)


def fetch_training_rows(conn) -> list[tuple[str, str, object]]:
    """(buyer_text, category_id, unspsc) for every item resolved to a CURATED family
    (not a UNSPSC fallback bucket) — the Tier-1 labels to learn from. The UNSPSC
    commodity code rides the ItemLicitacion directly, used as a feature."""
    rows = conn.query(
        "MATCH (i:ItemLicitacion)-[:RESOLVED_TO]->(g:GenericProduct) "
        "WHERE NOT g.category_id STARTS WITH $p AND i.descripcion_comprador IS NOT NULL "
        "RETURN i.descripcion_comprador AS text, g.category_id AS label, "
        "       i.codigo_unspsc_producto AS unspsc",
        parameters={"p": UNSPSC_PREFIX})
    return [(r["text"], r["label"], r["unspsc"]) for r in rows]
