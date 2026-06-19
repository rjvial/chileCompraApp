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


def train_pipeline(texts: list[str], labels: list[str]):
    """Fit a TF-IDF + logistic-regression pipeline on NORMALIZED texts -> labels.
    word 1-2 grams + char 3-5 grams (char grams catch terse/typo'd lines and
    shared morphology); balanced classes so rare families aren't drowned out.

    The vectorizers are CAPPED (max_features): uncapped, char 3-5 grams over a
    large corpus (300k+ rows) explode to millions of features, and the per-class
    coefficient matrix then blows past available RAM (one untuned run hit ~9 GB
    and swapped for an hour). The caps bound memory and training time and reduce
    overfit, with negligible accuracy cost since dropped grams are rare. min_df=2
    stays (small training sets must keep a non-empty vocabulary). saga handles the
    large sparse multi-class fit and honours max_iter as a real time bound."""
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import FeatureUnion, Pipeline

    features = FeatureUnion([
        ("word", TfidfVectorizer(ngram_range=(1, 2), min_df=2,
                                 max_features=50_000, sublinear_tf=True)),
        ("char", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5),
                                 min_df=2, max_features=100_000, sublinear_tf=True)),
    ])
    pipe = Pipeline([
        ("features", features),
        ("clf", LogisticRegression(max_iter=1000, C=10.0, class_weight="balanced",
                                   solver="saga")),
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

    def classify(self, normalized_text: str) -> Classification:
        if not normalized_text or not normalized_text.strip():
            return Classification(None, UNCLASSIFIED, tier="tier2")
        proba = self.pipeline.predict_proba([normalized_text])[0]
        i = int(proba.argmax())
        p = float(proba[i])
        if p >= self.threshold:
            return Classification(self.pipeline.classes_[i], CLASSIFIED,
                                  matched=(f"p={p:.2f}",), tier="tier2")
        return Classification(None, UNCLASSIFIED, tier="tier2")

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


def fetch_training_rows(conn) -> list[tuple[str, str]]:
    """(raw_text, category_id) for every item resolved to a CURATED family
    (not a UNSPSC fallback bucket) — the Tier-1 labels to learn from."""
    rows = conn.query(
        "MATCH (s:SourceRecord)-[:RESOLVED_TO]->(g:GenericProduct) "
        "WHERE NOT g.category_id STARTS WITH $p AND s.raw_text IS NOT NULL "
        "RETURN s.raw_text AS text, g.category_id AS label",
        parameters={"p": UNSPSC_PREFIX})
    return [(r["text"], r["label"]) for r in rows]
