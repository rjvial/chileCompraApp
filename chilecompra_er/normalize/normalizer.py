"""Shared normalization — the single function every downstream stage consumes.

Per the design note (§3, "How to build normalization & mining"): lowercase,
unicodedata accent stripping, and an owned, *versioned* abbreviation/synonym
table applied as ordered regex rules. The table is data, not code
(abbreviations_v1.csv); it grows empirically from corpus frequency work
during M0/M1. Patterns in the table are written against lowercased,
accent-stripped text.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

DEFAULT_TABLE = Path(__file__).with_name("abbreviations_v1.csv")

# Insert a space at digit<->letter boundaries so "ch16" -> "ch 16",
# "10ml" -> "10 ml", "100%" -> "100 %". Decimal points survive (handled
# before generic dot stripping).
_DIGIT_LETTER = re.compile(r"(?<=\d)(?=[a-z%])")
_LETTER_DIGIT = re.compile(r"(?<=[a-z])(?=\d)")
_DECIMAL_COMMA = re.compile(r"(?<=\d),(?=\d)")  # "7,5 cm" -> "7.5 cm" (Chilean decimals)
_DOT_NOT_DECIMAL = re.compile(r"(?<!\d)\.|\.(?!\d)")
_PUNCT = re.compile(r"[\"'()\[\]{},;:_\-+*/\\|·•¿?¡!#&<>=~]+")
_WS = re.compile(r"\s+")


def strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


class Normalizer:
    """Callable: raw description -> normalized text. Exposes .version for provenance."""

    def __init__(self, table_path: Path | str = DEFAULT_TABLE):
        self.table_path = Path(table_path)
        self.version = self.table_path.stem  # e.g. "abbreviations_v1"
        self.rules: list[tuple[re.Pattern, str]] = []
        with open(self.table_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pattern = (row.get("pattern") or "").strip()
                if not pattern or pattern.startswith("#"):
                    continue
                self.rules.append((re.compile(pattern), row.get("replacement") or ""))

    def __call__(self, text: str) -> str:
        t = strip_accents(text.lower())
        t = t.replace("°", " ").replace("º", " ").replace("ª", " ")
        for pattern, repl in self.rules:  # table rules run before punctuation
            t = pattern.sub(repl, t)      # is stripped, so "c/" style forms work
        t = _DECIMAL_COMMA.sub(".", t)
        t = _DOT_NOT_DECIMAL.sub(" ", t)
        t = _PUNCT.sub(" ", t)
        t = _DIGIT_LETTER.sub(" ", t)
        t = _LETTER_DIGIT.sub(" ", t)
        return _WS.sub(" ", t).strip()
