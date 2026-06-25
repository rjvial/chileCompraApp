"""L3 adjudication — Claude resolves the L2 matcher's residue (design: L0->L3).

The deterministic L2 matcher routes a small set of genuinely ambiguous cases to
L3: a shared model_token whose specs conflict, and a coarse partial spec that is
compatible with several divergent children. L3 asks a stronger model (Sonnet/Opus)
the substitutable-product question for each, and persists the verdict keyed by the
case (so re-runs are stable and no LLM work is re-paid).

SCAFFOLD: the pure question-building + the verdict store + the dry-run are done
and tested; the live call uses llm.complete_json_batch (Sonnet/Opus → API
credits), so a real run is gated on the same credits decision as the L1 bulk run.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from . import profile as P
from .matcher import MatchResult

DEFAULT_MODEL = "claude-sonnet-4-6"


# --- verdict store (JSONL keyed by case key) ----------------------------------

class VerdictStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self._cache: dict[str, dict] = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    rec = json.loads(line)
                    self._cache[rec["key"]] = rec["verdict"]

    def has(self, key: str) -> bool:
        return key in self._cache

    def get(self, key: str) -> dict | None:
        return self._cache.get(key)

    def put_many(self, items: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            for key, verdict in items.items():
                self._cache[key] = verdict
                f.write(json.dumps({"key": key, "verdict": verdict},
                                   ensure_ascii=False) + "\n")

    def __len__(self) -> int:
        return len(self._cache)


# --- structured-output verdict + system prompt --------------------------------

ADJUDICATION_SCHEMA: dict = {
    "type": "object",
    "additionalProperties": False,
    "required": ["decision", "target", "confidence", "rationale"],
    "properties": {
        "decision": {"type": "string", "enum": ["same", "different", "anchor"]},
        # for an ambiguous-partial case: the chosen finer signature, else null
        "target": {"anyOf": [{"type": "string"}, {"type": "null"}]},
        "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
        "rationale": {"type": "string"},
    },
}

SYSTEM_PROMPT = """\
You adjudicate hard product-matching cases from Chile's public-procurement
marketplace (medical/dental/lab supplies). Decide whether items are the SAME
SUBSTITUTABLE PRODUCT — independent of brand and packaging (a 5000 ml unit sold
singly or in a box of 2 is the same product; two brands of the same spec are the
same product). A genuine spec difference (16 Fr vs 18 Fr, 2.5% vs 4.25%) is a
DIFFERENT product.

You are given each item as its canonical signature plus brand / model / attribute
evidence. Respond with `decision`:
  - "same"      — the items are the same substitutable product.
  - "different" — a real spec difference; keep them separate.
  - "anchor"    — (ambiguous-partial cases only) the coarse spec is genuinely
                  ambiguous between the candidates; do not merge it into any one.
For an ambiguous-partial case where one candidate clearly fits, set decision
"same" and `target` to that candidate's signature; otherwise `target` null.
"""


# --- pure question building ---------------------------------------------------

@dataclass(frozen=True)
class AdjQuestion:
    key: str
    kind: str                 # model_token_conflict | ambiguous_partial
    signatures: tuple[str, ...]
    prompt: str


def _render(sig: str, prof: P.Profile | None) -> str:
    if prof is None:
        return f"[{sig}]"
    attrs = ", ".join(f"{a.name}={a.value} (\"{a.evidence}\")"
                      for a in prof.identity_attributes) or "(no attributes)"
    return (f"[{sig}] brand={prof.brand} model={prof.model_token}\n"
            f"      attrs: {attrs}")


def build_questions(result: MatchResult,
                    sig_to_profile: dict[str, P.Profile]) -> list[AdjQuestion]:
    """Turn the matcher residue into adjudication questions (pure)."""
    qs: list[AdjQuestion] = []
    for r in result.residue:
        if r["type"] == "model_token_conflict":
            sigs = tuple(sorted(r["signatures"]))
            key = "mtc|" + r.get("model_token", "") + "|" + "||".join(sigs)
            body = "\n".join(_render(s, sig_to_profile.get(s)) for s in sigs)
            prompt = (f"These share the model token {r.get('model_token')!r} but "
                      f"their specs differ. Same product?\n{body}")
            qs.append(AdjQuestion(key, r["type"], sigs, prompt))
        elif r["type"] == "ambiguous_partial":
            coarse = r["signature"]
            kids = tuple(r["children"])
            key = "amb|" + coarse + "|" + "||".join(sorted(kids))
            body = "COARSE:\n" + _render(coarse, sig_to_profile.get(coarse)) + \
                "\nCANDIDATES:\n" + "\n".join(
                    _render(k, sig_to_profile.get(k)) for k in kids)
            prompt = ("A coarse spec is compatible with several more-specific "
                      f"products. Which does it refer to, or is it ambiguous?\n{body}")
            qs.append(AdjQuestion(key, r["type"], (coarse, *kids), prompt))
    return qs


def signature_profiles(items: list[tuple[str, P.Profile]]) -> dict[str, P.Profile]:
    """Representative profile per signature (first wins) — the L3 case payload."""
    out: dict[str, P.Profile] = {}
    for _h, p in items:
        out.setdefault(p.signature(), p)
    return out


# --- driver -------------------------------------------------------------------

@dataclass
class AdjStats:
    questions: int = 0
    cached: int = 0
    adjudicated: int = 0
    failed: int = 0


def adjudicate(questions: list[AdjQuestion], store: VerdictStore, *,
               model: str = DEFAULT_MODEL, dry_run: bool = False,
               log=lambda _m: None) -> AdjStats:
    stats = AdjStats(questions=len(questions))
    todo = [q for q in questions if not store.has(q.key)]
    stats.cached = len(questions) - len(todo)
    log(f"L3: {len(questions)} cases; {stats.cached} cached, {len(todo)} to adjudicate")
    if dry_run:
        log("dry run — no LLM calls (no API credits spent)")
        return stats

    from ..llm import complete_json_many
    requests = [(q.key, q.prompt) for q in todo]
    results = complete_json_many(requests, ADJUDICATION_SCHEMA, SYSTEM_PROMPT,
                                 model=model, log=log)
    to_store: dict[str, dict] = {}
    for q in todo:
        v = results.get(q.key)
        if v is None:
            stats.failed += 1
            continue
        to_store[q.key] = {**v, "kind": q.kind, "signatures": list(q.signatures)}
        stats.adjudicated += 1
    store.put_many(to_store)
    log(f"L3 done: {stats.adjudicated} adjudicated, {stats.failed} failed "
        f"(store now {len(store)})")
    return stats
