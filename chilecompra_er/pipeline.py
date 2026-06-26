"""End-to-end redesign-pipeline orchestration with step-level resume.

`chilecompra-er pipeline [--resume]` runs the whole build as an ordered
sequence of steps and records which steps have completed in
`data/pipeline.checkpoint.json`. If any step is interrupted, re-running with
`--resume` skips the completed steps and continues at the interrupted one.

Two layers of resume:
  * STEP level (this module): the checkpoint's `done` list says which whole
    steps finished; `--resume` re-runs only what's left.
  * WITHIN a step: each long stage already owns its resume state — the profile
    store (text-hash cache, skip-done), the match `:OFFERS` stream-offset checkpoint,
    the adjudicate verdict store, the `register` vet checkpoint. So re-entering an
    unfinished step continues from the exact item, never the top.

This module is pure orchestration STATE — no graph/LLM/CLI imports — so the
ordering + checkpoint logic is unit-testable without a live graph (the actual
step execution lives in cli.cmd_pipeline, which drives the existing cmd_*
handlers). Mirrors the split in ingest/resume.py (data here, I/O in the caller).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

# Ordered pipeline steps. Linear dependency chain: each needs the prior one's
# output (in the graph, or on disk). The redesign build, end to end:
#   instance  — boot the Neo4j EC2 box
#   migrate   — graph constraints + indexes (incl. 005 clusters, 006 products)
#   register  — VOCABULARY: profile + vet families, register them + draft schemas.
#               Incremental: built only when absent / to fill gaps (see cmd_pipeline).
#   canonicalize — descriptions → profiles (Haiku; text-hash cache)
#   match     — cluster profiles → :ProductCluster/:Product, bind :OFFERS
#   adjudicate — Claude settles the matcher residue (non-fatal)
#   coherence-check — structural gate + semantic/health backlogs
STEP_INSTANCE = "instance"
STEP_MIGRATE = "migrate"
STEP_REGISTER = "register"
STEP_CANONICALIZE = "canonicalize"
STEP_MATCH = "match"
STEP_ADJUDICATE = "adjudicate"
STEP_COHERENCE = "coherence-check"

PIPELINE_STEPS = [
    STEP_INSTANCE,
    STEP_MIGRATE,
    STEP_REGISTER,
    STEP_CANONICALIZE,
    STEP_MATCH,
    STEP_ADJUDICATE,
    STEP_COHERENCE,
]


def pipeline_checkpoint_path(data_dir: Path) -> Path:
    return data_dir / "pipeline.checkpoint.json"


@dataclass
class PipelineCheckpoint:
    segment: int | None
    limit: int | None
    done: list[str] = field(default_factory=list)

    def to_json(self) -> dict:
        return {"segment": self.segment, "limit": self.limit, "done": self.done}

    @classmethod
    def from_json(cls, d: dict) -> "PipelineCheckpoint":
        return cls(segment=d.get("segment"), limit=d.get("limit"),
                   done=list(d.get("done", [])))

    def mark_done(self, step: str) -> None:
        if step not in self.done:
            self.done.append(step)

    def is_complete(self) -> bool:
        return all(s in self.done for s in PIPELINE_STEPS)

    def mismatches(self, *, segment, limit) -> list[str]:
        """Reasons the current invocation is incompatible with this checkpoint
        (resuming with different scope would mix two different builds)."""
        out = []
        if self.segment != segment:
            out.append(f"segment: checkpoint={self.segment!r} now={segment!r}")
        if self.limit != limit:
            out.append(f"limit: checkpoint={self.limit!r} now={limit!r}")
        return out


def save_pipeline_checkpoint(path: Path, cp: PipelineCheckpoint) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Temp-then-replace so a kill mid-write can't leave a truncated checkpoint
    # that would block resume (same guard as ingest/resume.save_checkpoint).
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(cp.to_json(), ensure_ascii=False, indent=2),
                   encoding="utf-8")
    tmp.replace(path)


def load_pipeline_checkpoint(path: Path) -> PipelineCheckpoint | None:
    if not path.exists():
        return None
    return PipelineCheckpoint.from_json(json.loads(path.read_text(encoding="utf-8")))


def remaining_steps(done: list[str], *, from_step: str | None = None,
                    only: str | None = None) -> list[str]:
    """The steps still to run, in pipeline order.

    * `only`  -> just that one step (manual single-step run).
    * `from_step` -> that step and everything after it, ignoring `done`
      (manual override: force a redo from a chosen point).
    * otherwise -> every step not already in `done`.
    """
    if only is not None:
        if only not in PIPELINE_STEPS:
            raise ValueError(f"unknown step {only!r}; choose from {PIPELINE_STEPS}")
        return [only]
    if from_step is not None:
        if from_step not in PIPELINE_STEPS:
            raise ValueError(f"unknown step {from_step!r}; choose from {PIPELINE_STEPS}")
        return PIPELINE_STEPS[PIPELINE_STEPS.index(from_step):]
    done_set = set(done)
    return [s for s in PIPELINE_STEPS if s not in done_set]
