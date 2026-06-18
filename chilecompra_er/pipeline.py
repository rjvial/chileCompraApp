"""End-to-end pipeline orchestration with step-level resume.

`chilecompra-er pipeline [--resume]` runs the whole catalog build as an ordered
sequence of steps and records which steps have completed in
`data/pipeline.checkpoint.json`. If any step is interrupted, re-running with
`--resume` skips the completed steps and continues at the interrupted one.

Two layers of resume:
  * STEP level (this module): the checkpoint's `done` list says which whole
    steps finished; `--resume` re-runs only what's left.
  * WITHIN a step (ingest/resume.py): the two long `resolve` steps keep their
    own per-run checkpoints under distinct `--out` prefixes, so a kill mid-
    resolve continues from the exact record, not the top of the step.

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
# output in the graph. Resolve steps (build/final-resolve) additionally resume
# within themselves via ingest/resume.py.
STEP_INSTANCE = "instance"
STEP_MIGRATE = "migrate"
STEP_REGISTER = "register"
STEP_BUILD = "build"
STEP_FALLBACK_REPORT = "fallback-report"
STEP_REGISTER_FALLBACK = "register-fallback"
STEP_TRAIN_TIER2 = "train-tier2"
STEP_BRANDS = "build-brand-lexicon"
STEP_FINAL = "final-resolve"

PIPELINE_STEPS = [
    STEP_INSTANCE,
    STEP_MIGRATE,
    STEP_REGISTER,
    STEP_BUILD,
    STEP_FALLBACK_REPORT,
    STEP_REGISTER_FALLBACK,
    STEP_TRAIN_TIER2,
    STEP_BRANDS,
    STEP_FINAL,
]

# Output prefixes for the two resolve steps — distinct so their within-run
# checkpoints (ingest/resume.py) never collide.
BUILD_PREFIX_NAME = "pipeline_build"
FINAL_PREFIX_NAME = "pipeline_final"


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
