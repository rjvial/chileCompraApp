"""Append-only progress timeline (monitoring-only).

A tiny JSONL appender shared by the long-running steps so `pipeline --status`
can show rate + ETA, and so progress survives across kills (the file is
appended, never rewritten — a resumed run continues the same timeline). One
JSON object per line.

Lives on its own (json-only, no graph/LLM deps) so any step can record samples
without dragging in heavier machinery.
"""

from __future__ import annotations

import json
from pathlib import Path


def append_progress(path: Path, sample: dict) -> None:
    # Best-effort: the progress timeline is monitoring-only, so a transient file
    # lock (antivirus, a syncing folder, the editor) must never crash the run —
    # drop the sample and carry on.
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(sample, ensure_ascii=False) + "\n")
    except OSError:
        pass
