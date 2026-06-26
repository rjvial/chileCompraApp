"""End-to-end redesign-pipeline orchestration: checkpoint, step ordering, resume,
and the incremental vocabulary policy."""

import pytest

from chilecompra_er import cli, graphdb
from chilecompra_er.categories import schema as cat_schema
from chilecompra_er.cli import build_parser
from chilecompra_er.pipeline import (
    PIPELINE_STEPS,
    STEP_CANONICALIZE,
    STEP_INSTANCE,
    STEP_MIGRATE,
    STEP_REGISTER,
    PipelineCheckpoint,
    load_pipeline_checkpoint,
    pipeline_checkpoint_path,
    remaining_steps,
    save_pipeline_checkpoint,
)

# cmd_pipeline drives these module-level handlers; stubbing them lets us test
# the orchestration (ordering, skipping, checkpoint persistence) with no graph.
_HANDLERS = ["cmd_migrate", "cmd_register", "cmd_generate_schemas",
             "cmd_canonicalize", "cmd_match", "cmd_adjudicate", "cmd_coherence_check"]


# --- pure checkpoint + ordering logic ----------------------------------------

def test_checkpoint_roundtrip(tmp_path):
    cp = PipelineCheckpoint(segment=42, limit=None, done=[STEP_INSTANCE, STEP_MIGRATE])
    p = pipeline_checkpoint_path(tmp_path)
    save_pipeline_checkpoint(p, cp)
    back = load_pipeline_checkpoint(p)
    assert back.segment == 42 and back.limit is None
    assert back.done == [STEP_INSTANCE, STEP_MIGRATE]
    assert not back.is_complete()


def test_checkpoint_complete():
    cp = PipelineCheckpoint(segment=42, limit=None, done=list(PIPELINE_STEPS))
    assert cp.is_complete()


def test_old_checkpoint_json_loads_cleanly():
    # a checkpoint JSON written before a field was dropped still loads
    cp = PipelineCheckpoint.from_json({"segment": 42, "limit": None,
                                       "done": [STEP_INSTANCE], "loop_sizes": {}})
    assert cp.segment == 42 and cp.done == [STEP_INSTANCE]


def test_mismatch_detection():
    cp = PipelineCheckpoint(segment=42, limit=5000, done=[])
    assert cp.mismatches(segment=42, limit=5000) == []
    bad = cp.mismatches(segment=12, limit=None)
    assert any("segment" in m for m in bad) and any("limit" in m for m in bad)


def test_remaining_skips_done():
    done = [STEP_INSTANCE, STEP_MIGRATE]
    assert remaining_steps(done) == PIPELINE_STEPS[2:]
    assert remaining_steps([]) == PIPELINE_STEPS
    assert remaining_steps(list(PIPELINE_STEPS)) == []


def test_remaining_from_step_ignores_done():
    # --from re-runs that step and everything after, regardless of done
    assert remaining_steps(list(PIPELINE_STEPS), from_step=STEP_CANONICALIZE) \
        == PIPELINE_STEPS[PIPELINE_STEPS.index(STEP_CANONICALIZE):]


def test_remaining_only_is_single_step():
    assert remaining_steps([], only=STEP_REGISTER) == [STEP_REGISTER]


def test_remaining_unknown_step_raises():
    with pytest.raises(ValueError):
        remaining_steps([], from_step="bogus")
    with pytest.raises(ValueError):
        remaining_steps([], only="bogus")


# --- cmd_pipeline orchestration (handlers stubbed) ---------------------------

def _stub_handlers(monkeypatch, *, vocab=None, reachable=False):
    """Replace every cmd_* the pipeline drives with a recorder returning rc=0, and
    stub the instance STARTER + reachability probe. `reachable` drives the instance
    precheck (True -> the starter is skipped); the starter records "starter" and
    returns a fake IP. `vocab` overrides what the register step sees via
    load_register (None = the real committed register, which is present+complete
    -> the register step skips)."""
    calls = []
    for name in _HANDLERS:
        monkeypatch.setattr(cli, name,
                            lambda args, _n=name: calls.append(_n) or 0)
    monkeypatch.setattr(graphdb, "graph_reachable", lambda *a, **k: reachable)
    monkeypatch.setattr(graphdb, "start_neo4j_instance",
                        lambda **kw: calls.append("starter") or "1.2.3.4")
    if vocab is not None:
        monkeypatch.setattr(cat_schema, "load_register", lambda *a, **k: vocab)
    return calls


def _run(argv, tmp_path):
    args = build_parser().parse_args(
        ["pipeline", "--data-dir", str(tmp_path)] + argv)
    return cli.cmd_pipeline(args)


def test_full_run_marks_every_step_done(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    assert _run([], tmp_path) == 0
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.done == PIPELINE_STEPS and cp.is_complete()


def test_fresh_run_refuses_when_checkpoint_exists(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE]))
    # no --resume/--restart -> refuse rather than clobber
    assert _run([], tmp_path) == 1


def test_resume_runs_only_remaining_steps(tmp_path, monkeypatch):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    calls = _stub_handlers(monkeypatch)
    assert _run(["--resume"], tmp_path) == 0
    # instance + migrate already done -> their handlers must NOT be called again
    assert "starter" not in calls and "cmd_migrate" not in calls
    # the remaining build steps ran
    assert "cmd_canonicalize" in calls and "cmd_match" in calls
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.is_complete()


def test_resume_with_nothing_left_is_noop(tmp_path, monkeypatch):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=list(PIPELINE_STEPS)))
    calls = _stub_handlers(monkeypatch)
    assert _run(["--resume"], tmp_path) == 0
    assert calls == []


def test_resume_scope_mismatch_refuses(tmp_path, monkeypatch):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None, done=[]))
    _stub_handlers(monkeypatch)
    # checkpoint was segment 42; resuming as a different segment must refuse
    assert _run(["--resume", "--segment", "12"], tmp_path) == 1


def test_halt_on_step_failure_leaves_prior_steps_done(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    # make migrate fail
    monkeypatch.setattr(cli, "cmd_migrate", lambda a: 2)
    assert _run([], tmp_path) == 2
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    # instance completed before migrate failed; migrate not marked done
    assert cp.done == [STEP_INSTANCE]


def test_adjudicate_failure_is_non_fatal(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    # adjudicate returning non-zero must NOT halt the build
    monkeypatch.setattr(cli, "cmd_adjudicate", lambda a: 7)
    assert _run([], tmp_path) == 0
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.is_complete()


def test_coherence_breach_fails_the_run(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    # a structural breach (rc=1) at the gate fails the pipeline
    monkeypatch.setattr(cli, "cmd_coherence_check", lambda a: 1)
    assert _run([], tmp_path) == 1
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert "coherence-check" not in cp.done


def test_restart_clears_and_reruns(tmp_path, monkeypatch):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    calls = _stub_handlers(monkeypatch)
    assert _run(["--restart"], tmp_path) == 0
    # everything re-ran from the top
    assert "starter" in calls and "cmd_migrate" in calls
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.done == PIPELINE_STEPS


# --- incremental vocabulary policy (the register step) -----------------------

def test_vocab_built_from_nothing(tmp_path, monkeypatch):
    # empty register -> full register (profile + vet + draft)
    calls = _stub_handlers(monkeypatch, vocab={"categories": []})
    assert _run(["--only", "register"], tmp_path) == 0
    assert "cmd_register" in calls and "cmd_generate_schemas" not in calls


def test_vocab_fills_missing_schemas_only(tmp_path, monkeypatch):
    # families present but a schema file is missing -> draft gaps, no re-vet
    calls = _stub_handlers(monkeypatch, vocab={"categories": [
        {"category_id": "x", "schema_file": "schemas/__nonexistent__.json"}]})
    assert _run(["--only", "register"], tmp_path) == 0
    assert "cmd_generate_schemas" in calls and "cmd_register" not in calls


def test_vocab_present_and_complete_is_skipped(tmp_path, monkeypatch):
    # real committed register: present + all schemas on disk -> neither runs
    calls = _stub_handlers(monkeypatch)  # vocab=None -> real load_register
    assert _run(["--only", "register"], tmp_path) == 0
    assert "cmd_register" not in calls and "cmd_generate_schemas" not in calls
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert STEP_REGISTER in cp.done  # skipped but marked done


def test_rebuild_vocab_forces_register(tmp_path, monkeypatch):
    # --rebuild-vocab forces a full register even when vocabulary is present
    calls = _stub_handlers(monkeypatch, vocab={"categories": [
        {"category_id": "x", "schema_file": "schemas/x.json"}]})
    assert _run(["--only", "register", "--rebuild-vocab"], tmp_path) == 0
    assert "cmd_register" in calls


# --- per-stage precheck + skip reporting -------------------------------------

def test_instance_skipped_when_reachable(tmp_path, monkeypatch):
    calls = _stub_handlers(monkeypatch, reachable=True)
    assert _run(["--only", "instance"], tmp_path) == 0
    assert "starter" not in calls               # precheck skipped the starter
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert STEP_INSTANCE in cp.done             # skipped but marked done


def test_starter_runs_when_unreachable(tmp_path, monkeypatch):
    calls = _stub_handlers(monkeypatch, reachable=False)
    assert _run(["--only", "instance"], tmp_path) == 0
    assert "starter" in calls                   # not reachable -> starter ran
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert STEP_INSTANCE in cp.done


def test_starter_failure_halts_cleanly(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch, reachable=False)

    def boom(**kw):
        raise RuntimeError("security group blocks this IP")
    monkeypatch.setattr(graphdb, "start_neo4j_instance", boom)
    assert _run([], tmp_path) == 1              # clean non-zero, not a traceback
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp is None or cp.done == []          # instance never completed


def test_skips_are_reported_with_reasons(tmp_path, monkeypatch, capsys):
    # instance+migrate already done (checkpoint), vocabulary complete (precheck):
    # every skip must be named with its reason.
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    _stub_handlers(monkeypatch)  # real (complete) vocabulary -> register precheck skips
    assert _run(["--resume"], tmp_path) == 0
    cap = capsys.readouterr()
    out = cap.out + cap.err
    # checkpoint skips named
    assert "SKIP instance: already completed" in out
    assert "SKIP migrate: already completed" in out
    # precheck skip named with reason
    assert "SKIP: vocabulary present" in out
    # summary lists what ran vs skipped
    assert "pipeline summary" in out and "skipped : register" in out


# --- status / watch ----------------------------------------------------------

def test_status_reports_plan(tmp_path, monkeypatch, capsys):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    _stub_handlers(monkeypatch)
    assert _run(["--status"], tmp_path) == 0
    out = capsys.readouterr().out
    assert f"2/{len(PIPELINE_STEPS)} steps done" in out
    assert "[x] instance" in out and "[x] migrate" in out
    assert "[>] register" in out          # the next step to run
    assert "[ ] coherence-check" in out


def test_status_with_no_checkpoint_is_noop(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    assert _run(["--status"], tmp_path) == 0  # nothing established yet


def test_watch_flag_parses(tmp_path):
    args = build_parser().parse_args(
        ["pipeline", "--data-dir", str(tmp_path), "--watch", "--interval", "5"])
    assert args.watch is True and args.interval == 5
