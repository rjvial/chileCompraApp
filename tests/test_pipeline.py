"""End-to-end pipeline orchestration: checkpoint, step ordering, resume."""

import types

import pytest

from chilecompra_er import cli, graphdb
from chilecompra_er.cli import build_parser
from chilecompra_er.ingest import neo4j_source
from chilecompra_er.pipeline import (
    PIPELINE_STEPS,
    RESOLVE_STEPS,
    STEP_BUILD,
    STEP_FINAL,
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
_HANDLERS = ["cmd_instance", "cmd_migrate", "cmd_register", "cmd_resolve",
             "cmd_fallback_report", "cmd_train_tier2", "cmd_build_brand_lexicon"]

_LOOP_SIZE = 1000  # what the stubbed count returns for the resolve loop size


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
    assert remaining_steps(list(PIPELINE_STEPS), from_step=STEP_BUILD) \
        == PIPELINE_STEPS[PIPELINE_STEPS.index(STEP_BUILD):]


def test_remaining_only_is_single_step():
    assert remaining_steps([], only=STEP_REGISTER) == [STEP_REGISTER]


def test_remaining_unknown_step_raises():
    with pytest.raises(ValueError):
        remaining_steps([], from_step="bogus")
    with pytest.raises(ValueError):
        remaining_steps([], only="bogus")


# --- cmd_pipeline orchestration (handlers stubbed) ---------------------------

def _stub_handlers(monkeypatch):
    """Replace every cmd_* the pipeline drives with a recorder returning rc=0,
    and stub the loop-size precompute so it never touches a real graph."""
    calls = []
    for name in _HANDLERS:
        monkeypatch.setattr(cli, name,
                            lambda args, _n=name: calls.append(_n) or 0)
    monkeypatch.setattr(graphdb, "get_connection",
                        lambda: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(neo4j_source, "count_resolve_items",
                        lambda conn, **kw: _LOOP_SIZE)
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
    assert "cmd_instance" not in calls and "cmd_migrate" not in calls
    # register ran (the first remaining step)
    assert "cmd_register" in calls
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
    calls = []

    def ok(args, _n):
        calls.append(_n)
        return 0

    for name in _HANDLERS:
        monkeypatch.setattr(cli, name, lambda a, _n=name: ok(a, _n))
    # make migrate fail
    monkeypatch.setattr(cli, "cmd_migrate", lambda a: 2)
    assert _run([], tmp_path) == 2
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    # instance completed before migrate failed; migrate not marked done
    assert cp.done == [STEP_INSTANCE]


def test_restart_clears_and_reruns(tmp_path, monkeypatch):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    calls = _stub_handlers(monkeypatch)
    assert _run(["--restart"], tmp_path) == 0
    # everything re-ran from the top
    assert "cmd_instance" in calls and "cmd_migrate" in calls
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.done == PIPELINE_STEPS


# --- precomputed loop size (deterministic, saved, consultable, resumable) ----

def test_loop_sizes_roundtrip(tmp_path):
    cp = PipelineCheckpoint(segment=42, limit=None, done=[],
                            loop_sizes={STEP_BUILD: 1342833, STEP_FINAL: 1342833})
    p = pipeline_checkpoint_path(tmp_path)
    save_pipeline_checkpoint(p, cp)
    assert load_pipeline_checkpoint(p).loop_sizes == {STEP_BUILD: 1342833,
                                                      STEP_FINAL: 1342833}


def test_old_checkpoint_has_empty_loop_sizes():
    # a checkpoint JSON written before the field existed loads cleanly
    cp = PipelineCheckpoint.from_json({"segment": 42, "limit": None,
                                       "done": [STEP_INSTANCE]})
    assert cp.loop_sizes == {}


def test_loop_sizes_precomputed_after_migrate(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    assert _run([], tmp_path) == 0
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    # both resolve steps got the deterministic count, computed once after migrate
    assert cp.loop_sizes == {STEP_BUILD: _LOOP_SIZE, STEP_FINAL: _LOOP_SIZE}
    assert set(cp.loop_sizes) == set(RESOLVE_STEPS)


def test_loop_sizes_backfilled_on_resume_when_missing(tmp_path, monkeypatch):
    # checkpoint predates the field (migrate already done, no loop_sizes)
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(segment=42, limit=None,
                                                done=[STEP_INSTANCE, STEP_MIGRATE]))
    _stub_handlers(monkeypatch)
    assert _run(["--resume"], tmp_path) == 0
    cp = load_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path))
    assert cp.loop_sizes == {STEP_BUILD: _LOOP_SIZE, STEP_FINAL: _LOOP_SIZE}


def test_loop_size_passed_to_resolve(tmp_path, monkeypatch):
    seen = {}
    _stub_handlers(monkeypatch)

    def capture_resolve(args):
        seen[args.out.name] = args.total
        return 0

    # override the resolve stub to capture the precomputed total it receives
    monkeypatch.setattr(cli, "cmd_resolve", capture_resolve)
    assert _run([], tmp_path) == 0
    # both resolve invocations were handed the deterministic loop size
    assert set(seen.values()) == {_LOOP_SIZE}


def test_status_reports_plan_and_progress(tmp_path, monkeypatch, capsys):
    save_pipeline_checkpoint(pipeline_checkpoint_path(tmp_path),
                             PipelineCheckpoint(
                                 segment=42, limit=None,
                                 done=[STEP_INSTANCE, STEP_MIGRATE],
                                 loop_sizes={STEP_BUILD: 1342833,
                                             STEP_FINAL: 1342833}))
    _stub_handlers(monkeypatch)
    assert _run(["--status"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "2/9 steps done" in out
    assert "1,342,833" in out          # the precomputed loop size is shown
    assert "[x] instance" in out and "[ ] build" in out


def test_status_with_no_checkpoint_is_noop(tmp_path, monkeypatch):
    _stub_handlers(monkeypatch)
    assert _run(["--status"], tmp_path) == 0  # nothing established yet
