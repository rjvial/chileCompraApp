from pathlib import Path

from chilecompra_er.cli import build_parser


def parse(argv):
    return build_parser().parse_args(argv)


def test_match_is_offline_by_default():
    args = parse(["match", "--segment", "42"])
    assert args.persist is False
    assert args.segment == 42


def test_pipeline_limit_all_means_no_cap():
    assert parse(["pipeline", "--limit", "all"]).limit is None
    assert parse(["pipeline", "--limit", "0"]).limit is None
    assert parse(["pipeline", "--limit", "500"]).limit == 500


def test_wipe_requires_explicit_yes_flag():
    args = parse(["wipe-clusters"])
    assert args.yes is False  # cmd refuses without it


def test_register_registers_by_default():
    args = parse(["register", "--segment", "42", "--count", "10", "--reprofile"])
    # default runs the whole loop and registers — neither opt-out flag set
    assert args.apply is False and args.preview is False
    assert args.segment == 42 and args.count == 10 and args.reprofile is True
    assert args.proposals == Path("data/proposals.json")


def test_register_preview_flag_skips_registering():
    args = parse(["register", "--preview"])
    assert args.preview is True and args.apply is False


def test_register_apply_reads_the_proposals_file():
    args = parse(["register", "--apply", "--proposals", "data/p.json"])
    assert args.apply is True and args.proposals == Path("data/p.json")


def test_register_preview_and_apply_are_mutually_exclusive():
    import pytest
    with pytest.raises(SystemExit):
        parse(["register", "--preview", "--apply"])


def test_instance_actions():
    for action in ("start", "stop", "status"):
        assert parse(["instance", action]).action == action


def test_clean_keeps_cached_inputs_removes_artifacts(tmp_path):
    from chilecompra_er.cli import cmd_clean
    import argparse
    for name in ("profiling.csv", "proposals.json", "check_resoluciones.csv",
                 "check.checkpoint.json", "run.log", "price_series_x.csv"):
        (tmp_path / name).write_text("x", encoding="utf-8")
    cmd_clean(argparse.Namespace(dir=tmp_path, all=False, dry_run=False))
    left = {p.name for p in tmp_path.iterdir()}
    assert left == {"profiling.csv", "proposals.json"}


def test_clean_dry_run_deletes_nothing(tmp_path):
    from chilecompra_er.cli import cmd_clean
    import argparse
    (tmp_path / "check_resoluciones.csv").write_text("x", encoding="utf-8")
    cmd_clean(argparse.Namespace(dir=tmp_path, all=False, dry_run=True))
    assert (tmp_path / "check_resoluciones.csv").exists()


def test_clean_all_removes_cached_inputs_too(tmp_path):
    from chilecompra_er.cli import cmd_clean
    import argparse
    (tmp_path / "profiling.csv").write_text("x", encoding="utf-8")
    (tmp_path / "proposals.json").write_text("x", encoding="utf-8")
    cmd_clean(argparse.Namespace(dir=tmp_path, all=True, dry_run=False))
    assert list(tmp_path.iterdir()) == []


def test_every_subcommand_has_a_handler():
    for argv in (["status"], ["migrate", "--dry-run"],
                 ["generate-schemas", "--only", "jeringas"]):
        assert callable(parse(argv).func)
