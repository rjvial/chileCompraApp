from pathlib import Path

from chilecompra_er.cli import build_parser


def parse(argv):
    return build_parser().parse_args(argv)


def test_resolve_is_dry_run_by_default():
    args = parse(["resolve", "--contains", "foley", "--limit", "10"])
    assert args.persist is False
    assert args.kind == "tender"


def test_resolve_limit_all_means_no_cap():
    assert parse(["resolve", "--limit", "all"]).limit is None
    assert parse(["resolve", "--limit", "0"]).limit is None
    assert parse(["resolve", "--limit", "500"]).limit == 500


def test_wipe_requires_explicit_yes_flag():
    args = parse(["wipe-category", "sondas_foley"])
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


def test_every_subcommand_has_a_handler():
    for argv in (["status"], ["migrate", "--dry-run"],
                 ["generate-schemas", "--only", "jeringas"]):
        assert callable(parse(argv).func)
