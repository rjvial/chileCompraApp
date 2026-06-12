from chilecompra_er.cli import build_parser


def parse(argv):
    return build_parser().parse_args(argv)


def test_resolve_is_dry_run_by_default():
    args = parse(["resolve", "--contains", "foley", "--limit", "10"])
    assert args.persist is False
    assert args.kind == "tender"


def test_wipe_requires_explicit_yes_flag():
    args = parse(["wipe-category", "sondas_foley"])
    assert args.yes is False  # cmd refuses without it


def test_profile_arguments():
    args = parse(["profile", "--segment", "42", "--top", "10"])
    assert args.segment == 42 and args.top == 10


def test_instance_actions():
    for action in ("start", "stop", "status"):
        assert parse(["instance", action]).action == action


def test_every_subcommand_has_a_handler():
    for argv in (["status"], ["migrate", "--dry-run"],
                 ["generate-schemas", "--only", "jeringas"]):
        assert callable(parse(argv).func)
