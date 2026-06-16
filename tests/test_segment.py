"""segment_bounds: UNSPSC prefix -> numeric range for index-backed scoping."""

from chilecompra_er.ingest.neo4j_source import segment_bounds


def test_none_means_all_segments():
    assert segment_bounds(None) == (None, None)


def test_two_digit_segment():
    assert segment_bounds(42) == (42_000_000, 43_000_000)


def test_single_digit_prefix():
    assert segment_bounds(4) == (40_000_000, 50_000_000)


def test_family_level_prefix():
    assert segment_bounds(4214) == (42_140_000, 42_150_000)


def test_full_code_is_exact_match():
    low, high = segment_bounds(42142614)
    assert (low, high) == (42142614, 42142615)
    assert high - low == 1


def test_range_is_half_open_and_contiguous():
    # segment 42's upper bound is segment 43's lower bound (no overlap/gap)
    _, h42 = segment_bounds(42)
    l43, _ = segment_bounds(43)
    assert h42 == l43
