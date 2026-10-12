from pitlake.snapshots import equivalent_version


def test_optimize_after_the_commit_keeps_the_same_rows():
    history = [(13, "OPTIMIZE"), (14, "OPTIMIZE")]
    assert equivalent_version(12, history) == 14


def test_a_write_after_the_commit_is_never_read():
    history = [(13, "OPTIMIZE"), (14, "WRITE"), (15, "OPTIMIZE")]
    assert equivalent_version(12, history) == 13
    assert equivalent_version(12, [(13, "WRITE")]) == 12
    assert equivalent_version(12, []) == 12
