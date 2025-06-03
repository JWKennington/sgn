"""Pytest configuration for test ordering."""


def pytest_collection_modifyitems(session, config, items):
    """Reorder tests to run subprocess tests first.

    This helps avoid multiprocessing pickling issues that occur when
    spawn mode is set after other tests have already run.
    """
    # Separate subprocess tests from others
    subprocess_tests = []
    other_tests = []

    for item in items:
        if "subprocess" in str(item.fspath):
            subprocess_tests.append(item)
        else:
            other_tests.append(item)

    # Reorder: subprocess tests first, then everything else
    items[:] = subprocess_tests + other_tests
