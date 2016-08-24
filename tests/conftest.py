import py.test

def pytest_addoption(parser):
    parser.addoption('--skip', action='store_true', default=False,
                      help='Also run skip marked tests')


def pytest_runtest_setup(item):
    """Skip tests if they are marked as slow and --slow is not given"""
    if getattr(item.obj, 'skip', None) and not item.config.getvalue('skip'):
        py.test.skip('skip marked tests not requested')