def pytest_addoption(parser):
    parser.addoption(
        "--passWithNoTests",
        action="store_true",
        default=False,
        help="Exit 0 even when no tests are collected (mirrors Jest behaviour).",
    )


def pytest_sessionfinish(session, exitstatus):
    if exitstatus == 5 and session.config.getoption("--passWithNoTests", default=False):
        session.exitstatus = 0
