# conftest.py
import pytest

_total_duration = 0.0


def pytest_runtest_logreport(report: pytest.TestReport):
    global _total_duration
    # count only the actual test call, not setup/teardown
    if report.when == "call":
        _total_duration += report.duration


def _format_duration(seconds: float) -> str:
    minutes, secs = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{int(hours)}h {int(minutes):02d}m {secs:05.2f}s"
    elif minutes:
        return f"{int(minutes)}m {secs:05.2f}s"
    else:
        return f"{secs:.2f}s"


def pytest_terminal_summary(terminalreporter, exitstatus):
    # This hook runs after all tests *and* after most summary info is gathered.
    pretty = _format_duration(_total_duration)
    terminalreporter.write_sep(
        "=",
        f"Total test duration (sum of all tests): {pretty}",
    )
