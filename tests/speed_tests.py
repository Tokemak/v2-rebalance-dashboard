import sys
import pytest


def main() -> None:
    # Run all page tests (autopool, protocol, risk metrics) with a single worker for consistent timings
    sys.exit(
        pytest.main(
            [
                "tests/test_app_pages.py",
                "-m",
                "not marketing",
                "-n",
                "1",
                "--durations=0",
            ]
        )
    )
