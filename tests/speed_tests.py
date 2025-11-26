import sys
import pytest


def main() -> None:
    # Only run tests marked "speed", with a single worker
    sys.exit(pytest.main(["-m", "speed", "-n", "1"]))
