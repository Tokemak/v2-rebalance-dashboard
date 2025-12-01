import os
import datetime
import pandas as pd
import pytest
import streamlit as st

from mainnet_launch.constants import SessionState

os.environ.setdefault("STREAMLIT_HEADLESS", "1")


def set_streamlit_session_state_for_tests():
    st.session_state[SessionState.RECENT_START_DATE] = pd.Timestamp(
        datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=90)
    ).isoformat()


def pytest_addoption(parser):
    parser.addoption(
        "--only-render-recent-data",
        action="store",
        type=lambda v: v.lower(),  # normalize case
        choices=("true", "false"),  # parser will error if not one of these
        default="true",
        help='Restrict to rendering only 90 days of data: "true" or "false".',
    )


@pytest.fixture(autouse=True)
def _set_session_state(request):
    st.session_state.clear()

    only_recent_raw = request.config.getoption("--only-render-recent-data")
    only_recent = only_recent_raw == "true"

    if only_recent:
        set_streamlit_session_state_for_tests()

    yield
    st.session_state.clear()


# only local machine Dec 1, 2025
# poetry run pytest --only-render-recent-data=true  -> 47 seconds
# poetry run pytest --only-render-recent-data=false -> 72 seconds
