from mainnet_launch.database.schema.full import RebalancePlans
from mainnet_launch.database.schema.postgres_operations import get_highest_value_in_field_where
from datetime import datetime, timezone, timedelta

import sys
import time
import streamlit as st
from io import StringIO
from contextlib import redirect_stdout
from mainnet_launch.database.schema.ensure_tables_are_current.ensure_all_tables_are_current import (
    ensure_database_is_current,
)


def _human_timedelta(td: timedelta) -> str:
    secs = int(td.total_seconds())
    days, secs = divmod(secs, 86400)
    hours, secs = divmod(secs, 3600)
    minutes, secs = divmod(secs, 60)
    if days > 0:
        return f"{days}d {hours}h {minutes}m ago"
    elif hours > 0:
        return f"{hours}h {minutes}m ago"
    elif minutes > 0:
        return f"{minutes}m {secs}s ago"
    else:
        return f"{secs}s ago"


@st.cache_data(ttl=3600)  # Cache for 1 hour
def _should_update_streamlit_server() -> bool:
    # update on the 12 hour mark
    # update if we don't have a plan in the last hour
    latest_datetime = get_highest_value_in_field_where(
        RebalancePlans, RebalancePlans.datetime_generated, where_clause=None
    )

    now = datetime.now(timezone.utc)
    delta = now - latest_datetime
    print(
        f"Time since last update: {_human_timedelta(delta)}, "
        f"Last update: {latest_datetime}, "
        f"Current time: {now}"
    )
    return delta > timedelta(hours=12)


def _update_on_streamlit_server():
    st.title("Database Update Log")

    # Create a placeholder for live log streaming
    log_placeholder = st.empty()
    log_buffer = StringIO()

    # Wrap the function call to capture stdout
    with redirect_stdout(log_buffer):
        ensure_database_is_current(echo_sql_to_console=True)

    # Split output into lines and stream them gradually
    for line in log_buffer.getvalue().splitlines():
        log_placeholder.text(log_placeholder.text() + "\n" + line if log_placeholder.text() else line)
        time.sleep(0.05)  # small delay for streaming effect

    st.success("Database update complete.")


def update_if_needed_on_streamlit_server():
    """
    Checks if the database needs to be updated and performs the update if necessary.
    This function is intended to be called on the Streamlit server.
    """
    if _should_update_streamlit_server():
        _update_on_streamlit_server()
    else:
        print("Database is up-to-date. No update needed.")
