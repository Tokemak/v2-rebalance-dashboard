import streamlit as st

# this needs to be first because otherwise we get this error:
# `StreamlitAPIException: set_page_config() can only be called once per app page,
# and must be called as the first Streamlit command in your script.`
st.set_page_config(
    page_title="Mainnet Autopool Diagnostics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


import threading
import time
import datetime


from mainnet_launch.top_level.key_metrics import display_key_metrics
from mainnet_launch.destination_diagnostics.weighted_crm import display_weighted_crm
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    display_destination_allocation_over_time,
)
from mainnet_launch.solver_diagnostics.rebalance_events import display_rebalance_events
from mainnet_launch.autopool_diagnostics.autopool_lp_stats import display_autopool_lp_stats

from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
)


def display_autopool_exposure(pool_name):
    st.write(f"Displaying Autopool Exposure for {pool_name}...")
    # Add content here


CONTENT_FUNCTIONS = {
    "Key Metrics": display_key_metrics,
    "Autopool Exposure": display_autopool_exposure,
    "Allocation Over Time": display_destination_allocation_over_time,
    "Weighted CRM": display_weighted_crm,
    "Rebalance Events": display_rebalance_events,
    "Autopool Deposits and Withdrawals": display_autopool_lp_stats,
}


def main():

    st.markdown(STREAMLIT_MARKDOWN_HTML, unsafe_allow_html=True)
    st.title("Autopool Diagnostics Dashboard")

    st.sidebar.title("Navigation")

    names = [autopool.name for autopool in ALL_AUTOPOOLS]
    pool_name = st.sidebar.selectbox("Select Pool", names)
    autopool = AUTOPOOL_NAME_TO_CONSTANTS[pool_name]
    page = st.sidebar.radio("Go to", CONTENT_FUNCTIONS.keys())

    CONTENT_FUNCTIONS[page](autopool)


if __name__ == "__main__":
    main()
