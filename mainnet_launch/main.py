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


from mainnet_launch.autopool_diagnostics.fees import display_autopool_fees
from mainnet_launch.autopool_diagnostics.deposits_and_withdrawals import display_autopool_deposit_withdraw_stats
from mainnet_launch.autopool_diagnostics.count_of_destinations import display_autopool_destination_counts
from mainnet_launch.autopool_diagnostics.turnover import display_autopool_turnover


from mainnet_launch.top_level.key_metrics import display_key_metrics
from mainnet_launch.destination_diagnostics.weighted_crm import display_weighted_crm
from mainnet_launch.autopool_diagnostics.destination_allocation_over_time import (
    display_destination_allocation_over_time,
)
from mainnet_launch.solver_diagnostics.rebalance_events import display_rebalance_events


from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)


def display_autopool_diagnostics(autopool: AutopoolConstants):
    display_autopool_fees(autopool)
    display_autopool_deposit_withdraw_stats(autopool)
    display_autopool_destination_counts(autopool)
    display_autopool_turnover(autopool)


def display_autopool_exposure(autopool: AutopoolConstants):
    display_destination_allocation_over_time(autopool)

    st.text(
        """ 

        - Token Exposure pie chart
        
        """
    )


def display_solver_diagnostics(autopool: AutopoolConstants):
    st.text(
    """
    - Up time
    - rebalance plans generated over 7 days, over 30 days, YTD
    - rebalance plans successfully executed (% execution)
    - Solver Gas Costs
    - Solver Earnings
    - Aggregator Win Distribution (% 0x, prop, lifi wins)
    - Swap costs distribution (absolute & normalized by ETH)
    - Predicted gain distribution (absolute & normalized by ETH)
    - Rebalance size distribution
    - Rank of the destination chosen for “add” in the list of destinations sorted by in-CRM
    - Size of the candidate set that qualified for “add”
    """
    )


def display_destination_diagnostics(autopool: AutopoolConstants):
    st.text(
        """
    - unweighted price return (discounts/premiums) over time
    - fee apr
    - base apr
    - incentive apr (in / out)
    - points hook output → autoLRT
    - Growth of a dollar per destination, (don’t auto compound rewards tokens, just let the rewards pile up and price at eth value)
    - Time between Incentive APR snapshots
    - LP safe price
    - LP spot price

    """
    )


CONTENT_FUNCTIONS = {
    "Key Metrics": display_key_metrics,
    "Autopool Diagnostics": display_autopool_diagnostics,
    "Autopool Exposure": display_autopool_exposure,
    "Autopool APRs": display_weighted_crm,
    "Destination Diagnostics": display_destination_diagnostics,
    "Rebalance Events": display_rebalance_events,
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
