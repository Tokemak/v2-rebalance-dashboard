import streamlit as st


from mainnet_launch.autopool_diagnostics.fees import fetch_autopool_fee_data, fetch_and_render_autopool_fee_data
from mainnet_launch.autopool_diagnostics.deposits_and_withdrawals import display_autopool_deposit_withdraw_stats
from mainnet_launch.autopool_diagnostics.count_of_destinations import display_autopool_destination_counts
from mainnet_launch.autopool_diagnostics.turnover import display_autopool_turnover
from mainnet_launch.constants import (
    ALL_AUTOPOOLS,
    AUTOPOOL_NAME_TO_CONSTANTS,
    STREAMLIT_MARKDOWN_HTML,
    AutopoolConstants,
)


@st.cache_data(ttl=3600)
def fetch_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_autopool_fee_data(autopool)


def fetch_and_render_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_and_render_autopool_fee_data(autopool)

    # display_autopool_deposit_withdraw_stats(autopool)
    # display_autopool_destination_counts(autopool)
    # display_autopool_turnover(autopool)
