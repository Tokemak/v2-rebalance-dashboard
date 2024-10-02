import streamlit as st


from mainnet_launch.autopool_diagnostics.fees import fetch_autopool_fee_data, fetch_and_render_autopool_fee_data
from mainnet_launch.autopool_diagnostics.deposits_and_withdrawals import (
    fetch_autopool_deposit_and_withdraw_stats_data,
    fetch_and_render_autopool_deposit_and_withdraw_stats_data,
)
from mainnet_launch.autopool_diagnostics.count_of_destinations import (
    fetch_autopool_destination_counts_data,
    fetch_and_render_autopool_destination_counts_data,
)
from mainnet_launch.autopool_diagnostics.turnover import fetch_turnover_data, fetch_and_render_turnover_data
from mainnet_launch.constants import CACHE_TIME, AutopoolConstants


def fetch_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_autopool_fee_data(autopool)
    fetch_turnover_data(autopool)
    fetch_autopool_deposit_and_withdraw_stats_data(autopool)
    fetch_autopool_destination_counts_data(autopool)


def fetch_and_render_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_and_render_autopool_fee_data(autopool)
    fetch_and_render_turnover_data(autopool)
    fetch_and_render_autopool_deposit_and_withdraw_stats_data(autopool)
    fetch_and_render_autopool_destination_counts_data(autopool)
