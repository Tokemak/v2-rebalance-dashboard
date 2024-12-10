import streamlit as st


from mainnet_launch.autopool_diagnostics.fees import (
    fetch_autopool_fee_data,
    fetch_and_render_autopool_fee_data,
    fetch_autopool_destination_debt_reporting_events,
    fetch_and_render_autopool_rewardliq_plot,
)
from mainnet_launch.autopool_diagnostics.deposits_and_withdrawals import (
    fetch_autopool_deposit_and_withdraw_stats_data,
    fetch_and_render_autopool_deposit_and_withdraw_stats_data,
)
from mainnet_launch.autopool_diagnostics.count_of_destinations import (
    fetch_autopool_destination_counts_data,
    fetch_and_render_autopool_destination_counts_data,
)
from mainnet_launch.autopool_diagnostics.turnover import fetch_turnover_data, fetch_and_render_turnover_data

from mainnet_launch.autopool_diagnostics.returns_before_expenses import (
    fetch_autopool_return_and_expenses_metrics,
    fetch_and_render_autopool_return_and_expenses_metrics,
)
from mainnet_launch.constants import AutopoolConstants


def fetch_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_autopool_fee_data(autopool)
    fetch_turnover_data(autopool)
    fetch_autopool_deposit_and_withdraw_stats_data(autopool)
    fetch_autopool_destination_counts_data(autopool)
    fetch_autopool_return_and_expenses_metrics(autopool)
    fetch_autopool_destination_debt_reporting_events(autopool)


def fetch_and_render_autopool_diagnostics_data(autopool: AutopoolConstants):
    fetch_and_render_autopool_fee_data(autopool)
    fetch_and_render_turnover_data(autopool)
    fetch_and_render_autopool_deposit_and_withdraw_stats_data(autopool)
    fetch_and_render_autopool_destination_counts_data(autopool)
    fetch_and_render_autopool_return_and_expenses_metrics(autopool)
    fetch_and_render_autopool_rewardliq_plot(autopool)
