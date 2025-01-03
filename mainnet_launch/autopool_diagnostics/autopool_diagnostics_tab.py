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


import time
from typing import Callable


def time_function(func: Callable, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Function {func.__name__} took {elapsed_time:.4f} seconds.")
    return result, elapsed_time


def fetch_and_render_autopool_diagnostics_data_time(autopool):
    timings = {}

    # Timing each function call
    _, timings["autopool_fee_data"] = time_function(fetch_and_render_autopool_fee_data, autopool)
    _, timings["turnover_data"] = time_function(fetch_and_render_turnover_data, autopool)
    _, timings["deposit_withdraw_stats_data"] = time_function(
        fetch_and_render_autopool_deposit_and_withdraw_stats_data, autopool
    )
    _, timings["destination_counts_data"] = time_function(fetch_and_render_autopool_destination_counts_data, autopool)
    _, timings["return_and_expenses_metrics"] = time_function(
        fetch_and_render_autopool_return_and_expenses_metrics, autopool
    )
    _, timings["rewardliq_plot"] = time_function(fetch_and_render_autopool_rewardliq_plot, autopool)

    # Print overall timings
    print("\nTiming Summary:")
    for key, value in timings.items():
        print(f"{key}: {value:.4f} seconds")

    return timings


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH

    fetch_and_render_autopool_diagnostics_data_time(AUTO_ETH)
