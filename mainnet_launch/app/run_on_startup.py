import pandas as pd
import os
import time
import cProfile
import pstats
import io
from functools import wraps
from datetime import datetime
import streamlit as st

import concurrent.futures

from mainnet_launch.data_fetching.add_info_to_dataframes import initialize_tx_hash_to_gas_info_db
from mainnet_launch.data_fetching.get_state_by_block import _add_to_blocks_to_use_table
from mainnet_launch.database.should_update_database import ensure_table_to_last_updated_exists
from mainnet_launch.pages.key_metrics.fetch_nav_per_share import add_new_nav_per_share_to_table
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
    add_new_destination_summary_stats_to_table,
)
from mainnet_launch.pages.rebalance_events.rebalance_events import add_new_rebalance_events_for_each_autopool_to_table
from mainnet_launch.destinations import add_new_destination_details_for_each_chain_to_table
from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
    add_new_fee_events_to_table,
    add_new_debt_reporting_events_to_table,
)
from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
    ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
)
from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import (
    fetch_keeper_network_gas_costs,
    add_chainlink_upkeep_events_to_table,
)
from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import (
    add_new_reward_token_swapped_events_to_table,
)
from mainnet_launch.pages.autopool_diagnostics.deposits_and_withdrawals import (
    add_new_autopool_deposit_and_withdraw_events_to_table,
)
from mainnet_launch.pages.autopool_diagnostics.fetch_values_nav_and_shares_and_expenses import (
    add_new_acutal_nav_and_acutal_shares_to_table,
)
from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
    add_new_asset_oracle_and_discount_price_rows_to_table,
)
from mainnet_launch.constants import STARTUP_LOG_FILE


def write_pandas(stats, top_level_function):
    """
    Process the profiling stats and append the data to a CSV file using pandas.
    The CSV will have columns: ncalls, tottime, percall_tottime, cumtime, percall_cumtime,
    filename, lineno, function.
    """
    rows = []
    # stats.stats is a dict where each key is (filename, lineno, funcname)
    # and each value is a tuple: (primitive_calls, total_calls, total_time, cumulative_time, callers)
    for key, value in stats.stats.items():
        filename, lineno, func_name = key
        # Only include functions from the mainnet_launch package
        if "mainnet_launch" in filename:
            primitive_calls, total_calls, total_time, cumulative_time, callers = value
            percall_tottime = total_time / total_calls if total_calls else 0
            percall_cumtime = cumulative_time / total_calls if total_calls else 0
            rows.append(
                {
                    "top_level_function": top_level_function,
                    "ncalls": total_calls,
                    "tottime": total_time,
                    "percall_tottime": percall_tottime,
                    "cumtime": cumulative_time,
                    "percall_cumtime": percall_cumtime,
                    "filename": filename,
                    "lineno": lineno,
                    "function": func_name,
                }
            )
    if rows:
        df_new = pd.DataFrame(rows)
        # Append to the CSV if it exists; otherwise, create a new file with a header.
        if os.path.exists(STARTUP_LOG_FILE):
            df_new.to_csv(STARTUP_LOG_FILE, mode="a", header=False, index=False)
        else:
            df_new.to_csv(STARTUP_LOG_FILE, mode="w", header=True, index=False)


def log_usage(production_logger):
    """
    A decorator that profiles the decorated function using cProfile and logs both a
    human-readable summary and appends profiling data to a CSV file using pandas.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Profile the function execution.
            profiler = cProfile.Profile()
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()

            # Generate the stats summary.
            stream = io.StringIO()
            stats = pstats.Stats(profiler, stream=stream)

            write_pandas(stats, top_level_function=func.__name__)

            return result

        return wrapper

    return decorator


functions_to_run = [
    add_new_destination_details_for_each_chain_to_table,
    add_new_nav_per_share_to_table,
    add_new_destination_summary_stats_to_table,
    add_new_rebalance_events_for_each_autopool_to_table,
    add_new_fee_events_to_table,
    add_new_debt_reporting_events_to_table,
    ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
    fetch_keeper_network_gas_costs,
    add_new_reward_token_swapped_events_to_table,
    add_new_autopool_deposit_and_withdraw_events_to_table,
    add_new_acutal_nav_and_acutal_shares_to_table,
    add_chainlink_upkeep_events_to_table,
    add_new_asset_oracle_and_discount_price_rows_to_table,
]


def run_all_data_fetching_concurrently():
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_func = {executor.submit(fn): fn.__name__ for fn in functions_to_run}
        for future in concurrent.futures.as_completed(future_to_func):
            func_name = future_to_func[future]
            try:
                future.result()
            except Exception as e:
                st.text(f"An error occurred in function {func_name}: {e}")


def first_run_of_db(production_logger):
    log_usage(production_logger)(initialize_tx_hash_to_gas_info_db)()
    log_usage(production_logger)(ensure_table_to_last_updated_exists)()
    log_usage(production_logger)(_add_to_blocks_to_use_table)()
    log_usage(production_logger)(run_all_data_fetching_concurrently)()
