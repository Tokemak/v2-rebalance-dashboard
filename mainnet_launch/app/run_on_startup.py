import pandas as pd
import os
import csv  # only needed if you still want to use csv elsewhere
import time
import cProfile
import pstats
import io
from functools import wraps
from datetime import datetime
import streamlit as st
from mainnet_launch.data_fetching.add_info_to_dataframes import initialize_tx_hash_to_gas_info_db
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
from mainnet_launch.constants import PRODUCTION_LOG_FILE_NAME, STARTUP_LOG_FILE


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
            start_time = time.time()
            start_datetime = datetime.fromtimestamp(start_time).isoformat()

            # Profile the function execution.
            profiler = cProfile.Profile()
            profiler.enable()
            result = func(*args, **kwargs)
            profiler.disable()

            elapsed = time.time() - start_time

            # Generate the stats summary.
            stream = io.StringIO()
            stats = pstats.Stats(profiler, stream=stream)
            # stats.sort_stats("cumulative")
            # with open(STARTUP_LOG_FILE, "a") as f:
            #     f.write(f"{func.__name__} started at {start_datetime} and took {elapsed:.2f} seconds\n")
            #     f.write("Detailed function timing breakdown:\v n")
            #     f.write(stream.getvalue())
            #     f.write("\n" + "=" * 80 + "\n")

            # Append the profiling data to a CSV using pandas.
            write_pandas(stats, top_level_function=func.__name__)

            return result

        return wrapper

    return decorator


functions_to_run = [
    initialize_tx_hash_to_gas_info_db,
    ensure_table_to_last_updated_exists,
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


def first_run_of_db(production_logger):
    status_container = st.empty()
    # # Read and display the complete production log.
    # if os.path.exists(STARTUP_LOG_FILE):
    #     with open(STARTUP_LOG_FILE, "r") as log_file:
    #         log_contents = log_file.read()
    #     status_container.text(log_contents)
    # else:
    #     status_container.text("Production log file not found.")

    for func in functions_to_run:
        log_usage(production_logger)(func)()
        status_container.text(func.__name__)
        # status_container = st.empty()
        # # Read and display the updated production log.
        # if os.path.exists(STARTUP_LOG_FILE):
        #     with open(STARTUP_LOG_FILE, "r") as log_file:
        #         log_contents = log_file.read()
        #     status_container.text(log_contents)
        # else:
        #     status_container.text("Production log file not found.")


# from mainnet_launch.data_fetching.add_info_to_dataframes import initialize_tx_hash_to_gas_info_db
# from mainnet_launch.database.should_update_database import ensure_table_to_last_updated_exists
# from mainnet_launch.pages.key_metrics.fetch_nav_per_share import add_new_nav_per_share_to_table
# from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
#     add_new_destination_summary_stats_to_table,
# )
# from mainnet_launch.pages.rebalance_events.rebalance_events import add_new_rebalance_events_for_each_autopool_to_table
# from mainnet_launch.destinations import add_new_destination_details_for_each_chain_to_table
# from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
#     add_new_fee_events_to_table,
#     add_new_debt_reporting_events_to_table,
# )
# from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
#     ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
# )
# from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import (
#     fetch_keeper_network_gas_costs,
#     add_chainlink_upkeep_events_to_table,
# )
# from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import (
#     add_new_reward_token_swapped_events_to_table,
# )
# from mainnet_launch.pages.autopool_diagnostics.deposits_and_withdrawals import (
#     add_new_autopool_deposit_and_withdraw_events_to_table,
# )
# from mainnet_launch.pages.autopool_diagnostics.fetch_values_nav_and_shares_and_expenses import (
#     add_new_acutal_nav_and_acutal_shares_to_table,
# )
# from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
#     add_new_asset_oracle_and_discount_price_rows_to_table,
# )

# from mainnet_launch.constants import PRODUCTION_LOG_FILE_NAME, STARTUP_LOG_FILE

# import time
# import cProfile
# import pstats
# import io
# from functools import wraps
# from datetime import datetime
# import streamlit as st
# import os
# import re


# def log_usage(production_logger):
#     """
#     A decorator that profiles the decorated function using cProfile and logs
#     the total execution time and a detailed timing breakdown (top 20 entries)
#     using the production logger.
#     """

#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             start_time = time.time()
#             start_datetime = datetime.fromtimestamp(start_time).isoformat()

#             # Profile the function execution.
#             profiler = cProfile.Profile()
#             profiler.enable()
#             result = func(*args, **kwargs)
#             profiler.disable()

#             elapsed = time.time() - start_time

#             stream = io.StringIO()
#             stats = pstats.Stats(profiler, stream=stream)
#             stats.sort_stats("cumulative")
#             stats.print_stats(10, "mainnet_launch")

#             with open(STARTUP_LOG_FILE, "a") as f:
#                 f.write(f"{func.__name__} started at {start_datetime} and took {elapsed:.2f} seconds\n")
#                 f.write("Detailed function timing breakdown:\v n")
#                 f.write(stream.getvalue())
#                 f.write("\n" + "=" * 80 + "\n")

#             return result

#         return wrapper

#     return decorator


# functions_to_run = [
#     initialize_tx_hash_to_gas_info_db,
#     ensure_table_to_last_updated_exists,
#     add_new_destination_details_for_each_chain_to_table,
#     add_new_nav_per_share_to_table,
#     add_new_destination_summary_stats_to_table,
#     add_new_rebalance_events_for_each_autopool_to_table,
#     add_new_fee_events_to_table,
#     add_new_debt_reporting_events_to_table,
#     ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
#     fetch_keeper_network_gas_costs,
#     add_new_reward_token_swapped_events_to_table,
#     add_new_autopool_deposit_and_withdraw_events_to_table,
#     add_new_acutal_nav_and_acutal_shares_to_table,
#     add_chainlink_upkeep_events_to_table,
#     add_new_asset_oracle_and_discount_price_rows_to_table,
# ]


# def first_run_of_db(production_logger):
#     status_container = st.empty()
#     # Read and display the complete production log.
#     if os.path.exists(STARTUP_LOG_FILE):
#         with open(STARTUP_LOG_FILE, "r") as log_file:
#             log_contents = log_file.read()
#         status_container.text(log_contents)
#     else:
#         status_container.text("Production log file not found.")

#     for func in functions_to_run:
#         log_usage(production_logger)(func)()
#         status_container = st.empty()
#         # Read and display the complete production log.
#         if os.path.exists(STARTUP_LOG_FILE):
#             with open(STARTUP_LOG_FILE, "r") as log_file:
#                 log_contents = log_file.read()
#             status_container.text(log_contents)
#         else:
#             status_container.text("Production log file not found.")


# # from datetime import datetime

# # import streamlit as st


# # from mainnet_launch.data_fetching.add_info_to_dataframes import initialize_tx_hash_to_gas_info_db

# # from mainnet_launch.database.should_update_database import ensure_table_to_last_updated_exists

# # from mainnet_launch.pages.key_metrics.fetch_nav_per_share import add_new_nav_per_share_to_table

# # from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
# #     add_new_destination_summary_stats_to_table,
# # )

# # from mainnet_launch.pages.rebalance_events.rebalance_events import add_new_rebalance_events_for_each_autopool_to_table


# # from mainnet_launch.destinations import add_new_destination_details_for_each_chain_to_table


# # from mainnet_launch.pages.protocol_level_profit_and_loss.fees import (
# #     add_new_fee_events_to_table,
# #     add_new_debt_reporting_events_to_table,
# # )


# # from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import (
# #     ensure_all_rebalance_plans_are_loaded_from_s3_bucket,
# # )

# # from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import fetch_keeper_network_gas_costs
# # from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import (
# #     add_new_reward_token_swapped_events_to_table,
# # )


# # from mainnet_launch.pages.autopool_diagnostics.deposits_and_withdrawals import (
# #     add_new_autopool_deposit_and_withdraw_events_to_table,
# # )

# # from mainnet_launch.pages.autopool_diagnostics.fetch_values_nav_and_shares_and_expenses import (
# #     add_new_acutal_nav_and_acutal_shares_to_table,
# # )

# # from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import add_chainlink_upkeep_events_to_table


# # from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
# #     add_new_asset_oracle_and_discount_price_rows_to_table,
# # )


# # def first_run_of_db(production_logger):
# #     """
# #     Create and populate the database with data up to the current day.
# #     Only runs once, at the start of the application.
# #     """
# #     status_container = st.empty()

# #     messages = []

# #     def log_and_display(message):
# #         """Helper to log message, add a timestamp, and display it on the webpage."""
# #         timestamped_message = f"{datetime.now().isoformat()} - {message}"
# #         messages.append(timestamped_message)
# #         production_logger.info(timestamped_message)
# #         status_container.text("\n".join(messages))

# #     log_and_display("Starting first_run_of_db process.")

# #     try:
# #         log_and_display("Initializing transaction hash to gas info database...")
# #         initialize_tx_hash_to_gas_info_db()
# #         log_and_display("Transaction hash to gas info database initialized.")

# #         log_and_display("Ensuring table for last updated timestamp exists...")
# #         ensure_table_to_last_updated_exists()
# #         log_and_display("Table for last updated timestamp ensured.")

# #         log_and_display("Adding new destination details for each chain to table...")
# #         add_new_destination_details_for_each_chain_to_table()
# #         log_and_display("New destination details added.")

# #         log_and_display("Adding new NAV per share to table...")
# #         add_new_nav_per_share_to_table()
# #         log_and_display("New NAV per share added.")

# #         log_and_display("Adding new destination summary stats to table...")
# #         add_new_destination_summary_stats_to_table()
# #         log_and_display("New destination summary stats added.")

# #         log_and_display("Adding new rebalance events for each autopool to table...")
# #         add_new_rebalance_events_for_each_autopool_to_table()
# #         log_and_display("New rebalance events added.")

# #         log_and_display("Adding new fee events to table...")
# #         add_new_fee_events_to_table()
# #         log_and_display("New fee events added.")

# #         log_and_display("Adding new debt reporting events to table...")
# #         add_new_debt_reporting_events_to_table()
# #         log_and_display("New debt reporting events added.")

# #         log_and_display("Ensuring all rebalance plans are loaded from S3 bucket...")
# #         ensure_all_rebalance_plans_are_loaded_from_s3_bucket()
# #         log_and_display("All rebalance plans loaded.")

# #         log_and_display("Fetching keeper network gas costs...")
# #         fetch_keeper_network_gas_costs()  # need to be cached? maybe or put together from cached calls
# #         log_and_display("Keeper network gas costs fetched.")

# #         log_and_display("Fetching reward token Swapped events")
# #         add_new_reward_token_swapped_events_to_table()
# #         log_and_display("New reward token swapped events added.")

# #         log_and_display("Fetching autopool deposit and withdraw events...")
# #         add_new_autopool_deposit_and_withdraw_events_to_table()
# #         log_and_display("New autopool deposit and withdraw added")

# #         log_and_display("Fetching Autopool actual nav and actual shares...")
# #         add_new_acutal_nav_and_acutal_shares_to_table()
# #         log_and_display("New autopool actual nav and actual shares added")

# #         log_and_display("Chainlink Keeper Network Gas Costs...")
# #         add_chainlink_upkeep_events_to_table()
# #         log_and_display("Chainlink Keeper Network Gas Costs added")

# #         log_and_display("Asset Backing and Oracle Price...")
# #         add_new_asset_oracle_and_discount_price_rows_to_table()
# #         log_and_display("Asset Backing and Oracle Price added")

# #     except Exception as e:
# #         error_msg = f"Error during first_run_of_db: {e}"
# #         production_logger.error(error_msg, exc_info=True)
# #         log_and_display(error_msg)
# #         raise
# #     finally:
# #         final_msg = "first_run_of_db process completed"
# #         log_and_display(final_msg)


# # if __name__ == "__main__":
# #     import logging

# #     production_logger = logging.getLogger("production_logger")
# #     production_logger.setLevel(logging.INFO)

# #     # Only add the handler if it doesn't already exist
# #     if not production_logger.hasHandlers():
# #         handler = logging.FileHandler("first run in debug mode.txt", mode="w")
# #         handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
# #         production_logger.addHandler(handler)
# #         production_logger.propagate = False

# #     first_run_of_db(production_logger)
