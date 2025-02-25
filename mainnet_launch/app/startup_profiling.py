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
# import time
# import cProfile
# import pstats
# import io
# from functools import wraps
# import datetime

# def log_usage(production_logger):
#     """
#     A decorator that profiles the decorated function using cProfile and logs
#     the total execution time and a detailed timing breakdown (top 20 entries)
#     using the production logger.
#     """
#     def decorator(func):
#         @wraps(func)
#         def wrapper(*args, **kwargs):
#             # Start measuring total time.
#             start_time = time.time()
#             start_datetime = datetime.fromtimestamp(start_time).isoformat()


#             # Profile the function execution.
#             profiler = cProfile.Profile()
#             profiler.enable()
#             result = func(*args, **kwargs)
#             profiler.disable()

#             elapsed = time.time() - start_time

#             # Capture profiling statistics.
#             stream = io.StringIO()
#             stats = pstats.Stats(profiler, stream=stream)
#             stats.sort_stats("cumulative")
#             stats.print_stats(20)  # Display top 20 time-consuming entries.

#             # Log the results using the production logger.
#             production_logger.info(f"{func.__name__} started at {start_datetime} and took {elapsed:.2f} seconds")

#             production_logger.info("Detailed function timing breakdown:")
#             production_logger.info(stream.getvalue())

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

# if __name__ == "__main__":
#     OUTPUT_LOG_FILE = "usage_log.txt"

#     for func in functions_to_run:
#         wrapped_f = log_usage(output_file=OUTPUT_LOG_FILE)(func)
#         wrapped_f()
