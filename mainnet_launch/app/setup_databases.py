"""Runs once in the lifecycle of the app"""

from mainnet_launch.data_fetching.add_info_to_dataframes import initalize_tx_hash_to_gas_info_db
from mainnet_launch.database.should_update_database import ensure_table_to_last_updated_exists

from mainnet_launch.pages.key_metrics.fetch_nav_per_share import add_new_nav_per_share_to_table

from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import (
    add_new_destination_summary_stats_to_table,
)

from mainnet_launch.pages.rebalance_events.fetch_rebalance_events import add_new_rebalance_events_for_each_autopool_to_table


from mainnet_launch.destinations import add_new_destination_details_for_each_chain_to_table


from mainnet_launch.pages.protocol_level_profit_and_loss.fees import add_new_fee_events_to_table, add_new_debt_reporting_events_to_table


from mainnet_launch.pages.solver_diagnostics.solver_diagnostics import ensure_all_rebalance_plans_are_loaded_from_s3_bucket

from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import fetch_keeper_network_gas_costs
from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import add_new_reward_token_swapped_events_to_table

from mainnet_launch.constants import DB_FILE, time_decorator


@time_decorator
def first_run_of_db():
    """

    Create and populate the database with data up to the current day
    only ran once, at the start of the application.

    """
    # resets the autopool dashboard db and makes sure it exists
    with open(DB_FILE, "w") as _:
        pass

    initalize_tx_hash_to_gas_info_db()
    ensure_table_to_last_updated_exists()  # creates autopool_dashboard.db

    add_new_destination_details_for_each_chain_to_table()
    add_new_nav_per_share_to_table()
    add_new_destination_summary_stats_to_table()
    add_new_rebalance_events_for_each_autopool_to_table()
    add_new_fee_events_to_table()
    add_new_debt_reporting_events_to_table()

    # reads from s3 bucket
    ensure_all_rebalance_plans_are_loaded_from_s3_bucket()

    fetch_keeper_network_gas_costs()
    # this does not create a new table but instead
    # adds block timestamp and gas costs data for the chainlink keeper upkeep contracts

    add_new_reward_token_swapped_events_to_table()


if __name__ == "__main__":
    first_run_of_db()
