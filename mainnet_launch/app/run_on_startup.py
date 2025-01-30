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

from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import fetch_keeper_network_gas_costs
from mainnet_launch.pages.incentive_token_prices.incentive_token_liqudiation_prices import (
    add_new_reward_token_swapped_events_to_table,
)


from mainnet_launch.pages.autopool_diagnostics.deposits_and_withdrawals import (
    add_new_autopool_deposit_and_withdraw_events_to_table,
)

from mainnet_launch.pages.autopool_diagnostics.fetch_values_nav_and_shares_and_expenses import (
    add_new_acutal_nav_and_acutal_shares_to_table,
)

from mainnet_launch.pages.gas_costs.keeper_network_gas_costs import add_chainlink_upkeep_events_to_table


from mainnet_launch.pages.asset_discounts.fetch_and_render_asset_discounts import (
    add_new_asset_oracle_and_discount_price_rows_to_table,
)


def first_run_of_db(production_logger):
    """
    Create and populate the database with data up to the current day.
    Only runs once, at the start of the application.
    """
    status_container = st.empty()

    messages = []

    def log_and_display(message):
        """Helper to log message, add a timestamp, and display it on the webpage."""
        timestamped_message = f"{datetime.now().isoformat()} - {message}"
        messages.append(timestamped_message)
        production_logger.info(timestamped_message)
        status_container.text("\n".join(messages))

    log_and_display("Starting first_run_of_db process.")

    try:
        log_and_display("Initializing transaction hash to gas info database...")
        initialize_tx_hash_to_gas_info_db()
        log_and_display("Transaction hash to gas info database initialized.")

        log_and_display("Ensuring table for last updated timestamp exists...")
        ensure_table_to_last_updated_exists()
        log_and_display("Table for last updated timestamp ensured.")

        log_and_display("Adding new destination details for each chain to table...")
        add_new_destination_details_for_each_chain_to_table()
        log_and_display("New destination details added.")

        log_and_display("Adding new NAV per share to table...")
        add_new_nav_per_share_to_table()
        log_and_display("New NAV per share added.")

        log_and_display("Adding new destination summary stats to table...")
        add_new_destination_summary_stats_to_table()
        log_and_display("New destination summary stats added.")

        log_and_display("Adding new rebalance events for each autopool to table...")
        add_new_rebalance_events_for_each_autopool_to_table()
        log_and_display("New rebalance events added.")

        log_and_display("Adding new fee events to table...")
        add_new_fee_events_to_table()
        log_and_display("New fee events added.")

        log_and_display("Adding new debt reporting events to table...")
        add_new_debt_reporting_events_to_table()
        log_and_display("New debt reporting events added.")

        log_and_display("Ensuring all rebalance plans are loaded from S3 bucket...")
        ensure_all_rebalance_plans_are_loaded_from_s3_bucket()
        log_and_display("All rebalance plans loaded.")

        log_and_display("Fetching keeper network gas costs...")
        fetch_keeper_network_gas_costs()  # need to be cached? maybe or put together from cached calls
        log_and_display("Keeper network gas costs fetched.")

        log_and_display("Fetching reward token Swapped events")
        add_new_reward_token_swapped_events_to_table()
        log_and_display("New reward token swapped events added.")

        log_and_display("Fetching autopool deposit and withdraw events...")
        add_new_autopool_deposit_and_withdraw_events_to_table()
        log_and_display("New autopool deposit and withdraw added")

        log_and_display("Fetching Autopool actual nav and actual shares...")
        add_new_acutal_nav_and_acutal_shares_to_table()
        log_and_display("New autopool actual nav and actual shares added")

        log_and_display("Chainlink Keeper Network Gas Costs...")
        add_chainlink_upkeep_events_to_table()
        log_and_display("Chainlink Keeper Network Gas Costs added")

        log_and_display("Asset Backing and Oracle Price...")
        add_new_asset_oracle_and_discount_price_rows_to_table()
        log_and_display("Asset Backing and Oracle Price added")

    except Exception as e:
        error_msg = f"Error during first_run_of_db: {e}"
        production_logger.error(error_msg, exc_info=True)
        log_and_display(error_msg)
        raise
    finally:
        final_msg = "first_run_of_db process completed"
        log_and_display(final_msg)


if __name__ == "__main__":
    import logging

    production_logger = logging.getLogger("production_logger")
    production_logger.setLevel(logging.INFO)

    # Only add the handler if it doesn't already exist
    if not production_logger.hasHandlers():
        handler = logging.FileHandler("first run in debug mode.txt", mode="w")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
        production_logger.addHandler(handler)
        production_logger.propagate = False

    first_run_of_db(production_logger)
