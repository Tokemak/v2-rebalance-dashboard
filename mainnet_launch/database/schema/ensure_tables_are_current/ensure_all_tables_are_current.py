"""

Top line script that updates the database to the current time
Run this after midnight UTC via github actions to keep the database current

open questions:

- Is there any way I can get a single boolean, (maybe for each autopool) that is:
- autopool is current
- that should save a lot of checks to see if we have all the needed blocks

"""

import time


from mainnet_launch.constants import ALL_CHAINS, profile_function, WORKING_DATA_DIR
from mainnet_launch.database.schema.full import ENGINE

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_blocks_is_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_rebalance_plans import (
    ensure_destination_states_from_rebalance_plan_are_current,
    ensure_rebalance_plans_table_are_current,
    ensure_rebalance_events_are_current,
)


from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent import (
    ensure_autopool_destination_states_are_current,
    ensure_autopool_states_are_current,
    ensure_autopools_are_current,
    ensure_destination_token_values_are_current,
    ensure_destination_states_are_current,
    ensure__destinations__tokens__and__destination_tokens_are_current,
    ensure_token_values_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_incentives import (
    ensure_incentive_token_swapped_events_are_current,
    ensure_incentive_token_prices_are_current,
    ensure_incentive_token_balance_updated_is_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools import (
    ensure_autopool_fees_are_current,
    ensure_autopool_deposits_are_current,
    ensure_autopool_transfers_are_current,
    ensure_autopool_withdraws_are_current,
    ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_destinations import (
    ensure_destination_underlying_deposits_are_current,
    ensure_destination_underlying_withdraw_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_gas_costs import (
    ensure_tokemak_EOA_gas_costs_are_current,
    ensure_chainlink_gas_costs_table_are_current,
)


def ensure_database_is_current_slow_and_sequential(echo_sql_to_console: bool = False):
    ENGINE.echo = echo_sql_to_console

    run_path = "update-prod-db.txt"
    # note is fetching duplicate data somewhere, not sure where yet for sure in eunsure atupools
    steps = [
        ensure_blocks_is_current,
        ensure_autopools_are_current,
        ensure__destinations__tokens__and__destination_tokens_are_current,  # faster
        ensure_tokemak_EOA_gas_costs_are_current,
        ensure_chainlink_gas_costs_table_are_current,
        ensure_autopool_fees_are_current,  # faster
        ensure_incentive_token_swapped_events_are_current,  # faster
        ensure_incentive_token_balance_updated_is_current,
        ensure_incentive_token_prices_are_current,
        ensure_destination_underlying_deposits_are_current,
        ensure_destination_underlying_withdraw_are_current,
        ensure_destination_states_from_rebalance_plan_are_current,
        ensure_destination_states_are_current,
        ensure_destination_token_values_are_current,
        ensure_autopool_destination_states_are_current,
        ensure_autopool_states_are_current,
        ensure_token_values_are_current,
        ensure_rebalance_plans_table_are_current,
        ensure_rebalance_events_are_current,
        ensure_autopool_transfers_are_current,
        ensure_autopool_deposits_are_current,
        ensure_autopool_withdraws_are_current,
        ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit,
    ]

    overall_t0 = time.perf_counter()
    with open(run_path, "w", encoding="utf-8") as f:
        for func in steps:
            try:
                t0 = time.perf_counter()
                print(f"Starting step: {func.__name__}")
                profile_function(func)
                elapsed = time.perf_counter() - t0
                f.write(f"{func.__name__}, {elapsed:.6f}\n")
                f.flush()
            except Exception as e:
                f.write(f"{func.__name__}, ERROR: {str(e)}\n")
                f.flush()
                print(f"Error in step {func.__name__}: {str(e)}")

        f.write(f"TOTAL, {time.perf_counter() - overall_t0:.6f}\n")
        f.flush()

    print("finished update")


def sequential_main():
    # ensure_database_is_current_slow_and_sequential()
    profile_function(ensure_database_is_current_slow_and_sequential, echo_sql_to_console=False)


def main():
    # profile_function(ensure_database_is_current_slow_and_sequential, echo_sql_to_console=False)
    ensure_database_is_current_slow_and_sequential()


if __name__ == "__main__":
    main()
