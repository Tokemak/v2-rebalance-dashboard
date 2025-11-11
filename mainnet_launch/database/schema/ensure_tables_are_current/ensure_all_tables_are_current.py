"""

Top line script that updates the database to the current time
Run this after midnight UTC via github actions to keep the database current

open questions:

- Is there any way I can get a single boolean, (maybe for each autopool) that is:
- autopool is current
- that should save a lot of checks to see if we have all the needed blocks

"""

from concurrent.futures import ThreadPoolExecutor
from mainnet_launch.constants import ALL_CHAINS, profile_function
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


def _ensure_chain_top_block_are_cached():
    for chain in ALL_CHAINS:
        print(
            f"Universe: {chain.name} start_block={chain.block_autopool_first_deployed}, end_block={chain.get_block_near_top()}"
        )


def _setup_constants():
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            # ensure_blocks_is_current is slow because it makes a http call for each 2 x day x chain. each time, almost all are redundent
            executor.submit(ensure_blocks_is_current),
            executor.submit(ensure_autopools_are_current),
            # ensure__destinations__tokens__and__destination_tokens_are_current is slow because it doesn't check what destinations are already added
            executor.submit(ensure__destinations__tokens__and__destination_tokens_are_current),
        ]
        # Block until all complete; any exception will propagate and crash
        for f in futures:
            f.result()


def _fully_independent_update_functions():
    """
    These don't depend on anything else, so can be run in parallel with other things.

    currently running in order becuase the other parts are slow and even if this takes a bit longer, it doesn't matter
    """

    ensure_tokemak_EOA_gas_costs_are_current()
    ensure_chainlink_gas_costs_table_are_current()
    ensure_autopool_fees_are_current()

    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_balance_updated_is_current()
    ensure_incentive_token_prices_are_current()

    ensure_autopool_transfers_are_current()
    ensure_autopool_deposits_are_current()
    ensure_autopool_withdraws_are_current()
    ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit()  # depends on autopool deposits and withdraws


def _independent_after_constants():
    ensure_destination_underlying_deposits_are_current()  # depends on destinations
    ensure_destination_underlying_withdraw_are_current()  # depends on destinations


def _sequential_after_constants():
    ensure_destination_states_from_rebalance_plan_are_current()  # big
    ensure_destination_states_are_current()
    ensure_destination_token_values_are_current()
    ensure_autopool_destination_states_are_current()
    ensure_autopool_states_are_current()
    ensure_token_values_are_current()

    ensure_rebalance_plans_table_are_current()  # big
    ensure_rebalance_events_are_current()


def ensure_database_is_current(echo_sql_to_console: bool = False):
    ENGINE.echo = echo_sql_to_console
    _ensure_chain_top_block_are_cached()
    with ThreadPoolExecutor(max_workers=5) as executor:

        constants_task = executor.submit(_setup_constants)
        fully_independent_task = executor.submit(_fully_independent_update_functions)
        constants_task.result()

        independent_after_constants_task = executor.submit(_independent_after_constants)
        sequential_after_constants_task = executor.submit(_sequential_after_constants)

        sequential_after_constants_task.result()
        fully_independent_task.result()
        independent_after_constants_task.result()


def ensure_database_is_current_slow_and_sequential(echo_sql_to_console: bool = False):
    ENGINE.echo = echo_sql_to_console
    _ensure_chain_top_block_are_cached()

    ensure_blocks_is_current()
    ensure_autopools_are_current()
    ensure__destinations__tokens__and__destination_tokens_are_current()

    ensure_tokemak_EOA_gas_costs_are_current()
    ensure_chainlink_gas_costs_table_are_current()
    ensure_autopool_fees_are_current()

    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_balance_updated_is_current()
    ensure_incentive_token_prices_are_current()

    ensure_destination_underlying_deposits_are_current()
    ensure_destination_underlying_withdraw_are_current()

    ensure_destination_states_from_rebalance_plan_are_current()
    ensure_destination_states_are_current()
    ensure_destination_token_values_are_current()
    ensure_autopool_destination_states_are_current()
    ensure_autopool_states_are_current()
    ensure_token_values_are_current()

    ensure_rebalance_plans_table_are_current()
    ensure_rebalance_events_are_current()

    ensure_autopool_transfers_are_current()
    ensure_autopool_deposits_are_current()
    ensure_autopool_withdraws_are_current()
    ensure_an_autopool_state_exists_for_each_autopool_withdrawal_or_deposit()


def sequential_main():
    # ensure_database_is_current_slow_and_sequential()
    profile_function(ensure_database_is_current_slow_and_sequential, echo_sql_to_console=False)


def main():
    profile_function(ensure_database_is_current, echo_sql_to_console=False)


# if __name__ == "__main__":
#     sequential_main()
