"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambda function (or as needed) to update the dashboard pulling from the db

open questions:

- Is there any way I can get a single boolean, (maybe for each autopool) that is:
- autopool is current
- that should save a lot of checks to see if we have all the needed blocks

"""

from concurrent.futures import ThreadPoolExecutor

from mainnet_launch.constants import ALL_CHAINS

from mainnet_launch.database.schema.full import ENGINE
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_blocks import (
    ensure_blocks_is_current,
)
from mainnet_launch.constants import profile_function

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure__destinations__tokens__and__destination_tokens_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_autopools_table import (
    ensure_autopools_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_token_values_table import (
    ensure_token_values_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destination_token_values_tables import (
    ensure_destination_token_values_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_destinations_states_table import (
    ensure_destination_states_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_autopool_destination_states_table import (
    ensure_autopool_destination_states_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.order_dependent.update_autopool_states import (
    ensure_autopool_states_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_rebalance_plans.update_destination_states_from_rebalance_plan import (
    ensure_destination_states_from_rebalance_plan_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_rebalance_plans.update_rebalance_plans import (
    ensure_rebalance_plans_table_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_rebalance_plans.update_rebalance_events import (
    ensure_rebalance_events_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_gas_costs.update_transactions_table_for_gas_costs import (
    update_tokemak_EOA_gas_costs_from_0,
    update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_gas_costs.update_chainlink_keeper_gas_costs_table import (
    ensure_chainlink_gas_costs_table_is_updated,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_destinations.update_destination_underlying_deposited import (
    ensure_destination_underlying_deposits_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_destinations.update_destination_underlying_withdraw import (
    ensure_destination_underlying_withdraw_are_current,
)


from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_incentives.update_incentive_token_sales import (
    ensure_incentive_token_swapped_events_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_incentives.update_incentive_token_prices import (
    ensure_incentive_token_prices_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools import (
    ensure_autopool_fees_are_current,
    ensure_autopool_deposits_are_current,
    ensure_autopool_transfers_are_current,
    ensure_autopool_withdraws_are_current,
)



# from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools.update_autopool_fees import (
#     ensure_autopool_fees_are_current,
# )


# from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools.update_autopool_vault_token_transfers import (
#     ensure_autopool_transfers_are_current,
# )

# from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools.update_autopool_vault_deposits import (
#     ensure_autopool_deposits_are_current,
# )

# from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.about_autopools.update_autopool_vault_withdraws import (
#     ensure_autopool_withdraws_are_current,
# )


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

    update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched()
    ensure_chainlink_gas_costs_table_is_updated()
    ensure_autopool_fees_are_current()
    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_prices_are_current()
    ensure_autopool_transfers_are_current()
    ensure_autopool_deposits_are_current()
    ensure_autopool_withdraws_are_current()


def _independent_after_constants():
    ensure_destination_underlying_deposits_are_current()  # depends on destinations
    ensure_destination_underlying_withdraw_are_current()  # depends on destinations


def _sequential_after_constants():
    ensure_destination_states_from_rebalance_plan_are_current()  # big,
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

    update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched()
    ensure_chainlink_gas_costs_table_is_updated()
    ensure_autopool_fees_are_current()

    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_prices_are_current()

    ensure_destination_underlying_deposits_are_current()  # depends on destinations
    ensure_destination_underlying_withdraw_are_current()  #  depends on destinations

    ensure_destination_states_from_rebalance_plan_are_current()  # big, 33 seconds, with just a few new plans
    ensure_destination_states_are_current()  # .3 seconds
    ensure_destination_token_values_are_current()  # 30 seconds not in parallel,
    ensure_autopool_destination_states_are_current()  # maybe can be parrallel? 1.5 per autopool not in parallel
    ensure_autopool_states_are_current()  # faster with threads
    ensure_token_values_are_current()  # 30 seconds not in parallel

    ensure_rebalance_plans_table_are_current()  # big, 23 seconds on empty, does not early exit properly
    ensure_rebalance_events_are_current()  # slow, not optimized

    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_prices_are_current()


    ensure_autopool_fees_are_current()

    ensure_autopool_transfers_are_current()
    ensure_autopool_deposits_are_current()
    ensure_autopool_withdraws_are_current()


def sequential_main():
    profile_function(ensure_database_is_current_slow_and_sequential, echo_sql_to_console=False)


def main():
    profile_function(ensure_database_is_current, echo_sql_to_console=False)


if __name__ == "__main__":
    main()