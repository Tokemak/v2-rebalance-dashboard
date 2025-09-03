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
from mainnet_launch.data_fetching.block_timestamp import ensure_blocks_is_current
from mainnet_launch.constants import profile_function

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destinations_tokens_and_autopoolDestinations_table import (
    ensure__destinations__tokens__and__destination_tokens_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_autopools_table import (
    ensure_autopools_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_token_values_table import (
    ensure_token_values_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destination_token_values_tables import (
    ensure_destination_token_values_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destinations_states_table import (
    ensure_destination_states_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_autopool_destination_states_table import (
    ensure_autopool_destination_states_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_autopool_states import (
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

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_transactions_table_for_gas_costs import (
    update_tokemak_EOA_gas_costs_from_0,
    update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_chainlink_keeper_gas_costs_table import (
    ensure_chainlink_gas_costs_table_is_updated,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_destination_underlying_deposited import (
    ensure_destination_underlying_deposits_are_current,
)
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_destination_underlying_withdraw import (
    ensure_destination_underlying_withdraw_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_autopool_fees import (
    ensure_autopool_fees_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_incentive_token_sales import (
    ensure_incentive_token_swapped_events_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_incentive_token_prices import (
    ensure_incentive_token_prices_are_current,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.not_order_dependent.update_autopool_vault_token_transfers import (
    ensure_autopool_transfers_are_current,
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

    update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched()
    ensure_chainlink_gas_costs_table_is_updated()
    ensure_autopool_fees_are_current()
    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_prices_are_current()
    ensure_autopool_transfers_are_current()


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

    ensure_autopool_fees_are_current()
    ensure_incentive_token_swapped_events_are_current()
    ensure_incentive_token_prices_are_current()
    ensure_autopool_transfers_are_current()


def sequential_main():
    profile_function(ensure_database_is_current_slow_and_sequential, echo_sql_to_console=False)


def main():
    profile_function(ensure_database_is_current, echo_sql_to_console=False)


if __name__ == "__main__":
    main()

# aug 26

# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#    101                                           def ensure_database_is_current(echo_sql_to_console: bool = False):
#    102         1          0.0      0.0      0.0      ENGINE.echo = echo_sql_to_console
#    103
#    104         2          0.0      0.0      0.0      with ThreadPoolExecutor(max_workers=5) as executor:
#    105         1          0.0      0.0      0.0          constants_task = executor.submit(_setup_constants)
#    106         1          0.0      0.0      0.0          fully_independent_task = executor.submit(_fully_independent_update_functions)
#    107         1         30.5     30.5     22.3          constants_task.result()
#    108
#    109         1          0.0      0.0      0.0          independent_after_constants_task = executor.submit(_independent_after_constants)
#    110         1          0.0      0.0      0.0          sequential_after_constants_task = executor.submit(_sequential_after_constants)
#    111
#    112         1        106.1    106.1     77.7          sequential_after_constants_task.result()
#    113
#    114         1          0.0      0.0      0.0          fully_independent_task.result()
#    115
#    116         1          0.0      0.0      0.0          independent_after_constants_task.result()


# Total time: 234.538 s
# File: /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/ensure_all_tables_are_current.py
# Function: ensure_database_is_current_old at line 118

# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#    118                                           def ensure_database_is_current_old(echo_sql_to_console: bool = False):
#    119         1          0.0      0.0      0.0      ENGINE.echo = echo_sql_to_console
#    120
#    121         1         13.9     13.9      5.9      ensure_blocks_is_current()
#    122         1          0.9      0.9      0.4      ensure_autopools_are_current()
#    123         1         16.4     16.4      7.0      ensure__destinations__tokens__and__destination_tokens_are_current()
#    124
#    125         1         10.1     10.1      4.3      ensure_destination_underlying_deposits_are_current()  # depends on destinations
#    126         1          8.6      8.6      3.7      ensure_destination_underlying_withdraw_are_current()  #  depends on destinations
#    127
#    128         1         13.7     13.7      5.8      ensure_destination_states_from_rebalance_plan_are_current()  # big,
#    129         1          2.3      2.3      1.0      ensure_destination_states_are_current()
#    130         1         33.9     33.9     14.4      ensure_destination_token_values_are_current()
#    131         1         19.0     19.0      8.1      ensure_autopool_destination_states_are_current()
#    132         1         10.7     10.7      4.5      ensure_autopool_states_are_current()
#    133         1         25.9     25.9     11.0      ensure_token_values_are_current()
#    134         1         16.9     16.9      7.2      ensure_rebalance_plans_table_are_current()  # big
#    135         1         34.9     34.9     14.9      ensure_rebalance_events_are_current()  # big ensure_rebalance_events_are_current
#    136
#    137         1         11.5     11.5      4.9      update_tokemak_EOA_gas_costs_based_on_highest_block_already_fetched()  # independent
#    138         1          6.0      6.0      2.6      ensure_chainlink_gas_costs_table_is_updated()  # idependent
#    139         1         10.0     10.0      4.3      ensure_autopool_fees_are_current()  # independent
