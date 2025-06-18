"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambada function (or as needed) to update the dashboard pulling from the db

"""

from datetime import datetime
from pprint import pprint

from mainnet_launch.database.schema.full import drop_and_full_rebuild_db, ENGINE
from mainnet_launch.data_fetching.block_timestamp import ensure_blocks_is_current


from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_destinations_table import (
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


def ensure_database_is_current(full_reset_and_refetch: bool = False, echo_sql_to_console: bool = True):
    ENGINE.echo = echo_sql_to_console

    # if full_reset_and_refetch:
    #     drop_and_full_rebuild_db()
    time_taken = {}
    for i, func in [
        ensure_blocks_is_current(),
        ensure_autopools_are_current(),
        ensure__destinations__tokens__and__destination_tokens_are_current(),  # I don't like this name
        ensure_destination_states_from_rebalance_plan_are_current(),
        ensure_destination_states_are_current(),
        ensure_destination_token_values_are_current(),
        ensure_autopool_destination_states_are_current(),
        ensure_autopool_states_are_current(),
        ensure_token_values_are_current(),
        ensure_rebalance_plans_table_are_current(),
        ensure_rebalance_events_are_current(),
    ]:
        start = datetime.now()
        func()
        fin = datetime.now() - start
        time_taken[func.__name__] = fin
        print(func.__name__, fin)

    pprint(time_taken)

    # rebalance events

    # self contained parts add later

    # add after autoUSD
    # IncentiveTokenLiquidations   # AutopoolWithdrawal
    # AutopoolDeposit
    # chainlink gas costs
    # solver profit ( maybe exclude for complexity reasons, and solver profit is near 0)
    # debt reporting

    # last time database made to be current,

    # add to schema (maybe there is a way to store as one row instead of many)
    # tx_hash, asset, amount, to_user_address, from, (primary key serial (auto incrementing))
    # some person takes assets out, ETH, 100, bob, autopool
    # some person takes assets out, curve LP tokens, 20, bob
    # tx_hash, aave aWETH, 20, bob

    # has it at least an hour


def main():
    ensure_database_is_current(full_reset_and_refetch=False, echo_sql_to_console=False)


if __name__ == "__main__":
    main()
