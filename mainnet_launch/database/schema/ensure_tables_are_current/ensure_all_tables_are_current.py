"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambada function (or as needed) to update the dashboard pulling from the db

"""

from mainnet_launch.constants import time_decorator


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
    update_destination_states_from_rebalance_plan,
)

from mainnet_launch.database.schema.ensure_tables_are_current.using_rebalance_plans.update_rebalance_plans import (
    ensure_rebalance_plans_table_are_current,
)


# ensure_database_is_current took 562.3635 seconds.
# swithcing all to 100 semaphore limit
# ensure_database_is_current took 337.8729 seconds. at 100 limit


# now checking 300 # ensure_database_is_current took 389.6745 seconds.
# 100 is fast enough


@time_decorator
def ensure_database_is_current(full_reset_and_refetch: bool = False, echo_sql_to_console: bool = True):
    ENGINE.echo = echo_sql_to_console
    # top level 6 hour check
    if full_reset_and_refetch:
        drop_and_full_rebuild_db()
    ensure_blocks_is_current()
    ensure_autopools_are_current()  #
    ensure__destinations__tokens__and__destination_tokens_are_current()  # I don't like this name

    ensure_destination_states_are_current()
    update_destination_states_from_rebalance_plan() # duplicates work
    ensure_destination_token_values_are_current()
    ensure_autopool_destination_states_are_current()

    ensure_autopool_states_are_current()

    ensure_token_values_are_current()

    ensure_rebalance_plans_table_are_current()

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

    # has it at least an hour,


import cProfile
import pstats


def main():
    ensure_database_is_current(full_reset_and_refetch=False, echo_sql_to_console=True)


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()

    stats = pstats.Stats(profiler)
    stats.strip_dirs()  # remove extraneous path info
    stats.sort_stats("cumtime")  # sort by cumulative time
    stats.print_stats(50)  # show top 50 lines


# without the multical prep args

# 2025-05-14 13:40:14,531 INFO sqlalchemy.engine.Engine [cached since 326.8s ago] {}
# 2025-05-14 13:40:14,715 INFO sqlalchemy.engine.Engine COMMIT
# ensure_database_is_current took 366.4500 seconds.
#          662609612 function calls (647811907 primitive calls) in 366.221 seconds

#    Ordered by: cumulative time
#    List reduced from 5560 to 50 due to restriction <50>

#    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
#         1    0.000    0.000  366.443  366.443 ensure_all_tables_are_current.py:107(main)
#      11/1    0.000    0.000  366.443  366.443 constants.py:179(wrapper)
#         1    0.012    0.012  366.443  366.443 ensure_all_tables_are_current.py:58(ensure_database_is_current)
#        36    0.000    0.000  246.431    6.845 nest_asyncio.py:25(run)
#        36    0.053    0.001  246.428    6.845 nest_asyncio.py:86(run_until_complete)
#    102311    0.979    0.000  246.363    0.002 nest_asyncio.py:100(_run_once)
#        26    0.000    0.000  242.661    9.333 get_state_by_block.py:70(get_raw_state_by_blocks)
#   1255044    0.319    0.000  126.424    0.000 events.py:78(_run)
#   1255044    0.257    0.000  126.105    0.000 {method 'run' of '_contextvars.Context' objects}
#    102311    0.182    0.000  118.252    0.001 selectors.py:554(select)
#    200436  118.119    0.001  118.119    0.001 {method 'control' of 'select.kqueue' objects}
#    576789    0.771    0.000  110.710    0.000 tasks.py:215(__step)
#    562272    0.131    0.000  108.680    0.000 {method 'send' of 'coroutine' objects}
#    151349    0.114    0.000   99.212    0.001 multicall.py:78(fetch_outputs)
#    166109    0.123    0.000   99.159    0.001 get_state_by_block.py:133(_fetch_data)
#         1    0.008    0.008   93.908   93.908 update_destination_states_from_rebalance_plan.py:104(update_destination_states_from_rebalance_plan)
#         1    0.034    0.034   92.087   92.087 update_destinations_states_table.py:455(ensure_destination_states_are_current)
#         2    0.012    0.006   92.040   46.020 update_destinations_states_table.py:342(_add_new_destination_states_to_db)
#    346883    0.104    0.000   85.633    0.000 utils.py:77(run_in_subprocess)
#    312375    0.112    0.000   75.406    0.000 signature.py:73(encode_data)
#    209744    0.140    0.000   75.262    0.000 codec.py:94(encode)
# 2450909/209744    0.483    0.000   74.529    0.000 encoding.py:97(__call__)
# 508248/209744    1.262    0.000   74.474    0.000 encoding.py:138(encode)
#         5    0.010    0.002   74.425   14.885 block_timestamp.py:123(ensure_all_blocks_are_in_table)
#    134100    0.050    0.000   71.635    0.001 eth_retry.py:78(auto_retry_wrap_async)
#    134100    0.101    0.000   71.554    0.001 call.py:108(coroutine)
#         1    0.033    0.033   69.524   69.524 update_destination_token_values_tables.py:267(ensure_destination_token_values_are_current)
#         3    0.023    0.008   69.467   23.156 update_destination_token_values_tables.py:131(_fetch_and_insert_destination_token_values)
#    180779    0.096    0.000   67.106    0.000 tasks.py:302(__wakeup)
# 4226585/2487337    1.186    0.000   62.256    0.000 address.py:35(is_address)
#   1744278    0.277    0.000   61.049    0.000 encoding.py:469(validate_value)
#   1791196    0.937    0.000   57.958    0.000 address.py:128(is_checksum_address)
#   1796245    2.355    0.000   56.621    0.000 address.py:106(to_checksum_address)
#       352   52.006    0.148   52.017    0.148 {method 'execute' of 'psycopg2.extensions.cursor' objects}
#     17292    0.016    0.000   51.606    0.003 call.py:130(prep_args)
#         3    0.000    0.000   48.603   16.201 update_destination_token_values_tables.py:50(_fetch_destination_token_value_data_from_external_source)
#         2    0.001    0.000   45.552   22.776 update_destinations_states_table.py:249(_fetch_destination_summary_stats_df)
# 1105256/806752    0.973    0.000   43.744    0.000 encoding.py:115(validate_value)
#         1    0.020    0.020   41.277   41.277 update_autopool_destination_states_table.py:187(ensure_autopool_destination_states_are_current)
#         3    0.010    0.003   41.240   13.747 update_autopool_destination_states_table.py:72(_fetch_and_insert_new_autopool_destination_states)
#     17292    0.018    0.000   39.829    0.002 encoding.py:720(encode)
#     17292    0.090    0.000   39.750    0.002 encoding.py:615(encode_elements)
#         3    0.008    0.003   36.666   12.222 block_timestamp.py:38(_fetch_block_df_from_subgraph)
#        60    0.001    0.000   31.172    0.520 sessions.py:500(request)
#        60    0.002    0.000   31.068    0.518 sessions.py:673(send)
#        61    0.001    0.000   31.063    0.509 connectionpool.py:592(urlopen)
#        61    0.001    0.000   31.049    0.509 connectionpool.py:377(_make_request)
#        60    0.001    0.000   30.680    0.511 adapters.py:613(send)
#        27    0.022    0.001   30.132    1.116 postgres_operations.py:95(insert_avoid_conflicts)
#        27    0.004    0.000   29.316    1.086 postgres_operations.py:126(bulk_copy_skip_duplicates)
