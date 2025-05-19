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
# @time_decorator
def ensure_database_is_current(full_reset_and_refetch: bool = False, echo_sql_to_console: bool = True):
    ENGINE.echo = echo_sql_to_console
    # top level 6 hour check
    if full_reset_and_refetch:
        drop_and_full_rebuild_db()

    ensure_blocks_is_current()
    ensure_autopools_are_current()
    ensure__destinations__tokens__and__destination_tokens_are_current()  # I don't like this name

    ensure_destination_states_are_current()
    update_destination_states_from_rebalance_plan()  # duplicates work
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
    ensure_database_is_current(full_reset_and_refetch=True, echo_sql_to_console=True)


if __name__ == "__main__":
    profiler = cProfile.Profile()
    profiler.enable()
    main()
    profiler.disable()

    stats = pstats.Stats(profiler)
    stats.strip_dirs()  # remove extraneous path info
    stats.sort_stats("cumtime")  # sort by cumulative time
    stats.print_stats(50)  # show top 50 lines

# 2025-05-15 14:05:02,114 INFO sqlalchemy.engine.Engine [cached since 251.7s ago] {}
# 2025-05-15 14:05:02,326 INFO sqlalchemy.engine.Engine COMMIT
# ensure_database_is_current took 461.2069 seconds.
#          892637730 function calls (874839592 primitive calls) in 460.898 seconds

#    Ordered by: cumulative time
#    List reduced from 5594 to 50 due to restriction <50>

#    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
#         1    0.000    0.000  461.204  461.204 ensure_all_tables_are_current.py:105(main)
#         1    0.000    0.000  461.204  461.204 constants.py:179(wrapper)
#         1    0.021    0.021  461.204  461.204 ensure_all_tables_are_current.py:58(ensure_database_is_current)
#        42    0.001    0.000  296.073    7.049 nest_asyncio.py:25(run)
#        42    0.040    0.001  296.069    7.049 nest_asyncio.py:86(run_until_complete)
#     73989    0.895    0.000  296.020    0.004 nest_asyncio.py:100(_run_once)
#        29    0.000    0.000  282.275    9.734 get_state_by_block.py:70(get_raw_state_by_blocks)
#   1336028    0.303    0.000  153.362    0.000 events.py:78(_run)
#   1336028    0.235    0.000  153.058    0.000 {method 'run' of '_contextvars.Context' objects}
#     73989    0.144    0.000  141.143    0.002 selectors.py:554(select)
#    147835  141.021    0.001  141.021    0.001 {method 'control' of 'select.kqueue' objects}
#    610351    0.748    0.000  139.057    0.000 tasks.py:215(__step)
#    608499    0.131    0.000  137.164    0.000 {method 'send' of 'coroutine' objects}
#    127815    0.106    0.000  128.112    0.001 multicall.py:78(fetch_outputs)
#    142799    0.117    0.000  127.853    0.001 get_state_by_block.py:134(_fetch_data)
#         1    0.026    0.026  126.598  126.598 update_destinations_states_table.py:447(ensure_destination_states_are_current)
#         2    0.008    0.004  126.560   63.280 update_destinations_states_table.py:339(_add_new_destination_states_to_db)
#    428494    0.126    0.000  115.611    0.000 utils.py:77(run_in_subprocess)
#    393463    0.143    0.000  104.231    0.000 signature.py:73(encode_data)
#    274273    0.208    0.000  104.048    0.000 codec.py:94(encode)
# 3220091/274273    0.616    0.000  103.064    0.000 encoding.py:97(__call__)
# 656166/274273    1.634    0.000  102.992    0.000 encoding.py:138(encode)
#         1    0.033    0.033   94.516   94.516 update_token_values_table.py:123(ensure_token_values_are_current)
#         3    0.020    0.007   94.455   31.485 update_token_values_table.py:77(_fetch_and_insert_new_token_values)
#    144849    0.072    0.000   86.832    0.001 tasks.py:302(__wakeup)
# 5983817/3513273    1.624    0.000   86.480    0.000 address.py:35(is_address)
#   2481420    0.383    0.000   84.854    0.000 encoding.py:469(validate_value)
#    110306    0.040    0.000   83.565    0.001 eth_retry.py:78(auto_retry_wrap_async)
#    110306    0.087    0.000   83.492    0.001 call.py:108(coroutine)
#   2523341    1.307    0.000   80.014    0.000 address.py:128(is_checksum_address)
#   2529549    3.203    0.000   77.743    0.000 address.py:106(to_checksum_address)
#         2    0.001    0.000   77.513   38.756 update_destinations_states_table.py:246(_fetch_destination_summary_stats_df)
#         3    0.000    0.000   76.316   25.439 update_token_values_table.py:197(_fetch_safe_and_backing_values)
#       465   73.289    0.158   73.314    0.158 {method 'execute' of 'psycopg2.extensions.cursor' objects}
#     17563    0.015    0.000   64.271    0.004 call.py:130(prep_args)
#         1    0.008    0.008   61.899   61.899 update_destination_states_from_rebalance_plan.py:104(update_destination_states_from_rebalance_plan)
#         1    0.024    0.024   59.915   59.915 update_destination_token_values_tables.py:245(ensure_destination_token_values_are_current)
#         3    0.017    0.006   59.873   19.958 update_destination_token_values_tables.py:142(_fetch_and_insert_destination_token_values)
# 1419952/1038059    1.243    0.000   58.817    0.000 encoding.py:115(validate_value)
#     17563    0.019    0.000   49.752    0.003 encoding.py:720(encode)
#     17563    0.100    0.000   49.674    0.003 encoding.py:615(encode_elements)
#        42    0.033    0.001   47.811    1.138 postgres_operations.py:95(insert_avoid_conflicts)
#        42    0.008    0.000   46.786    1.114 postgres_operations.py:126(bulk_copy_skip_duplicates)
#         5    0.009    0.002   44.820    8.964 block_timestamp.py:123(ensure_all_blocks_are_in_table)
#   2164469    1.082    0.000   40.209    0.000 encoding.py:207(encode)
#         3    0.000    0.000   40.199   13.400 update_destination_token_values_tables.py:91(_fetch_destination_token_value_data_from_external_source)
#     17509    0.007    0.000   40.123    0.002 multicall.py:20(get_args)
#     17509    0.074    0.000   40.116    0.002 multicall.py:23(<listcomp>)
#    375900    0.066    0.000   40.042    0.000 call.py:52(data)
#    399456    0.086    0.000   35.080    0.000 encoding.py:619(<genexpr>)


# second go

# 2025-05-15 14:07:00,463 INFO sqlalchemy.engine.Engine COMMIT
# ensure_database_is_current took 82.9656 seconds.
#          28244790 function calls (28075784 primitive calls) in 82.689 seconds

#    Ordered by: cumulative time
#    List reduced from 4780 to 50 due to restriction <50>

#    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
#         1    0.000    0.000   82.965   82.965 ensure_all_tables_are_current.py:105(main)
#         1    0.000    0.000   82.965   82.965 constants.py:179(wrapper)
#         1    0.013    0.013   82.965   82.965 ensure_all_tables_are_current.py:58(ensure_database_is_current)
#       195   31.413    0.161   31.420    0.161 {method 'execute' of 'psycopg2.extensions.cursor' objects}
#        36    0.002    0.000   25.219    0.701 postgres_operations.py:243(get_full_table_as_orm)
#         1    0.006    0.006   21.188   21.188 update_destination_states_from_rebalance_plan.py:104(update_destination_states_from_rebalance_plan)
#        23    0.000    0.000   20.237    0.880 postgres_operations.py:199(get_subset_not_already_in_column)
#        85    0.001    0.000   19.365    0.228 base.py:1788(_execute_context)
#        85    0.004    0.000   19.345    0.228 base.py:1847(_exec_single_context)
#        85    0.000    0.000   19.280    0.227 default.py:944(do_execute)
#        82    0.000    0.000   18.994    0.232 elements.py:514(_execute_on_connection)
#        82    0.002    0.000   18.994    0.232 base.py:1588(_execute_clauseelement)
#        61    0.001    0.000   18.959    0.311 base.py:1372(execute)
#        40    0.001    0.000   18.633    0.466 session.py:2138(_execute_internal)
#        38    0.000    0.000   18.051    0.475 session.py:2305(execute)
#        45    0.001    0.000   16.120    0.358 connectionpool.py:592(urlopen)
#        45    0.001    0.000   16.108    0.358 connectionpool.py:377(_make_request)
#         1    0.007    0.007   16.009   16.009 update_rebalance_plans.py:194(ensure_rebalance_plans_table_are_current)
#         1    0.004    0.004   14.747   14.747 block_timestamp.py:38(_fetch_block_df_from_subgraph)
#         1    0.003    0.003   14.410   14.410 update_destinations_table.py:293(ensure__destinations__tokens__and__destination_tokens_are_current)
#        38    0.001    0.000   13.261    0.349 sessions.py:500(request)
#        38    0.002    0.000   13.181    0.347 sessions.py:673(send)
#        38    0.001    0.000   13.159    0.346 adapters.py:613(send)
#       394    0.001    0.000   11.104    0.028 socket.py:691(readinto)
#       466   11.102    0.024   11.102    0.024 {method 'read' of '_ssl._SSLSocket' objects}
#       394    0.001    0.000   11.102    0.028 ssl.py:1296(recv_into)
#       394    0.001    0.000   11.100    0.028 ssl.py:1154(read)
#        24    0.003    0.000   10.270    0.428 postgres_operations.py:95(insert_avoid_conflicts)
#        22    0.001    0.000   10.236    0.465 api.py:103(post)
#        22    0.001    0.000   10.236    0.465 api.py:14(request)
#        21    0.001    0.000   10.225    0.487 postgres_operations.py:25(merge_tables_as_df)
#        21    0.007    0.000   10.206    0.486 sql.py:570(read_sql)
#        12    0.002    0.000   10.159    0.847 postgres_operations.py:126(bulk_copy_skip_duplicates)
#   223/101    0.002    0.000    9.949    0.099 state_changes.py:95(_go)
#         1    0.001    0.001    8.819    8.819 update_autopool_destination_states_table.py:194(ensure_autopool_destination_states_are_current)
#         3    0.000    0.000    8.818    2.939 update_autopool_destination_states_table.py:59(_fetch_and_insert_new_autopool_destination_states)
#        73    0.000    0.000    8.737    0.120 base.py:3276(raw_connection)
#        73    0.000    0.000    8.737    0.120 base.py:441(connect)
#        73    0.002    0.000    8.737    0.120 base.py:1256(_checkout)
#        45    0.000    0.000    8.219    0.183 connectionpool.py:1085(_validate_conn)
#        31    0.002    0.000    8.219    0.265 connection.py:669(connect)
#         1    0.000    0.000    7.998    7.998 update_token_values_table.py:123(ensure_token_values_are_current)
#         3    0.000    0.000    7.998    2.666 update_token_values_table.py:77(_fetch_and_insert_new_token_values)
#         3    0.049    0.016    7.998    2.666 update_token_values_table.py:48(_determine_what_blocks_are_needed)
#        45    0.003    0.000    7.860    0.175 connection.py:485(getresponse)
#        45    0.001    0.000    7.843    0.174 client.py:1331(getresponse)
#        45    0.002    0.000    7.840    0.174 client.py:311(begin)
#       779    0.001    0.000    7.828    0.010 {method 'readline' of '_io.BufferedReader' objects}
#        45    0.001    0.000    7.810    0.174 client.py:278(_read_status)
#        61    0.000    0.000    7.603    0.125 base.py:3251(connect)
