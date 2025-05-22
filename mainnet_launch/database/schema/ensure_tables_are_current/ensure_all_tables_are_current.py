"""

Top line scirpt that updates the database to the current time
Run this, via a once a day lambada function (or as needed) to update the dashboard pulling from the db

"""

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

def ensure_database_is_current(full_reset_and_refetch: bool = False, echo_sql_to_console: bool = True):
    ENGINE.echo = echo_sql_to_console

    # top level 6 hour check
    if full_reset_and_refetch:
        drop_and_full_rebuild_db()

    ensure_blocks_is_current()
    ensure_autopools_are_current()
    # return
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


def main():
    ensure_database_is_current(full_reset_and_refetch=False, echo_sql_to_console=True)


if __name__ == "__main__":
    from mainnet_launch.app.profiler import profile_function
    profile_function(main, top_n=10)


#    Ordered by: cumulative time
#    List reduced from 5584 to 175 due to restriction <'v2-rebalance-dashboard'>
#    List reduced from 175 to 100 due to restriction <100>

#    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
#         1    0.000    0.000  311.366  311.366 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/ensure_all_tables_are_current.py:105(main)
#         1    0.026    0.026  311.366  311.366 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/ensure_all_tables_are_current.py:57(ensure_database_is_current)
#        26    0.000    0.000  118.406    4.554 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:70(get_raw_state_by_blocks)
#     49724    0.048    0.000   83.043    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:134(_fetch_data)
#         1    0.036    0.036   72.717   72.717 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:546(ensure_destination_states_are_current)
#         2    0.010    0.005   72.242   36.121 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:307(_add_new_destination_states_to_db)
#         1    0.020    0.020   53.280   53.280 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_rebalance_plans/update_destination_states_from_rebalance_plan.py:230(update_destination_states_from_rebalance_plan)
#        42    0.028    0.001   46.334    1.103 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:92(insert_avoid_conflicts)
#        42    0.005    0.000   45.377    1.080 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:105(bulk_copy_skip_duplicates)
#         1    0.034    0.034   34.931   34.931 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_rebalance_plans/update_rebalance_plans.py:194(ensure_rebalance_plans_table_are_current)
#         1    0.021    0.021   32.201   32.201 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:188(ensure_autopool_destination_states_are_current)
#         3    0.015    0.005   32.162   10.721 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:61(_fetch_and_insert_new_autopool_destination_states)
#         2    0.000    0.000   28.681   14.340 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:67(_fetch_lp_token_spot_prices)
#         1    0.012    0.012   27.315   27.315 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:124(ensure_token_values_are_current)
#         3    0.009    0.003   27.292    9.097 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:77(_fetch_and_insert_new_token_values)
#         1    0.011    0.011   22.803   22.803 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:245(ensure_destination_token_values_are_current)
#         3    0.011    0.004   22.784    7.595 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:142(_fetch_and_insert_destination_token_values)
#         2    0.001    0.000   22.496   11.248 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:212(_fetch_destination_summary_stats_df)
#         1    0.006    0.006   20.068   20.068 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/block_timestamp.py:90(ensure_blocks_is_current)
#         2    0.000    0.000   18.356    9.178 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:203(_fetch_safe_and_backing_values)
#         2    0.009    0.005   17.175    8.588 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/block_timestamp.py:38(_fetch_block_df_from_subgraph)
#        23    0.001    0.000   15.375    0.668 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:25(merge_tables_as_df)
#         1    0.000    0.000   14.613   14.613 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:643(drop_and_full_rebuild_db)
#         1    0.006    0.006   13.962   13.962 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_states.py:149(ensure_autopool_states_are_current)
#         3    0.001    0.000   13.951    4.650 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_states.py:105(_fetch_and_insert_new_autopool_states)
#         1    0.004    0.004   12.933   12.933 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:149(ensure__destinations__tokens__and__destination_tokens_are_current)
#        28    0.001    0.000   12.247    0.437 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:177(get_subset_not_already_in_column)
#         2    0.000    0.000   10.379    5.190 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:91(_fetch_destination_token_value_data_from_external_source)
#        24    0.001    0.000   10.342    0.431 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:247(get_full_table_as_orm)
#         2    0.000    0.000    9.006    4.503 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:141(_fetch_autopool_points_apr)
#         5    0.001    0.000    7.176    1.435 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/block_timestamp.py:123(ensure_all_blocks_are_in_table)
#         1    0.000    0.000    6.500    6.500 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopools_table.py:30(ensure_autopools_are_current)
#         3    0.000    0.000    6.401    2.134 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_states.py:36(_fetch_new_autopool_state_rows)
#         2    0.000    0.000    4.770    2.385 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:114(_fetch_destination_total_supply_df)
#         8    0.000    0.000    4.589    0.574 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_events.py:104(fetch_events)
#         5    0.002    0.000    4.499    0.900 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/block_timestamp.py:23(add_blocks_from_dataframe_to_database)
#         3    0.002    0.001    3.883    1.294 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:31(_determine_what_blocks_are_needed)
#         3    0.002    0.001    3.780    1.260 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:48(_determine_what_blocks_are_needed)
#         3    0.003    0.001    3.673    1.224 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_states.py:120(_determine_what_blocks_are_needed)
#         8    0.000    0.000    3.633    0.454 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_events.py:57(_recursive_helper_get_all_events_within_range)
#         3    0.000    0.000    3.463    1.154 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:109(_determine_what_blocks_are_needed)
#        12    0.000    0.000    2.940    0.245 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:36(get_state_by_one_block)
#         3    0.000    0.000    1.917    0.639 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:147(_build_idle_autopool_destination_states)
#         2    0.000    0.000    1.847    0.923 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:256(_fetch_idle_destination_token_values)
#         2    0.000    0.000    1.469    0.734 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:158(_build_backing_calls)
#         2    0.001    0.000    1.417    0.708 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:69(_make_destination_vault_dicts)
#         2    0.015    0.008    1.316    0.658 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:241(_extract_new_destination_states)
#         4    0.000    0.000    1.189    0.297 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:25(_fetch_token_rows)
#     26340    0.055    0.000    1.132    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:259(_extract_destination_states)
#       446    0.063    0.000    1.106    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:89(_extract_token_values_by_row)
#       997    0.040    0.000    1.080    0.001 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:115(_extract_autopool_destination_vault_balance_of_block)
#       137    0.001    0.000    1.003    0.007 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:231(<lambda>)
#       137    0.035    0.000    1.002    0.007 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:193(_extract_destination_token_values)
#         2    0.000    0.000    0.930    0.465 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopools_table.py:16(_fetch_autopool_state_dicts)
#        42    0.041    0.001    0.929    0.022 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:101(<listcomp>)
#    213222    0.157    0.000    0.888    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:43(to_tuple)
#         2    0.000    0.000    0.826    0.413 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:163(get_highest_value_in_field_where)
#         2    0.000    0.000    0.820    0.410 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:278(build_blocks_to_use)
#         2    0.000    0.000    0.820    0.410 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:255(postgres_build_blocks_to_use)
#     39646    0.034    0.000    0.618    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:37(from_record)
#   1987189    0.160    0.000    0.584    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:47(<genexpr>)
#       137    0.007    0.000    0.517    0.004 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:228(<listcomp>)
#         1    0.000    0.000    0.426    0.426 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:415(_overwrite_bad_summary_states_rows)
#         1    0.000    0.000    0.426    0.426 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:383(set_some_cells_to_null)
#        52    0.001    0.000    0.172    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:88(async_safe_get_raw_state_by_block)
#       997    0.007    0.000    0.120    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_states.py:87(_extract_autopool_state)
#         5    0.002    0.000    0.109    0.022 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/block_timestamp.py:34(<listcomp>)
#     33167    0.012    0.000    0.104    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:220(<lambda>)
#        24    0.001    0.000    0.071    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:259(<listcomp>)
#      1226    0.007    0.000    0.070    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:49(from_tuple)
#        28    0.065    0.002    0.065    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:203(<listcomp>)
#      1738    0.003    0.000    0.062    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_autopool_destination_states_table.py:170(_extract_idle_autopool_destination_state)
#         2    0.001    0.000    0.059    0.030 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:385(_fetch_idle_destination_states)
#        26    0.001    0.000    0.055    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:181(_convert_multicall_responeses_to_df)
#        26    0.009    0.000    0.034    0.001 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:119(<listcomp>)
#     52680    0.025    0.000    0.025    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:159(_clean_summary_stats_info)
#       446    0.001    0.000    0.024    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:284(_extract_idle_destination_token_values)
#         1    0.000    0.000    0.022    0.022 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_rebalance_plans/update_destination_states_from_rebalance_plan.py:266(<dictcomp>)
#         2    0.000    0.000    0.020    0.010 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:135(_build_safe_price_calls)
#       216    0.000    0.000    0.020    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:180(_build_summary_stats_call)
#     26340    0.016    0.000    0.016    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:50(_handle_getRangePricesLP)
#         5    0.000    0.000    0.015    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:35(build_lp_token_spot_price_calls)
#         5    0.000    0.000    0.015    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:57(<listcomp>)
#    138852    0.014    0.000    0.014    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:194(safe_normalize_with_bool_success)
#         2    0.000    0.000    0.013    0.007 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:137(<listcomp>)
#     39646    0.012    0.000    0.012    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:40(<dictcomp>)
#         2    0.000    0.000    0.012    0.006 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:61(_build_ETH_autopool_price_calls)
#     39646    0.012    0.000    0.012    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/full.py:39(<setcomp>)
#         4    0.000    0.000    0.011    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:26(<listcomp>)
#         8    0.000    0.000    0.010    0.001 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_events.py:97(events_to_df)
#         4    0.000    0.000    0.010    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:57(<listcomp>)
#        54    0.001    0.000    0.009    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/postgres_operations.py:341(_where_clause_to_string)
#       163    0.000    0.000    0.009    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:168(<lambda>)
#         2    0.000    0.000    0.008    0.004 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:103(build_destinations_underlyingTotalSupply_calls)
#         2    0.000    0.000    0.008    0.004 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_states_table.py:104(<listcomp>)
#        26    0.000    0.000    0.008    0.000 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/data_fetching/get_state_by_block.py:55(_build_default_block_and_timestamp_calls)
#         1    0.002    0.002    0.008    0.008 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_rebalance_plans/update_destination_states_from_rebalance_plan.py:274(<listcomp>)
#         4    0.000    0.000    0.007    0.002 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destinations_table.py:35(<listcomp>)
#         2    0.000    0.000    0.007    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_token_values_table.py:146(<listcomp>)
#         2    0.000    0.000    0.007    0.003 /Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_token_values_tables.py:66(<listcomp>)
