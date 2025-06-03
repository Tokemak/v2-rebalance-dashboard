# import pandas as pd
# from mainnet_launch.constants import AutopoolConstants


# from mainnet_launch.data_fetching.get_state_by_block import safe_normalize_6_with_bool_success, safe_normalize_with_bool_success, get_raw_state_by_blocks


# from mainnet_launch.database.schema.postgres_operations import merge_tables_as_df, TableSelector, get_full_table_as_df, insert_avoid_conflicts
# from mainnet_launch.database.schema.full import (
#     RebalanceEvents,
#     RebalancePlans,
#     Blocks,
#     Destinations,
#     Transactions,
#     DestinationTokens,
#     Tokens,
# )


# def build_needed_balance_of_calls(rebalance_event_df:pd.DataFrame):


# # map decimals → normalizer
# dec_norm = {
#     6: safe_normalize_6_with_bool_success,
#     18: safe_normalize_with_bool_success,
# }

# autoUSD_flash_borrow_solver = "0xD02b50CFc6c2903bF13638B28D081ad11515B6f9"

# balance_of_calls = []
# for _, row in tokens_df.iterrows():
#     token = row["token_address"]
#     normalizer = dec_norm[row["decimals"]]
#     call = Call(
#         token,
#         ["balanceOf(address)(uint256)", autoUSD_flash_borrow_solver],
#         [(token, normalizer)],
#     )
#     balance_of_calls.append(call)

# before = get_state_by_one_block(balance_of_calls, 22597581 - 1, ETH_CHAIN)
# after = get_state_by_one_block(balance_of_calls, 22597581, ETH_CHAIN)


# diff = {}
# for k in before.keys():
#     if after[k] is not None:
#         diff[k] = after[k] - before[k]
# diff
# # looks like there was an extra $22 frxUSD left in the pool
