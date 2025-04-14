import pandas as pd
import streamlit as st
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.solver_diagnostics.solver_rebalance_plans_to_summary_stats import (
    DESTINATION_TOKEN_BLOCK_TABLE,
)
from mainnet_launch.database.database_operations import run_read_only_query


def fetch_and_render_asset_allocation_over_time(autopool: AutopoolConstants):
    asset_allocation_df = run_read_only_query(
        f"""
    SELECT amount * portion_ownership * safe_price as autopool_safe_token_value, token_symbol, block_timestamp FROM {DESTINATION_TOKEN_BLOCK_TABLE} where autopool = ?
    """,
        (autopool.name,),
    )
    asset_allocation_df = asset_allocation_df.groupby(["block_timestamp", "token_symbol"]).sum().reset_index()
    asset_allocation_df = asset_allocation_df.pivot(
        index="block_timestamp", columns="token_symbol", values="autopool_safe_token_value"
    )
    asset_allocation_df.index = pd.to_datetime(asset_allocation_df.index, unit="s", utc=True)
    asset_allocation_df = asset_allocation_df.sort_index()
    asset_allocation_df = asset_allocation_df.resample("1d").last()
    asset_percent_allocation_df = 100 * asset_allocation_df.div(asset_allocation_df.sum(axis=1), axis=0)

    st.plotly_chart(
        px.bar(
            asset_allocation_df, title="TVL Safe Value by Asset", labels={"yaxis_title": autopool.base_asset_symbol}
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(asset_percent_allocation_df, title="TVL Percent by Asset", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS

    for a in ALL_AUTOPOOLS:
        fetch_and_render_asset_allocation_over_time(a)


# import pandas as pd
# import plotly.graph_objects as go
# import streamlit as st
# import plotly.express as px
# from mainnet_launch.constants import (
#     AutopoolConstants,
#     STATS_CALCULATOR_REGISTRY,
#     ALL_AUTOPOOLS,
#     ETH_CHAIN,
#     ROOT_PRICE_ORACLE,
#     ChainData,
#     WETH,
# )
# from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
# from mainnet_launch.abis import AUTOPOOL_VAULT_ABI
# from mainnet_launch.pages.asset_discounts.fetch_eth_asset_discounts import _fetch_lst_calc_addresses_df

# from mainnet_launch.database.database_operations import (
#     write_dataframe_to_table,
#     get_earliest_block_from_table_with_chain,
#     get_all_rows_in_table_by_autopool,
#     get_all_rows_in_table_by_chain,
# )
# from mainnet_launch.database.should_update_database import should_update_table

# from mainnet_launch.abis import STATS_CALCULATOR_REGISTRY_ABI

# from mainnet_launch.pages.solver_diagnostics.solver_rebalance_plans_to_summary_stats import (
#     DESTINATION_TOKEN_BLOCK_TABLE,
# )
# from mainnet_launch.database.database_operations import run_read_only_query
# from multicall import Call
# from mainnet_launch.data_fetching.get_state_by_block import (
#     identity_with_bool_success,
#     get_raw_state_by_blocks,
#     get_state_by_one_block,
#     safe_normalize_with_bool_success,
#     build_blocks_to_use,
# )
# from mainnet_launch.destinations import get_destination_details, DestinationDetails
# from mainnet_launch.database.database_operations import (
#     write_dataframe_to_table,
#     get_earliest_block_from_table_with_autopool,
#     run_read_only_query,
#     does_table_exist,
# )

# AUTOPOOL_ASSET_ALLOCATION_TABLE = "AUTOPOOL_ASSET_ALLOCATION_TABLE"


# def add_new_asset_allocation_data_to_table():
#     if should_update_table(AUTOPOOL_ASSET_ALLOCATION_TABLE):
#         for autopool in ALL_AUTOPOOLS:
#             highest_block_already_fetched = get_earliest_block_from_table_with_autopool(
#                 AUTOPOOL_ASSET_ALLOCATION_TABLE, autopool
#             )
#             asset_allocation_over_time = _fetch_asset_allocation_over_time_from_external_source(
#                 autopool, highest_block_already_fetched
#             )
#             asset_allocation_over_time["autopool"] = autopool.name
#             # note this is a quantity, not a value
#             write_dataframe_to_table(asset_allocation_over_time, AUTOPOOL_ASSET_ALLOCATION_TABLE)


# def _fetch_asset_allocation_over_time_from_external_source(
#     autopool: AutopoolConstants, start_block: int
# ) -> pd.DataFrame:
#     # returns a table of the quantity of each asset this autopool controls

#     blocks = [b for b in build_blocks_to_use(autopool.chain) if b >= start_block]

#     destination_details = get_destination_details(autopool)

#     all_calls = []
#     id_to_dest = {}
#     for dest in destination_details:
#         if dest.autopool not in [ALL_AUTOPOOLS]:
#             calls, unique_id = _make_destination_asset_reserves_calls(dest)
#             all_calls.extend(calls)
#             id_to_dest[unique_id] = dest

#     idle_eth_call = _make_idle_eth_call(autopool)
#     all_calls.append(idle_eth_call)
#     df = get_raw_state_by_blocks(all_calls, blocks, chain=autopool.chain, include_block_number=True)

#     def _extract_quantity_of_assets(row: dict):
#         # returns a dictionary of {token_address:quantity of tokens the autopool controls}
#         quantity_of_assets = {}

#         for unique_id, dest in id_to_dest.items():
#             lp_total_supply = row[f"{unique_id}_total_supply"]
#             autopool_lp_tokens = row[f"{unique_id}_autopool_lp_tokens"]
#             if isinstance(lp_total_supply, float) and isinstance(autopool_lp_tokens, float):
#                 if lp_total_supply > 0 and autopool_lp_tokens > 0:

#                     portion_ownership_of_pool = autopool_lp_tokens / lp_total_supply

#                     for token_addr, amount in zip(
#                         row[f"{unique_id}_underlyingReserves_tokens"], row[f"{unique_id}_underlyingReserves_amounts"]
#                     ):
#                         if token_addr.lower() != dest.lpTokenAddress.lower():
#                             # for balancer stable pools skip the lp token
#                             if token_addr not in quantity_of_assets:

#                                 quantity_of_assets[token_addr] = portion_ownership_of_pool * amount
#                             else:
#                                 quantity_of_assets[token_addr] += portion_ownership_of_pool * amount

#         weth = WETH(autopool.chain).lower()
#         if weth not in quantity_of_assets:
#             quantity_of_assets[weth] = row["autopool_idle"]
#         else:
#             quantity_of_assets[weth] += row["autopool_idle"]

#         return quantity_of_assets

#     assets_df = pd.DataFrame.from_records(df.apply(lambda row: _extract_quantity_of_assets(row), axis=1))
#     assets_df.index = df.index
#     assets_df["block"] = df["block"]

#     lst_df = _fetch_lst_calc_addresses_df(autopool.chain)
#     asset_to_symbol = dict(zip(lst_df["lst"], lst_df["symbol"]))
#     asset_to_symbol[WETH(autopool.chain).lower()] = "WETH"
#     assets_df = assets_df.rename(columns=asset_to_symbol)

#     long_asset_df = pd.melt(assets_df, id_vars=["block"], var_name="symbol", value_name="quantity")
#     # wide_asset_df = long_asset_df.pivot(index="block", columns="symbol", values="quantity").reset_index()

#     return long_asset_df


# def same_normalize_with_bool_success_list(success, values):
#     if success:
#         return tuple([int(val) / 1e18 for val in values])


# def _make_idle_eth_call(autopool: AutopoolConstants) -> Call:
#     # gets the idle idle in the autopool

#     balance_of_call = Call(
#         WETH(autopool.chain),
#         ["balanceOf(address)(uint256)", autopool.autopool_addr],
#         [
#             (f"autopool_idle", safe_normalize_with_bool_success),
#         ],
#     )
#     return balance_of_call


# def _make_destination_asset_reserves_calls(dest: DestinationDetails) -> list[Call]:
#     unique_id = f"{dest.vault_name} {dest.vaultAddress}"
#     underlying_reserves_call = Call(
#         dest.vaultAddress,
#         ["underlyingReserves()(address[],uint256[])"],
#         [
#             (f"{unique_id}_underlyingReserves_tokens", None),
#             (f"{unique_id}_underlyingReserves_amounts", same_normalize_with_bool_success_list),
#         ],
#     )
#     underlyingTotalSupply_call = Call(
#         dest.vaultAddress,
#         ["underlyingTotalSupply()(uint256)"],
#         [
#             (f"{unique_id}_total_supply", safe_normalize_with_bool_success),
#         ],
#     )

#     balance_of_call = Call(
#         dest.vaultAddress,
#         ["balanceOf(address)(uint256)", dest.autopool.autopool_addr],
#         [
#             (f"{unique_id}_autopool_lp_tokens", safe_normalize_with_bool_success),
#         ],
#     )
#     return [underlying_reserves_call, underlyingTotalSupply_call, balance_of_call], unique_id
