import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import (
    AutopoolConstants,
    STATS_CALCULATOR_REGISTRY,
    ALL_AUTOPOOLS,
    ETH_CHAIN,
    ROOT_PRICE_ORACLE,
    ChainData,
    WETH,
)

from mainnet_launch.database.schema.full import AutopoolDestinationStates, Destinations, Blocks
from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
)


# by destination
def fetch_and_render_asset_allocation_over_time(autopool: AutopoolConstants):
    autopool_value_over_time_by_df = merge_tables_as_df(
        [
            TableSelector(
                table=AutopoolDestinationStates,
                select_fields=[AutopoolDestinationStates.total_safe_value],
                join_on=None,
                row_filter=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr),
            ),
            TableSelector(
                table=Destinations,
                select_fields=Destinations.underlying_symbol,
                join_on=(AutopoolDestinationStates.chain_id == Destinations.chain_id)
                & (AutopoolDestinationStates.destination_vault_address == Destinations.destination_vault_address),
            ),
            TableSelector(
                table=Blocks,
                select_fields=Blocks.datetime,
                join_on=(AutopoolDestinationStates.chain_id == Blocks.chain_id)
                & (AutopoolDestinationStates.block == Blocks.block),
            ),
        ],
        order_by=Blocks.datetime,
    )

    #
    # weETH/rETH
    # safe value is not correct
    autopool_value_over_time_by_df = (
        autopool_value_over_time_by_df.groupby(["datetime", "underlying_symbol"])["total_safe_value"]
        .sum()
        .reset_index()
    )

    safe_tvl_by_destination = autopool_value_over_time_by_df.pivot(
        index="datetime", values="total_safe_value", columns="underlying_symbol"
    ).fillna(0)

    # different destination vaults the same destination

    percent_tvl_by_destination = 100 * safe_tvl_by_destination.div(safe_tvl_by_destination.sum(axis=1), axis=0)
    print(safe_tvl_by_destination.tail().round())
    print(percent_tvl_by_destination.tail().round())
    # TODO switch to read the base asset symbol
    st.plotly_chart(
        px.bar(safe_tvl_by_destination, title="TVL ETH value by asset", labels={"value": "ETH"}),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(percent_tvl_by_destination, title="TVL Percent by asset", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    fetch_and_render_asset_allocation_over_time(ALL_AUTOPOOLS[0])
    pass


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
#         ["balanceOf(address)(uint256)", autopool.autopool_eth_addr],
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
#         ["balanceOf(address)(uint256)", dest.autopool.autopool_eth_addr],
#         [
#             (f"{unique_id}_autopool_lp_tokens", safe_normalize_with_bool_success),
#         ],
#     )
#     return [underlying_reserves_call, underlyingTotalSupply_call, balance_of_call], unique_id
