import pandas as pd
import streamlit as st
import plotly.express as px
from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, WETH, USDC

from mainnet_launch.database.schema.full import (
    AutopoolDestinationStates,
    Destinations,
    DestinationStates,
    Blocks,
    DestinationTokenValues,
    Tokens,
    TokenValues,
)
from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
)
from mainnet_launch.database.schema.views import get_all_autopool_destinations, fetch_autopool_destination_state_df


def _fetch_all_destination_lp_token_safe_value(autopool: AutopoolConstants) -> pd.DataFrame:
    pass


def _fetch_all_destination_lp_token_spot_value(autopool: AutopoolConstants) -> pd.DataFrame:
    pass


# def fetch_token_value_df(autopool: AutopoolConstants) -> pd.DataFrame:
#     all_primary_tokens_df = get_all_autopool_basket_of_primary_assets(autopool)

#     token_value_df = merge_tables_as_df(
#         [
#             TableSelector(
#                 table=Tokens,
#                 select_fields=[Tokens.symbol, Tokens.token_address],
#             ),
#             TableSelector(
#                 table=TokenValues,
#                 select_fields=[
#                     TokenValues.safe_price,
#                     TokenValues.block,
#                     TokenValues.denominated_in,
#                     TokenValues.backing,
#                 ],
#                 join_on=(TokenValues.chain_id == Tokens.chain_id) & (TokenValues.token_address == Tokens.token_address),
#             ),
#             TableSelector(
#                 table=Blocks,
#                 select_fields=[Blocks.datetime],
#                 join_on=(Blocks.chain_id == TokenValues.chain_id) & (Blocks.block == TokenValues.block),
#             ),
#         ],
#         where_clause=(TokenValues.denominated_in == autopool.base_asset)
#         & (Tokens.chain_id == autopool.chain.chain_id)
#         & (Tokens.token_address.in_(all_primary_tokens_df["token_address"]))
#         & (Blocks.datetime > autopool.start_display_date),
#     )
#     return token_value_df


# def fetch_destination_state_by_autopool(autopool: AutopoolConstants) -> pd.DataFrame:
#     destinations_df = merge_tables_as_df(
#         [
#             TableSelector(
#                 table=DestinationTokenValues,
#                 select_fields=[
#                     DestinationTokenValues.token_address,
#                     DestinationTokenValues.destination_vault_address,
#                     DestinationTokenValues.quantity,
#                     DestinationTokenValues.block,
#                 ],
#             ),
#             TableSelector(
#                 table=AutopoolDestinationStates,
#                 select_fields=[
#                     AutopoolDestinationStates.owned_shares,
#                 ],
#                 join_on=(DestinationTokenValues.chain_id == AutopoolDestinationStates.chain_id)
#                 & (
#                     DestinationTokenValues.destination_vault_address
#                     == AutopoolDestinationStates.destination_vault_address
#                 )
#                 & (DestinationTokenValues.block == AutopoolDestinationStates.block),
#             ),
#             TableSelector(
#                 table=DestinationStates,
#                 select_fields=[
#                     DestinationStates.underlying_token_total_supply,
#                     DestinationStates.lp_token_safe_price,
#                 ],
#                 join_on=(DestinationStates.chain_id == DestinationTokenValues.chain_id)
#                 & (DestinationStates.destination_vault_address == DestinationTokenValues.destination_vault_address)
#                 & (DestinationStates.block == DestinationTokenValues.block),
#             ),
#             TableSelector(
#                 table=Destinations,
#                 select_fields=[Destinations.underlying_name, Destinations.exchange_name],
#                 join_on=(Destinations.chain_id == DestinationTokenValues.chain_id)
#                 & (Destinations.destination_vault_address == DestinationTokenValues.destination_vault_address),
#             ),
#         ],
#         where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr),
#     )

#     destinations_df["readable_name"] = destinations_df.apply(
#         lambda row: f"{row['underlying_name']} ({row['exchange_name']})", axis=1
#     )

#     # for the idle destination, owned shares == underlying_token_total_supply
#     # not certain is needed
#     destinations_df.loc[
#         destinations_df["destination_vault_address"] == autopool.autopool_eth_addr, "underlying_token_total_supply"
#     ] = destinations_df.loc[destinations_df["destination_vault_address"] == autopool.autopool_eth_addr]["owned_shares"]

#     # of the total supply, how much do we own,
#     # note, I don't think this right, because of
#     # how (some) amount of lp tokens are not staked on convex or aura
#     destinations_df["portion_owned"] = (
#         destinations_df["owned_shares"] / destinations_df["underlying_token_total_supply"]
#     )

#     return destinations_df


# def _fetch_tvl_by_asset_and_destination(autopool: AutopoolConstants) -> pd.DataFrame:
#     token_value_df = fetch_token_value_df(autopool)
#     destinations_df = fetch_destination_state_by_autopool(autopool)

#     df = pd.merge(destinations_df, token_value_df, on=["block", "token_address"], how="left")

#     df["autopool_implied_safe_value"] = df["portion_owned"] * df["quantity"] * df["safe_price"]
#     df["autopool_implied_backing_value"] = df["portion_owned"] * df["quantity"] * df["backing"]
#     df["autopool_implied_quantity"] = df["portion_owned"] * df["quantity"]
#     return df


# def _extract_end_of_day_dfs(df:pd.DataFrame, autopool: AutopoolConstants) -> pd.DataFrame:
#     end_of_day_safe_value_by_destination = (
#         df.groupby(["datetime", "readable_name"])["autopool_implied_safe_value"]
#         .sum()
#         .reset_index()
#         .pivot(columns=["readable_name"], index=["datetime"], values="autopool_implied_safe_value")
#     ).resample("1D").last()

#     end_of_day_safe_value_by_asset = (
#         df.groupby(["datetime", "symbol"])["autopool_implied_safe_value"]
#         .sum()
#         .reset_index()
#         .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_safe_value")
#     ).resample("1D").last()

#     end_of_day_backing_value_by_destination = (
#         df.groupby(["datetime", "readable_name"])["autopool_implied_backing_value"]
#         .sum()
#         .reset_index()
#         .pivot(columns=["readable_name"], index=["datetime"], values="autopool_implied_backing_value")
#     ).resample("1D").last()


#     end_of_day_quantity_by_asset = (
#         df.groupby(["datetime", "symbol"])["autopool_implied_quantity"]
#         .sum()
#         .reset_index()
#         .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_quantity")
#     ).resample("1D").last()


#     return end_of_day_safe_value_by_destination, end_of_day_safe_value_by_asset, end_of_day_backing_value_by_destination, end_of_day_quantity_by_asset


def fetch_and_render_asset_allocation_over_time(autopool: AutopoolConstants):
    df = fetch_autopool_destination_state_df(autopool)

    end_of_day_safe_value_by_destination = (
        (
            df.groupby(["datetime", "readable_name"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["readable_name"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    end_of_day_safe_value_by_asset = (
        (
            df.groupby(["datetime", "symbol"])["autopool_implied_safe_value"]
            .sum()
            .reset_index()
            .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_safe_value")
        )
        .resample("1D")
        .last()
    )

    percent_tvl_by_destination = 100 * end_of_day_safe_value_by_destination.div(
        end_of_day_safe_value_by_destination.sum(axis=1), axis=0
    )

    latest = percent_tvl_by_destination.tail(1).iloc[0]
    destinations_over_point_1_percent = latest[latest >= 0.1]

    st.plotly_chart(
        px.pie(
            values=destinations_over_point_1_percent.values,
            names=destinations_over_point_1_percent.index,
            title="Percent Allocation by Destination (≥.1%)",
        ),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(
            end_of_day_safe_value_by_destination,
            title="TVL by Destination",
            labels={"value": autopool.base_asset_symbol},
        ),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(percent_tvl_by_destination, title="TVL Percent by Destination", labels={"value": "Percent"}),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(end_of_day_safe_value_by_asset, title="TVL by Asset", labels={"value": autopool.base_asset_symbol}),
        use_container_width=True,
    )

    percent_tvl_by_asset = 100 * end_of_day_safe_value_by_asset.div(end_of_day_safe_value_by_asset.sum(axis=1), axis=0)

    st.plotly_chart(
        px.bar(percent_tvl_by_asset, title="TVL Percent by Asset", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import *

    fetch_and_render_asset_allocation_over_time(SONIC_USD)
