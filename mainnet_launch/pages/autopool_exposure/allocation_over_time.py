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
def _fetch_all_destination_lp_token_safe_value(autopool: AutopoolConstants) -> pd.DataFrame:
    pass

def _fetch_all_destination_lp_token_spot_value(autopool: AutopoolConstants) -> pd.DataFrame:
    pass

def _fetch_tvl_by_asset_and_destination(autopool: AutopoolConstants) -> pd.DataFrame:
    token_value_df = merge_tables_as_df(
        [
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol, Tokens.token_address],
                join_on=None,
                row_filter=(Tokens.chain_id == autopool.chain.chain_id),
            ),
            TableSelector(
                table=TokenValues,
                select_fields=[
                    TokenValues.safe_price,
                    TokenValues.block,
                    TokenValues.denominated_in,
                    TokenValues.backing,
                ],
                join_on=(TokenValues.chain_id == Tokens.chain_id) & (TokenValues.token_address == Tokens.token_address),
                row_filter=(TokenValues.denominated_in == autopool.base_asset),
            ),
            TableSelector(
                table=Blocks,
                select_fields=[Blocks.datetime],
                join_on=(Blocks.chain_id == TokenValues.chain_id) & (Blocks.block == TokenValues.block),
            ),
        ],
        where_clause=(TokenValues.denominated_in == autopool.base_asset),
    )

    destinations_df = merge_tables_as_df(
        [
            TableSelector(
                table=DestinationTokenValues,
                select_fields=[
                    DestinationTokenValues.token_address,
                    DestinationTokenValues.destination_vault_address,
                    DestinationTokenValues.quantity,
                ],
            ),
            TableSelector(
                table=AutopoolDestinationStates,
                select_fields=[
                    AutopoolDestinationStates.owned_shares,
                    AutopoolDestinationStates.destination_vault_address,
                    AutopoolDestinationStates.block,
                ],
                join_on=(DestinationTokenValues.chain_id == AutopoolDestinationStates.chain_id)
                & (
                    DestinationTokenValues.destination_vault_address
                    == AutopoolDestinationStates.destination_vault_address
                )
                & (DestinationTokenValues.block == AutopoolDestinationStates.block),
            ),
            TableSelector(
                table=DestinationStates,
                select_fields=[
                    DestinationStates.underlying_token_total_supply,
                    DestinationStates.lp_token_safe_price,
                ],
                join_on=(DestinationStates.chain_id == DestinationTokenValues.chain_id)
                & (DestinationStates.destination_vault_address == DestinationTokenValues.destination_vault_address)
                & (DestinationStates.block == DestinationTokenValues.block),
            ),
            TableSelector(
                table=Destinations,
                select_fields=[Destinations.underlying_symbol, Destinations.exchange_name],
                join_on=(Destinations.chain_id == DestinationTokenValues.chain_id)
                & (Destinations.destination_vault_address == DestinationTokenValues.destination_vault_address),
            ),
        ],
        where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr)
        & (AutopoolDestinationStates.block > autopool.block_deployed),
    )

    df = pd.merge(destinations_df, token_value_df, on=["block", "token_address"], how="left")

    df = df[df["datetime"] >= autopool.start_display_date].copy()

    underlying_symbol_to_readable_name = {
        underlying_symbol: f"{underlying_symbol} ({exchange_name})"
        for underlying_symbol, exchange_name in zip(df["underlying_symbol"], df["exchange_name"])
    }

    df["portion_owned"] = (df["owned_shares"] / df["underlying_token_total_supply"]).fillna(1.0)
    df["autopool_implied_safe_value"] = df["portion_owned"] * df["quantity"] * df["safe_price"]
    df["autopool_implied_backing_value"] = df["portion_owned"] * df["quantity"] * df["backing"]

    safe_value_by_destination = (
        df.groupby(["datetime", "underlying_symbol"])["autopool_implied_safe_value"]
        .sum()
        .reset_index()
        .pivot(columns=["underlying_symbol"], index=["datetime"], values="autopool_implied_safe_value")
    )
    safe_value_by_destination.columns = [
        underlying_symbol_to_readable_name[undelrying_symbol] for undelrying_symbol in safe_value_by_destination.columns
    ]
    safe_value_by_destination = safe_value_by_destination.resample("1D").last()

    safe_value_by_asset = (
        df.groupby(["datetime", "symbol"])["autopool_implied_safe_value"]
        .sum()
        .reset_index()
        .pivot(columns=["symbol"], index=["datetime"], values="autopool_implied_safe_value")
    )
    safe_value_by_asset = safe_value_by_asset.resample("1D").last()

    backing_value_by_destination = (
        df.groupby(["datetime", "underlying_symbol"])["autopool_implied_backing_value"]
        .sum()
        .reset_index()
        .pivot(columns=["underlying_symbol"], index=["datetime"], values="autopool_implied_backing_value")
    )

    backing_value_by_destination.columns = [
        underlying_symbol_to_readable_name[undelrying_symbol]
        for undelrying_symbol in backing_value_by_destination.columns
    ]

    backing_value_by_destination = backing_value_by_destination.resample("1D").last()
    return safe_value_by_destination, safe_value_by_asset, backing_value_by_destination


def fetch_and_render_asset_allocation_over_time(autopool: AutopoolConstants):
    safe_value_by_destination, safe_value_by_asset, backing_value_by_asset = _fetch_tvl_by_asset_and_destination(
        autopool
    )
    del backing_value_by_asset

    percent_tvl_by_destination = 100 * safe_value_by_destination.div(safe_value_by_destination.sum(axis=1), axis=0)

    latest = percent_tvl_by_destination.tail(1).iloc[0]

    # drop any destinations under .1%
    filtered = latest[latest >= 0.1]

    # plot only the remaining slices
    fig = px.pie(
        values=filtered.values,
        names=filtered.index,
        title="Percent Allocation by Destination (≥.1%)",
    )
    st.plotly_chart(fig, use_container_width=True)

    if autopool.base_asset in WETH:
        base_asset_name = "ETH"
    elif autopool.base_asset in USDC:
        base_asset_name = "USDC"
    else:
        raise ValueError(f"Unexpected {autopool.base_asset=}")

    st.plotly_chart(
        px.bar(safe_value_by_destination, title="TVL by Destination", labels={"value": base_asset_name}),
        use_container_width=True,
    )
    st.plotly_chart(
        px.bar(percent_tvl_by_destination, title="TVL Percent by Destination", labels={"value": "Percent"}),
        use_container_width=True,
    )

    st.plotly_chart(
        px.bar(safe_value_by_asset, title="TVL by Asset", labels={"value": base_asset_name}),
        use_container_width=True,
    )
    percent_tvl_by_asset = 100 * safe_value_by_asset.div(safe_value_by_asset.sum(axis=1), axis=0)

    st.plotly_chart(
        px.bar(percent_tvl_by_asset, title="TVL Percent by Asset", labels={"value": "Percent"}),
        use_container_width=True,
    )


if __name__ == "__main__":
    from mainnet_launch.constants import AUTO_ETH, AUTO_USD

    fetch_and_render_asset_allocation_over_time(AUTO_ETH)
    # fetch_and_render_asset_allocation_over_time(AUTO_ETH)
    # pass
