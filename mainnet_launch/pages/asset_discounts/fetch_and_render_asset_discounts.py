import plotly.express as px
import pandas as pd
import streamlit as st

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.schema.postgres_operations import *


# consider this as a view
def _fetch_autopool_dest_token_table(autopool: AutopoolConstants) -> pd.DataFrame:
    """
    returns a df of destinations, their tokens & token symbols for the given autopool
    """
    return merge_tables_as_df(
        selectors=[
            TableSelector(
                table=AutopoolDestinations,
            ),
            TableSelector(
                table=Destinations,
                select_fields=[Destinations.underlying_name, Destinations.exchange_name],
                join_on=(
                    (Destinations.destination_vault_address == AutopoolDestinations.destination_vault_address)
                    & (Destinations.chain_id == AutopoolDestinations.chain_id)
                ),
            ),
            TableSelector(
                table=DestinationTokens,
                select_fields=[
                    DestinationTokens.token_address,
                    DestinationTokens.index,
                ],
                join_on=(
                    (DestinationTokens.destination_vault_address == AutopoolDestinations.destination_vault_address)
                    & (DestinationTokens.chain_id == AutopoolDestinations.chain_id)
                ),
            ),
            TableSelector(
                table=Tokens,
                select_fields=[
                    Tokens.symbol,
                    Tokens.name,
                    Tokens.decimals,
                ],
                join_on=(
                    (Tokens.chain_id == DestinationTokens.chain_id)
                    & (Tokens.token_address == DestinationTokens.token_address)
                ),
            ),
        ],
        where_clause=(
            (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr)
            & (AutopoolDestinations.chain_id == autopool.chain.chain_id)
        ),
    )


def _fetch_token_values(
    autopool: AutopoolConstants, token_addresses: list[str], destination_vault_addresses: list[str]
):
    token_value_df = merge_tables_as_df(
        selectors=[
            TableSelector(table=DestinationTokenValues),
            TableSelector(
                table=TokenValues,
                select_fields=[
                    TokenValues.backing,
                    TokenValues.safe_price,
                ],
                join_on=(
                    (TokenValues.chain_id == DestinationTokenValues.chain_id)
                    & (TokenValues.token_address == DestinationTokenValues.token_address)
                    & (TokenValues.block == DestinationTokenValues.block)
                ),
            ),
            TableSelector(
                table=Blocks,
                select_fields=[Blocks.datetime],
                join_on=(Blocks.chain_id == TokenValues.chain_id) & (Blocks.block == TokenValues.block),
            ),
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol],
                join_on=(Tokens.chain_id == TokenValues.chain_id) & (Tokens.token_address == TokenValues.token_address),
            ),
        ],
        where_clause=(TokenValues.token_address.in_(token_addresses))
        & (TokenValues.denominated_in == autopool.base_asset)
        & (Blocks.datetime >= autopool.start_display_date)
        & (DestinationTokenValues.destination_vault_address.in_(destination_vault_addresses)),
    )
    token_value_df["price_return"] = (
        100 * (token_value_df["backing"] - token_value_df["safe_price"]) / token_value_df["backing"]
    )
    return token_value_df


def _render_component_token_safe_price_and_backing(token_value_df: pd.DataFrame):
    component_token_prices_df = token_value_df[
        ["datetime", "symbol", "backing", "safe_price", "price_return"]
    ].drop_duplicates()
    backing_df = (
        component_token_prices_df.pivot(index="datetime", columns="symbol", values="backing").resample("1d").last()
    )
    safe_price_df = (
        component_token_prices_df.pivot(index="datetime", columns="symbol", values="safe_price").resample("1d").last()
    )
    price_return_df = (
        component_token_prices_df.pivot(index="datetime", columns="symbol", values="price_return").resample("1d").last()
    )

    st.plotly_chart(px.line(safe_price_df, title="Safe Price"), use_container_width=True)
    st.plotly_chart(px.line(backing_df, title="Backing"), use_container_width=True)
    st.plotly_chart(px.line(price_return_df, title="Price Return"), use_container_width=True)

    with st.expander("Details"):
        st.markdown(
            """
        Uses the latest *safe_price* and *backing* values for each day.  
        selects either
        - the day's final rebalance plan  
        - the block with the highest number that day  
        """
        )


def _render_underlying_token_spot_and_safe_prices(token_value_df: pd.DataFrame):
    # TODO
    pass


def fetch_and_render_asset_discounts(autopool: AutopoolConstants):

    autopool_destinations_df = _fetch_autopool_dest_token_table(autopool)
    token_value_df = _fetch_token_values(
        autopool,
        autopool_destinations_df["token_address"].unique().tolist(),
        autopool_destinations_df["destination_vault_address"].unique().tolist(),
    )

    _render_component_token_safe_price_and_backing(token_value_df)


if __name__ == "__main__":
    fetch_and_render_asset_discounts(AUTO_USD)
