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
    df = merge_tables_as_df(
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
    df["destination_readable_name"] = df["underlying_name"] + " (" + df["exchange_name"] + ") "
    return df


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
    token_value_df["safe_spot_spread"] = (
        100 * (token_value_df["spot_price"] - token_value_df["safe_price"]) / token_value_df["safe_price"]
    )

    return token_value_df


def _render_component_token_safe_price_and_backing(token_value_df: pd.DataFrame):

    backing_df = (
        token_value_df[["datetime", "symbol", "backing"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="backing")
        .resample("1d")
        .last()
    )
    safe_price_df = (
        token_value_df[["datetime", "symbol", "safe_price"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="safe_price")
        .resample("1d")
        .last()
    )
    price_return_df = (
        token_value_df[["datetime", "symbol", "price_return"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="price_return")
    )

    safe_spot_spread_df = (
        token_value_df[["datetime", "destination_readable_name", "safe_spot_spread"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="destination_readable_name", values="safe_spot_spread")
    )

    st.plotly_chart(
        px.line(price_return_df.resample("1d").last(), title="Daily Price Return (backing - safe) / safe "),
        use_container_width=True,
    )
    st.plotly_chart(
        px.scatter(price_return_df, title="All Price Return (backing - safe) / safe "), use_container_width=True
    )
    st.plotly_chart(
        px.line(safe_spot_spread_df.resample("1d").last(), title="Daily Safe Spot Spread (spot_price - safe) / safe"),
        use_container_width=True,
    )
    st.plotly_chart(
        px.scatter(safe_spot_spread_df, title="All time Safe Spot Spread (spot_price - safe) / safe"),
        use_container_width=True,
    )

    st.subheader("Safe Spot Spread (bps) Stats")
    st.markdown("Positive Means Spot > Safe")
    st.markdown("Negative Means Safe > Spot")
    st.markdown("Mean, 10th percentile, 90th percentile")
    mean_safe_spot_spread = _compute_all_time_30_and_7_day_means(token_value_df)
    st.dataframe(mean_safe_spot_spread)
    st.plotly_chart(px.line(safe_price_df, title="Daily Safe Price"), use_container_width=True)
    st.plotly_chart(px.line(backing_df, title="Daily Backing"), use_container_width=True)


def _compute_all_time_30_and_7_day_means(token_value_df: pd.DataFrame):
    # token_value_df_no_duplicate_destinations = token_value_df.drop_duplicates(
    #     subset=["destination_readable_name", "token_address"]
    # )
    # TODO consider how to remove the duplicate destiatnions here, we don't need duplciate destinations
    safe_spot_spread_df = (
        token_value_df[["datetime", "destination_readable_name", "safe_spot_spread"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="destination_readable_name", values="safe_spot_spread")
    ) * 100

    latest = pd.Timestamp.utcnow()

    mean_df = pd.DataFrame(
        {
            "All Time Average Safe Spot Spread": safe_spot_spread_df.mean(),
            "Last 30 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=30)
            ].mean(),
            "Last 7 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=7)
            ].mean(),
        }
    )

    bottom_10th = pd.DataFrame(
        {
            "All Time Average Safe Spot Spread": safe_spot_spread_df.quantile(0.1),
            "Last 30 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=30)
            ].quantile(0.1),
            "Last 7 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=7)
            ].quantile(0.1),
        }
    )

    top_90th = pd.DataFrame(
        {
            "All Time Average Safe Spot Spread": safe_spot_spread_df.quantile(0.9),
            "Last 30 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=30)
            ].quantile(0.9),
            "Last 7 Days Average Safe Spot Spread": safe_spot_spread_df.loc[
                safe_spot_spread_df.index >= latest - pd.Timedelta(days=7)
            ].quantile(0.9),
        }
    )

    safe_spread_descriptive_stats_df = pd.DataFrame(
        {
            col: [
                (
                    round(mean, 2),
                    round(percentile_10, 2),
                    round(percentile_90, 2),
                )
                for count, mean, percentile_10, percentile_90 in zip(
                    mean_df[col],
                    bottom_10th[col],
                    top_90th[col],
                )
            ]
            for col in mean_df.columns
        },
        index=mean_df.index,
    )
    return safe_spread_descriptive_stats_df


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

    token_value_df["destination_readable_name"] = token_value_df["destination_vault_address"].map(
        autopool_destinations_df.set_index("destination_vault_address")["destination_readable_name"].to_dict()
    )
    token_value_df["underlying_name"] = token_value_df["destination_vault_address"].map(
        autopool_destinations_df.set_index("destination_vault_address")["underlying_name"].to_dict()
    )

    token_value_df["destination_readable_name"] = (
        token_value_df["symbol"] + "\t" + token_value_df["destination_readable_name"]
    )

    _render_component_token_safe_price_and_backing(token_value_df)


if __name__ == "__main__":
    fetch_and_render_asset_discounts(AUTO_USD)
