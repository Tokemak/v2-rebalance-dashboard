from __future__ import annotations


import plotly.express as px
import pandas as pd
import streamlit as st

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.postgres_operations import *


# consider this as a view
def _fetch_autopool_dest_token_table(autopool: AutopoolConstants) -> pd.DataFrame:
    # can be replaced by the view
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

    # Balancer Aave USDC-Aave GHO (balancerV3)
    # Balancer Aave GHO-USR (balancerV3)
    # have tiny maybe rounding differences, not sure why, TODO
    # might have to do with pricing as USDC instead of aUSDC?
    # not certain
    # figure out why later
    # eg {-0.08380000000000054, -0.08369999999999767}
    # {-0.03901209374906236, -0.03731156658563722}
    # {0.010102899532167506, 0.010202928240409647}

    price_return_df = (
        token_value_df[["datetime", "symbol", "price_return"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="price_return")
    )
    st.plotly_chart(
        px.scatter(price_return_df, title="All Time % Price Return (backing - safe) / safe "), use_container_width=True
    )

    safe_spot_spread_df = (
        token_value_df.groupby(
            [
                "datetime",
                "token_destination_readable_name",
            ]
        )[["safe_spot_spread"]]
        .first()
        .reset_index()
        .pivot(index="datetime", columns="token_destination_readable_name", values="safe_spot_spread")
    )

    st.plotly_chart(
        px.scatter(safe_spot_spread_df, title="All Time % 100 * (spot_price - safe) / safe"),
        use_container_width=True,
    )

    with st.expander("(click here) Safe Spot Bps Spread Descriptive Stats"):
        filter_text = st.text_input("Filter By Readable Name:", "")
        cols = [c for c in safe_spot_spread_df.columns if filter_text in c]

        mean_df, abs_mean_df, percentile_10_df, percentile_90_df = _compute_all_time_30_and_7_day_means(
            safe_spot_spread_df[cols]
        )
        st.markdown("10_000 * (spot - safe) / safe")
        st.markdown("Positive Means Spot > Safe")
        st.markdown("Negative Means Safe > Spot\n\n")
        st.markdown("All values are in bps \n")
        st.markdown("Mean")
        st.dataframe(mean_df, use_container_width=True)

        st.markdown("Absolute Mean")
        st.dataframe(abs_mean_df, use_container_width=True)

        st.markdown("Bottom 10th Percentile")
        st.dataframe(percentile_10_df, use_container_width=True)

        st.markdown("Top 90th Percentile")
        st.dataframe(percentile_90_df, use_container_width=True)

    backing_df = (
        token_value_df[["datetime", "symbol", "backing"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="backing")
    )

    safe_price_df = (
        token_value_df[["datetime", "symbol", "safe_price"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="safe_price")
    )

    spot_price_df = (
        token_value_df[["datetime", "token_destination_readable_name", "safe_price"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="token_destination_readable_name", values="safe_price")
    )

    st.plotly_chart(px.line(spot_price_df, title="All Time Spot Price"), use_container_width=True)
    st.plotly_chart(px.line(safe_price_df, title="All Time Safe Price"), use_container_width=True)
    st.plotly_chart(px.line(backing_df, title="All Time Backing"), use_container_width=True)


def _compute_all_time_30_and_7_day_means(safe_spot_spread_df: pd.DataFrame):
    latest = pd.Timestamp.utcnow()
    all_time_df = safe_spot_spread_df * 100
    last_7_days_df = safe_spot_spread_df.loc[safe_spot_spread_df.index >= latest - pd.Timedelta(days=7)] * 100
    last_30_days_df = safe_spot_spread_df.loc[safe_spot_spread_df.index >= latest - pd.Timedelta(days=30)] * 100

    mean_df = pd.DataFrame(
        {
            "Average (All Time)": all_time_df.mean(),
            "Average (Last 30 Days)": last_30_days_df.mean(),
            "Average (Last 7 Days)": last_7_days_df.mean(),
        }
    )

    abs_mean_df = pd.DataFrame(
        {
            "Absolute Average (All Time)": all_time_df.abs().mean(),
            "Absolute Average (Last 30 Days)": last_30_days_df.abs().mean(),
            "Absolute Average (Last 7 Days)": last_7_days_df.abs().mean(),
        }
    )

    percentile_10_df = pd.DataFrame(
        {
            "10th Percentile (All Time)": all_time_df.quantile(0.10),
            "10th Percentile (Last 30 Days)": last_30_days_df.quantile(0.10),
            "10th Percentile (Last 7 Days)": last_7_days_df.quantile(0.10),
        }
    )

    percentile_90_df = pd.DataFrame(
        {
            "90th Percentile (All Time)": all_time_df.quantile(0.90),
            "90th Percentile (Last 30 Days)": last_30_days_df.quantile(0.90),
            "90th Percentile (Last 7 Days)": last_7_days_df.quantile(0.90),
        }
    )

    return mean_df, abs_mean_df, percentile_10_df, percentile_90_df


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

    token_value_df["token_destination_readable_name"] = (
        token_value_df["symbol"] + "\t" + token_value_df["destination_readable_name"]
    )

    _render_component_token_safe_price_and_backing(token_value_df)


if __name__ == "__main__":
    fetch_and_render_asset_discounts(BASE_USD)
