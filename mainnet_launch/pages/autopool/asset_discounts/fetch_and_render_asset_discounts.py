import plotly.express as px
import pandas as pd
import streamlit as st

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.postgres_operations import *


def _fetch_autopool_destination_tokens(autopool: AutopoolConstants) -> pd.DataFrame:
    # consider this as a view
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
            TableSelector(
                table=DestinationTokenValues,
                select_fields=[DestinationTokenValues.spot_price, DestinationTokenValues.destination_vault_address],
            ),
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
            # I suspect that this is not needed, can do a dict map instead in python
            TableSelector(
                table=Tokens,
                select_fields=[Tokens.symbol],
                join_on=(Tokens.chain_id == TokenValues.chain_id) & (Tokens.token_address == TokenValues.token_address),
            ),
        ],
        where_clause=(TokenValues.token_address.in_(token_addresses))
        & (TokenValues.denominated_in == autopool.base_asset)
        & (Blocks.datetime >= autopool.get_display_date())
        & (DestinationTokenValues.destination_vault_address.in_(destination_vault_addresses)),
    )
    token_value_df["price_return"] = (
        100 * (token_value_df["backing"] - token_value_df["safe_price"]) / token_value_df["backing"]
    )
    token_value_df["safe_spot_spread"] = (
        100 * (token_value_df["spot_price"] - token_value_df["safe_price"]) / token_value_df["safe_price"]
    )

    return token_value_df


def _fetch_lp_token_spot_prices(autopool: AutopoolConstants, destination_readable_map: dict[str, str]) -> pd.DataFrame:
    lp_token_price_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                table=DestinationStates,
                select_fields=[
                    DestinationStates.destination_vault_address,
                    DestinationStates.lp_token_spot_price,
                    DestinationStates.lp_token_safe_price,
                ],
            ),
            TableSelector(
                table=AutopoolDestinations,
                select_fields=[AutopoolDestinations.autopool_vault_address],
                join_on=(
                    (AutopoolDestinations.destination_vault_address == DestinationStates.destination_vault_address)
                    & (AutopoolDestinations.chain_id == DestinationStates.chain_id)
                ),
            ),
            TableSelector(
                table=Blocks,
                select_fields=[Blocks.datetime],
                join_on=(DestinationStates.block == Blocks.block) & (DestinationStates.chain_id == Blocks.chain_id),
            ),
        ],
        where_clause=(
            (AutopoolDestinations.autopool_vault_address == autopool.autopool_eth_addr)
            & (DestinationStates.chain_id == autopool.chain.chain_id)
            & (Blocks.datetime >= autopool.get_display_date())
        ),
        order_by=Blocks.datetime,
    )

    if lp_token_price_df.empty:
        return lp_token_price_df

    lp_token_price_df["destination_readable_name"] = lp_token_price_df["destination_vault_address"].map(
        destination_readable_map
    )
    lp_token_price_df["lp_token_spot_price"] = pd.to_numeric(lp_token_price_df["lp_token_spot_price"])
    lp_token_price_df["lp_token_safe_price"] = pd.to_numeric(lp_token_price_df["lp_token_safe_price"])

    safe_price = lp_token_price_df["lp_token_safe_price"]
    lp_token_price_df["lp_token_percent_discount"] = (
        100 * (safe_price - lp_token_price_df["lp_token_spot_price"]) / safe_price
    )
    lp_token_price_df.loc[safe_price == 0, "lp_token_percent_discount"] = None

    return lp_token_price_df


def _render_component_token_safe_price_and_backing(token_value_df: pd.DataFrame):
    price_return_df = (
        token_value_df[["datetime", "symbol", "price_return"]]
        .drop_duplicates()
        .pivot(index="datetime", columns="symbol", values="price_return")
    )
    st.plotly_chart(
        px.scatter(price_return_df, title="All Time % Price Return (backing - safe) / backing "),
        use_container_width=True,
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
    pass


def _render_lp_token_spot_prices(lp_token_price_df: pd.DataFrame):
    st.subheader("LP Token Spot Discount (Root Price Oracle)")

    if lp_token_price_df.empty:
        st.info("No LP token price data available yet.")
        return

    spot_price_df = (
        lp_token_price_df.dropna(subset=["destination_readable_name", "lp_token_spot_price"])
        .pivot(index="datetime", columns="destination_readable_name", values="lp_token_spot_price")
    )
    if not spot_price_df.empty:
        st.plotly_chart(
            px.line(spot_price_df, title="LP Token Spot Price Over Time"),
            use_container_width=True,
        )

    discount_df = (
        lp_token_price_df.dropna(subset=["destination_readable_name", "lp_token_percent_discount"])
        .pivot(index="datetime", columns="destination_readable_name", values="lp_token_percent_discount")
    )
    if not discount_df.empty:
        st.plotly_chart(
            px.line(discount_df, title="LP Token Spot Discount vs Safe (%)"),
            use_container_width=True,
        )


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


# @time_decorator
def fetch_and_render_asset_discounts(autopool: AutopoolConstants):
    autopool_destinations_df = _fetch_autopool_destination_tokens(autopool)  # fast enough

    destination_readable_map = (
        autopool_destinations_df.set_index("destination_vault_address")["destination_readable_name"].to_dict()
    )

    token_value_df = _fetch_token_values(
        autopool,
        autopool_destinations_df["token_address"].unique().tolist(),
        autopool_destinations_df["destination_vault_address"].unique().tolist(),
    )

    token_value_df["destination_readable_name"] = token_value_df["destination_vault_address"].map(
        destination_readable_map
    )
    token_value_df["underlying_name"] = token_value_df["destination_vault_address"].map(
        autopool_destinations_df.set_index("destination_vault_address")["underlying_name"].to_dict()
    )

    token_value_df["token_destination_readable_name"] = (
        token_value_df["symbol"] + "\t" + token_value_df["destination_readable_name"]
    )

    lp_token_price_df = _fetch_lp_token_spot_prices(autopool, destination_readable_map)
    _render_lp_token_spot_prices(lp_token_price_df)

    # profile_function(_render_component_token_safe_price_and_backing, token_value_df)
    _render_component_token_safe_price_and_backing(token_value_df)


if __name__ == "__main__":

    from mainnet_launch.constants import *
    import streamlit as st
    import datetime

    st.session_state[SessionState.RECENT_START_DATE] = pd.Timestamp(
        datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=90)
    ).isoformat()

    # profile_function(fetch_and_render_asset_discounts, AUTO_USD)
    fetch_and_render_asset_discounts(AUTO_USD)
