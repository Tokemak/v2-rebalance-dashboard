import pandas as pd
import numpy as np
import streamlit as st
import psutil
import plotly.express as px
import plotly.graph_objects as go

from mainnet_launch.constants import AutopoolConstants, AUTO_USD
from mainnet_launch.database.schema.full import (
    AutopoolStates,
    Blocks,
    DestinationStates,
    Destinations,
    AutopoolDestinationStates,
)
from mainnet_launch.database.schema.postgres_operations import (
    merge_tables_as_df,
    TableSelector,
)

from mainnet_launch.pages.autopool_exposure.allocation_over_time import _fetch_tvl_by_asset_and_destination


def fetch_nav_per_share_and_total_nav(autopool: AutopoolConstants) -> pd.DataFrame:
    nav_per_share_df = merge_tables_as_df(
        [
            TableSelector(
                table=AutopoolStates,
                select_fields=[AutopoolStates.nav_per_share, AutopoolStates.total_nav],
                join_on=None,
                row_filter=(AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr),
            ),
            TableSelector(
                table=Blocks,
                select_fields=Blocks.datetime,
                join_on=(AutopoolStates.chain_id == Blocks.chain_id) & (AutopoolStates.block == Blocks.block),
            ),
        ],
        where_clause=(AutopoolStates.block > autopool.block_deployed),
        order_by=Blocks.datetime,
    )
    nav_per_share_df = nav_per_share_df.set_index("datetime").resample("1d").last()
    nav_per_share_df.columns = [autopool.name, "NAV"]

    nav_per_share_df["30_day_difference"] = nav_per_share_df[autopool.name].diff(periods=30)
    nav_per_share_df["30_day_annualized_return"] = (
        (nav_per_share_df["30_day_difference"] / nav_per_share_df[autopool.name].shift(30)) * (365 / 30) * 100
    )
    nav_per_share_df["7_day_difference"] = nav_per_share_df[autopool.name].diff(periods=7)
    nav_per_share_df["7_day_annualized_return"] = (
        (nav_per_share_df["7_day_difference"] / nav_per_share_df[autopool.name].shift(7)) * (365 / 7) * 100
    )
    nav_per_share_df["daily_return"] = nav_per_share_df[autopool.name].pct_change()
    nav_per_share_df["7_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=7).mean()
    nav_per_share_df["7_day_MA_annualized_return"] = nav_per_share_df["7_day_MA_return"] * 365 * 100
    nav_per_share_df["30_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=30).mean()
    nav_per_share_df["30_day_MA_annualized_return"] = nav_per_share_df["30_day_MA_return"] * 365 * 100
    return nav_per_share_df


def fetch_key_metrics_data(autopool: AutopoolConstants):
    nav_per_share_df = fetch_nav_per_share_and_total_nav(autopool)

    destination_state_df = merge_tables_as_df(
        selectors=[
            TableSelector(
                table=AutopoolDestinationStates,
            ),
            TableSelector(
                table=Destinations,
                join_on=(
                    (Destinations.destination_vault_address == AutopoolDestinationStates.destination_vault_address)
                    & (Destinations.chain_id == AutopoolDestinationStates.chain_id)
                ),
                select_fields=[Destinations.pool_type, Destinations.underlying_symbol, Destinations.exchange_name],
            ),
            TableSelector(
                table=DestinationStates,
                select_fields=[
                    DestinationStates.incentive_apr,
                    DestinationStates.fee_apr,
                    DestinationStates.base_apr,
                    DestinationStates.fee_plus_base_apr,
                    DestinationStates.lp_token_safe_price,
                    DestinationStates.total_apr_out,
                    DestinationStates.total_apr_in,
                ],
                join_on=(
                    (AutopoolDestinationStates.destination_vault_address == DestinationStates.destination_vault_address)
                    & (AutopoolDestinationStates.chain_id == DestinationStates.chain_id)
                    & (AutopoolDestinationStates.block == DestinationStates.block)
                ),
            ),
            TableSelector(
                table=Blocks,
                join_on=(
                    (AutopoolDestinationStates.block == Blocks.block)
                    & (AutopoolDestinationStates.chain_id == Blocks.chain_id)
                ),
                select_fields=[Blocks.datetime],
            ),
        ],
        # your global filter (you can also push this into a per‑selector row_filter if you prefer)
        where_clause=(AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr)
        & (AutopoolDestinationStates.block > autopool.block_deployed),
        order_by=Blocks.datetime,
        order="asc",
    )

    destination_state_df["unweighted_expected_apr"] = 100 * destination_state_df[
        ["fee_apr", "base_apr", "incentive_apr", "fee_plus_base_apr"]
    ].replace(np.nan, 0).sum(axis=1)

    destination_state_df["safe_tvl_by_destination"] = (
        destination_state_df["lp_token_safe_price"] * destination_state_df["owned_shares"]
    )

    destination_state_df["readable_name"] = destination_state_df.apply(
        lambda row: f"{row['underlying_symbol']} ({row['exchange_name']})", axis=1
    )

    # fluid is not scaled right, should be higher?
    safe_tvl_by_destination = (
        destination_state_df.groupby(["datetime", "readable_name"])[["safe_tvl_by_destination"]]
        .sum()
        .reset_index()
        .pivot(values="safe_tvl_by_destination", index="datetime", columns="readable_name")
    )

    total_safe_tvl_over_time = safe_tvl_by_destination.sum(axis=1)
    portion_alloaction_by_destination_df = safe_tvl_by_destination.div(total_safe_tvl_over_time, axis=0)

    max_apr_by_destination = (
        destination_state_df.groupby(["datetime", "readable_name"])[["unweighted_expected_apr"]]
        .max()
        .reset_index()
        .pivot(values="unweighted_expected_apr", index="datetime", columns="readable_name")
    )
    expected_return_series = (
        (max_apr_by_destination * portion_alloaction_by_destination_df).sum(axis=1).resample("1d").last()
    )
    total_nav_series = nav_per_share_df["NAV"]

    highest_block_and_datetime = destination_state_df[["block", "datetime"]].iloc[-1]

    return (
        nav_per_share_df,
        total_nav_series,
        expected_return_series,
        portion_alloaction_by_destination_df,
        highest_block_and_datetime,
    )


def _apply_default_style(fig: go.Figure) -> None:
    fig.update_traces(line=dict(width=3))
    fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=400,
        width=800,
        font=dict(size=16),
        xaxis_title="",
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray"),
        yaxis=dict(showgrid=True, gridcolor="lightgray"),
    )
    return fig


def _diffReturn(x: list):
    if len(x) < 2:
        return None  # Not enough elements to calculate difference
    return round(x.iloc[-1] - x.iloc[-2], 4)


def _compute_percent_deployed(
    portion_alloaction_by_destination_df: pd.DataFrame, autopool: AutopoolConstants
) -> tuple[float, float]:
    idle_yesterday = portion_alloaction_by_destination_df[f"{autopool.name} (tokemak)"].iloc[-2]
    idle_today = portion_alloaction_by_destination_df[f"{autopool.name} (tokemak)"].iloc[-1]
    return round(100 - (100 * idle_today), 2), round(100 - (100 * idle_yesterday), 2)


def _render_top_level_stats(nav_per_share_df, expected_return_series, portion_alloaction_by_destination_df, autopool):
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric(
        "30-day Rolling APY (%)",
        round(nav_per_share_df["30_day_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["30_day_annualized_return"]),
    )
    col2.metric(
        "30-day MA APY (%)",
        round(nav_per_share_df["30_day_MA_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["30_day_MA_annualized_return"]),
    )
    col3.metric(
        "7-day Rolling APY (%)",
        round(nav_per_share_df["7_day_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["7_day_annualized_return"]),
    )
    col4.metric(
        "7-day MA APY (%)",
        round(nav_per_share_df["7_day_MA_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["7_day_MA_annualized_return"]),
    )
    col5.metric(
        "Expected Annual Return (%)",
        round(expected_return_series.iloc[-1], 2),
        _diffReturn(expected_return_series),
    )

    percent_deployed_today, percent_deployed_yesterday = _compute_percent_deployed(
        portion_alloaction_by_destination_df, autopool
    )

    col6.metric(
        "Percent Deployed",
        round(percent_deployed_today, 2),
        round(percent_deployed_today - percent_deployed_yesterday, 2),
    )
    # might need to do this instead  # nav_per_share_fig.update_layout(yaxis_title="NAV Per Share")


def _render_top_level_charts(
    nav_per_share_df, autopool: AutopoolConstants, total_nav_series, expected_return_series, price_return_series
):
    nav_per_share_fig = _apply_default_style(px.line(nav_per_share_df, y=autopool.name, title="NAV Per Share"))
    price_return_fig = _apply_default_style(
        px.line(price_return_series, title="Autopool Estimated Price Return (%)", labels={"value": "Price Return (%)"})
    )
    price_return_fig.update_traces(showlegend=False)

    nav_fig = _apply_default_style(
        px.line(total_nav_series, title="Total NAV", labels={"value": autopool.base_asset_symbol})
    )
    nav_fig.update_traces(showlegend=False)

    annualized_30d_return_fig = _apply_default_style(
        px.line(nav_per_share_df, y="30_day_annualized_return", title="30-day Annualized Return (%)")
    )
    annualized_7d_return_fig = _apply_default_style(
        px.line(nav_per_share_df, y="7_day_annualized_return", title="7-day Rolling Annualized Return (%)")
    )

    annualized_7d_ma_return_fig = _apply_default_style(
        px.line(nav_per_share_df, y="7_day_MA_annualized_return", title="7-day MA Annualized Return (%)")
    )
    annualized_30d_ma_return_fig = _apply_default_style(
        px.line(nav_per_share_df, y="30_day_MA_annualized_return", title="30-day MA Annualized Return (%)")
    )
    uwcr_return_fig = _apply_default_style(
        px.line(expected_return_series, title="Expected Annualized Return (%)", labels={"value": "Expected Return (%)"})
    )
    uwcr_return_fig.update_traces(showlegend=False)

    st.markdown("<div style='margin: 7em 0;'></div>", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.plotly_chart(nav_per_share_fig, use_container_width=True)
    with col2:
        st.plotly_chart(nav_fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)

    with col1:
        st.plotly_chart(annualized_30d_return_fig, use_container_width=True)
    with col2:
        st.plotly_chart(annualized_30d_ma_return_fig, use_container_width=True)
    with col3:
        st.plotly_chart(annualized_7d_return_fig, use_container_width=True)

    st.markdown("<div style='margin: 7em 0;'></div>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("7-day MA Annualized Return (%)")
        st.plotly_chart(annualized_7d_ma_return_fig, use_container_width=True)
    with col2:
        st.subheader("Expected Annualized Return (%)")
        st.plotly_chart(uwcr_return_fig, use_container_width=True)
    with col3:
        st.subheader("Autopool Estimated Price Return")
        st.plotly_chart(price_return_fig, use_container_width=True)


def _fetch_price_return(autopool: AutopoolConstants):
    safe_value_by_destination, safe_value_by_asset, backing_value_by_destination = _fetch_tvl_by_asset_and_destination(
        autopool
    )
    del safe_value_by_asset

    backing_value_by_destination = backing_value_by_destination.replace(0, np.nan)
    safe_value_by_destination = safe_value_by_destination.replace(0, np.nan)

    autopool_safe_value = safe_value_by_destination.sum(axis=1)
    autopool_backing_value = backing_value_by_destination.sum(axis=1)

    autopool_price_return = 100 * (autopool_backing_value - autopool_safe_value) / autopool_backing_value

    if autopool == AUTO_USD:
        autopool_price_return = autopool_price_return[autopool_price_return.index > "4-8-2025"].copy()

    return autopool_price_return


def fetch_and_render_key_metrics_data(autopool: AutopoolConstants):
    (
        nav_per_share_df,
        total_nav_series,
        expected_return_series,
        portion_alloaction_by_destination_df,
        highest_block_and_datetime,
    ) = fetch_key_metrics_data(autopool)

    # autopool_price_return = 100 * (autopool_backing_value - autopool_safe_value) / autopool_backing_value
    weighted_price_return_series = _fetch_price_return(autopool)

    st.header(f"{autopool.name} Key Metrics")
    _render_top_level_stats(nav_per_share_df, expected_return_series, portion_alloaction_by_destination_df, autopool)
    _render_top_level_charts(
        nav_per_share_df, autopool, total_nav_series, expected_return_series, weighted_price_return_series
    )

    with st.expander("See explanation for Key Metrics"):
        st.write(
            """
        - NAV per share: The Net Asset Value per share over time.
        - NAV: The total Net Asset Value of the Autopool.
        - 30-day and 7-day Annualized Returns: Percent annual return derived from NAV Per Share changes. 
        - Expected Annualized Return: Projected percent annual return based on current allocations of the Autopool.
        """
        )

    st.markdown(
        f"""
        **Highest Block Used**: `{highest_block_and_datetime[0]}`  
        **Timestamp**: `{highest_block_and_datetime[1]}`  
        **Chain**: `{autopool.chain.name}`
        """
    )

    memory_used = psutil.Process().memory_info().rss / (1024**2)

    st.write(f"Memory Usage: {memory_used:.2f} MB")


if __name__ == "__main__":
    from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, AUTO_ETH, BASE_ETH, DINERO_ETH, AUTO_USD

    # fetch_and_render_key_metrics_data(AUTO_ETH)

    fetch_and_render_key_metrics_data(AUTO_USD)
    pass
