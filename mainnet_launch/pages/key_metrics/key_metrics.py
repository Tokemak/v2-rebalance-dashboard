import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
import psutil
import os
import datetime


from mainnet_launch.constants import AutopoolConstants, PRODUCTION_LOG_FILE_NAME, STARTUP_LOG_FILE
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.destinations import get_destination_details

from mainnet_launch.database.schema.full import (
    AutopoolStates,
    Blocks,
    DestinationStates,
    Destinations,
    AutopoolDestinationStates,
)
from mainnet_launch.database.schema.postgres_operations import natural_left_right_using_where, get_full_table_as_df


def fetch_nav_per_share(autopool: AutopoolConstants) -> pd.DataFrame:
    nav_per_share_df = natural_left_right_using_where(
        AutopoolStates,
        Blocks,
        using=[AutopoolStates.chain_id, AutopoolStates.block],
        where_clause=AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr,
    )[["nav_per_share", "datetime"]]

    nav_per_share_df = nav_per_share_df.set_index("datetime")
    nav_per_share_df.columns = [autopool.name]
    nav_per_share_df = nav_per_share_df.resample("1D").last()

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
    nav_per_share_df = fetch_nav_per_share(autopool)

    destination_state_df = natural_left_right_using_where(
        DestinationStates,
        AutopoolDestinationStates,
        using=[DestinationStates.chain_id, DestinationStates.block, DestinationStates.destination_vault_address],
        where_clause=AutopoolDestinationStates.autopool_vault_address == autopool.autopool_eth_addr,
    )

    destination_state_df = pd.merge(
        destination_state_df, get_full_table_as_df(Destinations), on=["destination_vault_address", "chain_id"]
    )
    destination_state_df = pd.merge(
        destination_state_df,
        get_full_table_as_df(Blocks, where_clause=Blocks.chain_id == autopool.chain.chain_id),
        on=["block", "chain_id"],
    )

    # must groupby destination vault address intead of by destiantion name because destinations are added and removed over time
    composite_return_out_df = destination_state_df.pivot(
        index="datetime", values="total_apr_out", columns="destination_vault_address"
    )
    price_return_df = destination_state_df.pivot(
        index="datetime", values="price_return", columns="destination_vault_address"
    )
    owned_shares_df = destination_state_df.pivot(index="datetime", values="amount", columns="destination_vault_address")
    price_per_share_df = destination_state_df.pivot(
        index="datetime", values="price_per_share", columns="destination_vault_address"
    )

    allocation_df = price_per_share_df * owned_shares_df
    total_nav_series = allocation_df.sum(axis=1)
    portion_df = allocation_df / total_nav_series

    destination_state_df["unweighted_apr"] = destination_state_df[["fee_apr", "base_apr", "incentive_apr"]].sum(axis=1)

    uwcr_df = destination_state_df.pivot(index="datetime", values="unweighted_apr", columns="destination_vault_address")
    expected_return_series = (portion_df.fillna(0) * uwcr_df.fillna(0)).sum(axis=1)
    weighted_price_return_series = (portion_df.fillna(0) * price_return_df.fillna(0)).sum(axis=1)

    highest_block_and_datetime = destination_state_df[["block", "datetime"]].iloc[-1]

    return (
        nav_per_share_df,
        composite_return_out_df,
        price_return_df,
        total_nav_series,
        uwcr_df,
        expected_return_series,
        allocation_df,
        weighted_price_return_series,
        highest_block_and_datetime,
    )


def get_memory_usage() -> float:
    """Returns curernt application memory usages in mb"""
    return psutil.Process().memory_info().rss / (1024**2)


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


def _compute_percent_deployed(allocation_df: pd.DataFrame, autopool: AutopoolConstants) -> tuple[float, float]:
    tvl_yesterday = allocation_df.iloc[-2].sum()
    idle_yesterday = allocation_df[autopool.autopool_eth_addr].iloc[-2]

    percent_deployed_yesterday = 100 * idle_yesterday / tvl_yesterday

    tvl_today = allocation_df.iloc[-1].sum()
    idle_today = allocation_df[autopool.autopool_eth_addr].iloc[-1]

    percent_deployed_today = 100 * idle_today / tvl_today

    return percent_deployed_today, percent_deployed_yesterday


def _render_top_level_stats(nav_per_share_df, uwcr_df, allocation_df, autopool):
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
        round(uwcr_df["Expected_Return"].iloc[-1], 2),
        _diffReturn(uwcr_df["Expected_Return"]),
    )

    percent_deployed_today, percent_deployed_yesterday = _compute_percent_deployed(allocation_df, autopool)

    col6.metric(
        "Percent Deployed", percent_deployed_today, round(percent_deployed_today - percent_deployed_yesterday, 2)
    )
    # might need to do this instead  # nav_per_share_fig.update_layout(yaxis_title="NAV Per Share")


def _render_top_level_charts(nav_per_share_df, autopool, total_nav_series, expected_return_series, price_return_series):
    nav_per_share_fig = _apply_default_style(px.line(nav_per_share_df, y=autopool.name, title="NAV Per Share"))
    price_return_fig = _apply_default_style(px.line(price_return_series, title="Autopool Estimated Price Return (%)"))
    nav_fig = _apply_default_style(px.line(total_nav_series, title="Total NAV"))

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
        px.line(expected_return_series, y="Expected_Return", title="Expected Annualized Return (%)")
    )

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


def fetch_and_render_key_metrics_data(autopool: AutopoolConstants):
    (
        nav_per_share_df,
        total_nav_series,
        uwcr_df,
        expected_return_series,
        allocation_df,
        price_return_series,
        highest_block_and_datetime,
    ) = fetch_key_metrics_data(autopool)

    st.header(f"{autopool.name} Key Metrics")
    _render_top_level_stats(nav_per_share_df, uwcr_df, allocation_df, autopool)
    _render_top_level_charts(nav_per_share_df, autopool, total_nav_series, expected_return_series, price_return_series)

    with st.expander("See explanation for Key Metrics"):
        st.write(
            """
        This section displays the key performance indicators for the Autopool:
        - NAV per share: The Net Asset Value per share over time.
        - NAV: The total Net Asset Value of the Autopool.
        - 30-day and 7-day Annualized Returns: Percent annual return derived from NAV per share changes. 
        - Expected Annualized Return: Projected percent annual return based on current allocations of the Autopool.
        """
        )

    highest_block_and_datetime
    st.markdown(
        f"""
        **Highest Block Used**: `{highest_block_and_datetime[0]}`  
        **Timestamp**: `{highest_block_and_datetime[1]}`  
        **Chain**: `{autopool.chain.name}`
        """
    )

    st.write(f"Memory Usage: {get_memory_usage():.2f} MB")


# def render_download_production_button():
#     if os.path.exists(PRODUCTION_LOG_FILE_NAME):
#         try:
#             with open(PRODUCTION_LOG_FILE_NAME, "r") as log_file:
#                 log_content = log_file.read()

#             st.download_button(
#                 label="📥 Download Log File",
#                 data=log_content,
#                 file_name=PRODUCTION_LOG_FILE_NAME,
#                 mime="text/plain",
#                 key="download_production_log",
#             )
#         except Exception as e:
#             st.error(f"An error occurred while reading the log file: {e}")
#     else:
#         st.warning("Log file not found. Please ensure that logging is properly configured.")


# def render_download_startup_log_button():
#     if os.path.exists(STARTUP_LOG_FILE):
#         try:
#             with open(STARTUP_LOG_FILE, "r") as log_file:
#                 log_content = log_file.read()

#             st.download_button(
#                 label="📥 Download Startup File",
#                 data=log_content,
#                 file_name="startup.txt",
#                 mime="text/plain",
#                 key="download_startup_log",
#             )
#         except Exception as e:
#             st.error(f"An error occurred while reading the log file: {e}")
#     else:
#         st.warning("Log file not found. Please ensure that logging is properly configured.")


if __name__ == "__main__":
    from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS, AUTO_ETH, BASE_ETH, DINERO_ETH

    fetch_and_render_key_metrics_data(AUTO_ETH)
    pass
