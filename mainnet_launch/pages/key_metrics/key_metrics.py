import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
import psutil
import os


from mainnet_launch.constants import AutopoolConstants, AUTO_LRT, PRODUCTION_LOG_FILE_NAME
from mainnet_launch.data_fetching.get_state_by_block import build_blocks_to_use
from mainnet_launch.pages.key_metrics.fetch_nav_per_share import fetch_nav_per_share
from mainnet_launch.pages.autopool_diagnostics.fetch_destination_summary_stats import fetch_destination_summary_stats
from mainnet_launch.destinations import get_destination_details


def fetch_key_metrics_data(autopool: AutopoolConstants):
    blocks = build_blocks_to_use(autopool.chain)
    nav_per_share_df = fetch_nav_per_share(autopool)

    compositeReturn_out_df = fetch_destination_summary_stats(autopool, "compositeReturn")
    priceReturn_df = fetch_destination_summary_stats(autopool, "priceReturn")

    pricePerShare_df = fetch_destination_summary_stats(autopool, "pricePerShare")
    ownedShares_df = fetch_destination_summary_stats(autopool, "ownedShares")
    allocation_df = pricePerShare_df * ownedShares_df
    total_nav_series = allocation_df.sum(axis=1)

    baseApr_df = fetch_destination_summary_stats(autopool, "baseApr")
    feeApr_df = fetch_destination_summary_stats(autopool, "feeApr")
    incentiveApr_df = fetch_destination_summary_stats(autopool, "incentiveApr")
    portion_df = allocation_df.div(total_nav_series, axis=0)

    uwcr_df = 100 * (baseApr_df + feeApr_df + incentiveApr_df)
    uwcr_df["Expected_Return"] = (uwcr_df.fillna(0) * portion_df.fillna(0)).sum(axis=1)

    key_metric_data = {
        "nav_per_share_df": nav_per_share_df,
        "uwcr_df": uwcr_df,
        "allocation_df": allocation_df,
        "compositeReturn_df": compositeReturn_out_df,
        "total_nav_df": total_nav_series,
        "priceReturn_df": priceReturn_df,
        "blocks": blocks,
    }
    return key_metric_data


def get_memory_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    return mem_info.rss / (1024**2)


def fetch_and_render_key_metrics_data(autopool: AutopoolConstants):
    key_metric_data = fetch_key_metrics_data(autopool)
    _show_key_metrics(key_metric_data, autopool)
    st.write(f"Memory Usage: {get_memory_usage():.2f} MB")


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


def _diffReturn(x: list):
    if len(x) < 2:
        return None  # Not enough elements to calculate difference
    return round(x.iloc[-1] - x.iloc[-2], 4)


def _get_percent_deployed(allocation_df: pd.DataFrame, autopool: AutopoolConstants) -> tuple[float, float]:

    daily_allocation_df = allocation_df.resample("1D").last()
    destinations = get_destination_details(autopool)
    autopool_name = [dest.vault_name for dest in destinations if dest.vaultAddress == autopool.autopool_eth_addr][0]

    tvl_according_to_allocation_df = float(daily_allocation_df.iloc[-1].sum())

    tvl_in_idle = float(daily_allocation_df[autopool_name].iloc[-1])
    percent_deployed_today = 100 * ((tvl_according_to_allocation_df - tvl_in_idle) / tvl_according_to_allocation_df)

    tvl_according_to_allocation_df = float(daily_allocation_df.iloc[-2].sum())
    tvl_in_idle = float(daily_allocation_df[autopool_name].iloc[-2])
    percent_deployed_yesterday = 100 * ((tvl_according_to_allocation_df - tvl_in_idle) / tvl_according_to_allocation_df)

    return round(percent_deployed_yesterday, 2), round(percent_deployed_today, 2)


def _show_key_metrics(key_metric_data: dict[str, pd.DataFrame], autopool: AutopoolConstants):
    st.header(f"{autopool.name} Key Metrics")
    nav_per_share_df = key_metric_data["nav_per_share_df"]
    uwcr_df = key_metric_data["uwcr_df"]
    allocation_df = key_metric_data["allocation_df"]
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

    percent_deployed_yesterday, percent_deployed_today = _get_percent_deployed(allocation_df, autopool)

    col6.metric(
        "Percent Deployed", percent_deployed_today, round(percent_deployed_today - percent_deployed_yesterday, 2)
    )

    nav_per_share_fig = px.line(nav_per_share_df, y=autopool.name, title=" ")
    _apply_default_style(nav_per_share_fig)
    nav_per_share_fig.update_layout(yaxis_title="NAV Per Share")

    # weighted price return
    total_nav_series = key_metric_data["allocation_df"].sum(axis=1)
    portion_df = key_metric_data["allocation_df"].div(total_nav_series, axis=0)
    # multiply by 100 to get % value
    wpReturn = (key_metric_data["priceReturn_df"].fillna(0) * portion_df.fillna(0)).sum(axis=1) * 100
    wpReturn = wpReturn.resample("1D").last()
    wpReturn = wpReturn.rename("wpr")
    wpr_fig = px.line(wpReturn, y="wpr", title=" ")
    _apply_default_style(wpr_fig)
    wpr_fig.update_layout(yaxis_title="Autopool Estimated Price Return (%)")

    total_nav_df = key_metric_data["total_nav_df"]

    nav_fig = px.line(total_nav_df, title=" ")
    _apply_default_style(nav_fig)
    nav_fig.update_layout(yaxis_title="Total Nav")
    nav_fig.update_layout(showlegend=False)

    annualized_30d_return_fig = px.line(nav_per_share_df, y="30_day_annualized_return", title=" ")
    _apply_default_style(annualized_30d_return_fig)
    annualized_30d_return_fig.update_layout(yaxis_title="30-day Annualized Return (%)")

    annualized_7d_return_fig = px.line(nav_per_share_df, y="7_day_annualized_return", title=" ")
    _apply_default_style(annualized_7d_return_fig)
    annualized_7d_return_fig.update_layout(yaxis_title="7-day Rolling Annualized Return (%)")

    annualized_7d_ma_return_fig = px.line(nav_per_share_df, y="7_day_MA_annualized_return", title=" ")
    _apply_default_style(annualized_7d_ma_return_fig)
    annualized_7d_ma_return_fig.update_layout(yaxis_title="7-day MA Annualized Return (%)")

    annualized_30d_ma_return_fig = px.line(nav_per_share_df, y="30_day_MA_annualized_return", title=" ")
    _apply_default_style(annualized_30d_ma_return_fig)
    annualized_30d_ma_return_fig.update_layout(yaxis_title="30-day MA Annualized Return (%)")

    uwcr_return_fig = px.line(uwcr_df, y="Expected_Return", title=" ")
    _apply_default_style(uwcr_return_fig)
    uwcr_return_fig.update_layout(yaxis_title="Expected Annualized Return (%)")

    # Insert gap
    st.markdown("<div style='margin: 7em 0;'></div>", unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.subheader("NAV per share")
        st.plotly_chart(nav_per_share_fig, use_container_width=True)
    with col2:
        st.subheader("NAV")
        st.plotly_chart(nav_fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("30-day Rolling Annualized Return (%)")
        st.plotly_chart(annualized_30d_return_fig, use_container_width=True)
    with col2:
        st.subheader("30-day MA Annualized Return (%)")
        st.plotly_chart(annualized_30d_ma_return_fig, use_container_width=True)
    with col3:
        st.subheader("7-day Rolling Annualized Return (%)")
        st.plotly_chart(annualized_7d_return_fig, use_container_width=True)

    # Insert gap
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
        st.plotly_chart(wpr_fig, use_container_width=True)

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
    highest_block_used = key_metric_data["blocks"][-1]
    highest_timestamp = allocation_df.index[-1]
    st.text(f"Highest block and time used {highest_timestamp=} {highest_block_used=} {autopool.chain.name=}")

    st.markdown("---")  # Add a horizontal line for separation

    # Section for Log File Download
    st.header("Download Logs")

    if os.path.exists(PRODUCTION_LOG_FILE_NAME):
        try:
            with open(PRODUCTION_LOG_FILE_NAME, "r") as log_file:
                log_content = log_file.read()

            st.download_button(
                label="📥 Download Log File",
                data=log_content,
                file_name="data_caching.log",
                mime="text/plain",
                key="download_log",
            )
        except Exception as e:
            st.error(f"An error occurred while reading the log file: {e}")
    else:
        st.warning("Log file not found. Please ensure that logging is properly configured.")


if __name__ == "__main__":
    from mainnet_launch.constants import (
        STREAMLIT_IN_MEMORY_CACHE_TIME,
        AutopoolConstants,
        ALL_AUTOPOOLS,
        AUTO_ETH,
        BASE_ETH,
    )

    fetch_and_render_key_metrics_data(AUTO_LRT)
