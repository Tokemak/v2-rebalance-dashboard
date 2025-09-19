import pandas as pd
import streamlit as st
import psutil
import plotly.express as px
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timezone

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.database.schema.full import AutopoolStates, Blocks, RebalanceEvents, RebalancePlans
from mainnet_launch.database.postgres_operations import (
    merge_tables_as_df,
    get_highest_value_in_field_where,
    TableSelector,
)


from mainnet_launch.database.views import (
    fetch_autopool_destination_state_df,
    get_latest_rebalance_event_datetime_for_autopool,
)

# all price return values +- 10% replace with Nan, since they are likely bad data
# if price return is ever +-10% we'll see it in the nav per share charts

MAX_DISPLAY_PRICE_RETURN_PERCENT = 10


def fetch_nav_per_share_and_total_nav(autopool: AutopoolConstants) -> pd.DataFrame:
    nav_per_share_df = merge_tables_as_df(
        [
            TableSelector(
                table=AutopoolStates,
                select_fields=[
                    AutopoolStates.nav_per_share,
                    AutopoolStates.total_nav,
                    AutopoolStates.autopool_vault_address,
                ],
                join_on=None,
            ),
            TableSelector(
                table=Blocks,
                select_fields=Blocks.datetime,
                join_on=(AutopoolStates.chain_id == Blocks.chain_id) & (AutopoolStates.block == Blocks.block),
            ),
        ],
        where_clause=(Blocks.datetime > autopool.start_display_date)
        & (AutopoolStates.autopool_vault_address == autopool.autopool_eth_addr),
        order_by=Blocks.datetime,
    )

    nav_per_share_df = nav_per_share_df.set_index("datetime").resample("1d").last()
    nav_per_share_df.columns = [autopool.name, "NAV", "autopool_vault_address"]

    nav_per_share_df["30_day_difference"] = nav_per_share_df[autopool.name].diff(periods=30)
    nav_per_share_df["30_day_annualized_return"] = (
        (nav_per_share_df["30_day_difference"] / nav_per_share_df[autopool.name].shift(30)) * (365 / 30) * 100
    )
    nav_per_share_df["7_day_difference"] = nav_per_share_df[autopool.name].diff(periods=7)
    nav_per_share_df["7_day_annualized_return"] = (
        (nav_per_share_df["7_day_difference"] / nav_per_share_df[autopool.name].shift(7)) * (365 / 7) * 100
    )
    nav_per_share_df["daily_return"] = nav_per_share_df[autopool.name].pct_change(fill_method=None)
    nav_per_share_df["7_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=7).mean()
    nav_per_share_df["7_day_MA_annualized_return"] = nav_per_share_df["7_day_MA_return"] * 365 * 100
    nav_per_share_df["30_day_MA_return"] = nav_per_share_df["daily_return"].rolling(window=30).mean()
    nav_per_share_df["30_day_MA_annualized_return"] = nav_per_share_df["30_day_MA_return"] * 365 * 100
    return nav_per_share_df


def fetch_key_metrics_data(autopool: AutopoolConstants):
    nav_per_share_df = fetch_nav_per_share_and_total_nav(autopool) # some values for arbUSD
    destination_state_df = fetch_autopool_destination_state_df(autopool) #empty
    # not certain why this is empty for arbUSD, but it is
    safe_tvl_by_destination = (
        (
            destination_state_df.groupby(["datetime", "readable_name"])[["autopool_implied_safe_value"]]
            .sum()
            .reset_index()
            .pivot(values="autopool_implied_safe_value", index="datetime", columns="readable_name")
        )
        .fillna(0)
        .resample("1d")
        .last()
    )

    backing_tvl_by_destination = (
        (
            destination_state_df.groupby(["datetime", "readable_name"])[["autopool_implied_backing_value"]]
            .sum()
            .reset_index()
            .pivot(values="autopool_implied_backing_value", index="datetime", columns="readable_name")
        )
        .fillna(0)
        .resample("1d")
        .last()
    )

    total_safe_tvl = safe_tvl_by_destination.sum(axis=1)
    total_backing_tvl = backing_tvl_by_destination.sum(axis=1)

    price_return_series = 100 * (total_backing_tvl - total_safe_tvl) / total_backing_tvl

    price_return_series = price_return_series.where(
        (price_return_series <= MAX_DISPLAY_PRICE_RETURN_PERCENT)
        & (price_return_series >= -MAX_DISPLAY_PRICE_RETURN_PERCENT),
        np.nan,
    )
    portion_allocation_by_destination_df = safe_tvl_by_destination.div(total_safe_tvl, axis=0)

    max_apr_by_destination = (
        (
            destination_state_df.groupby(["datetime", "readable_name"])[["unweighted_expected_apr"]]
            .max()
            .reset_index()
            .pivot(values="unweighted_expected_apr", index="datetime", columns="readable_name")
        )
        .resample("1d")
        .last()
    )  # looks right

    expected_return_series = (max_apr_by_destination * portion_allocation_by_destination_df).sum(axis=1)
    # replace all expected returns of 0 with NaN, since an autopool can't every have a 0% expceted return
    # except with 100% idle only at the start
    expected_return_series = expected_return_series.replace(0, np.nan)

    total_nav_series = nav_per_share_df["NAV"]
    highest_block_and_datetime = (
        destination_state_df[["block", "datetime"]].sort_values("block", ascending=True).iloc[-1]
    )

    return (
        nav_per_share_df,
        total_nav_series,
        expected_return_series,
        portion_allocation_by_destination_df,
        highest_block_and_datetime,
        price_return_series,
    )


def _compute_percent_deployed(
    portion_allocation_by_destination_df: pd.DataFrame, autopool: AutopoolConstants
) -> tuple[float, float]:

    idle_yesterday = portion_allocation_by_destination_df[f"{autopool.name} (tokemak)"].iloc[-2]
    idle_today = portion_allocation_by_destination_df[f"{autopool.name} (tokemak)"].iloc[-1]
    return round(100 - (100 * idle_today), 2), round(100 - (100 * idle_yesterday), 2)


def _render_top_level_stats(
    nav_per_share_df,
    expected_return_series,
    portion_allocation_by_destination_df,
    autopool,
    latest_rebalance_event_datetime,
):
    def _fmt_dt(dt):
        return dt.strftime("%m-%d %H:%M") if dt else "—"

    def _simple_timedelta(td):
        secs = int(td.total_seconds())
        days, secs = divmod(secs, 86400)
        hours, secs = divmod(secs, 3600)
        minutes, secs = divmod(secs, 60)
        if days > 0:
            return f"{days}d {hours}h ago"
        elif hours > 0:
            return f"{hours}h ago"
        elif minutes > 0:
            return f"{minutes}m ago"
        else:
            return f"{secs}s ago"

    now = datetime.now(timezone.utc)

    # Row 1
    r1c1, r1c2, r1c3 = st.columns(3)
    r1c1.metric(
        "30-day Rolling APY (%)",
        round(nav_per_share_df["30_day_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["30_day_annualized_return"]),
    )
    r1c2.metric(
        "30-day MA APY (%)",
        round(nav_per_share_df["30_day_MA_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["30_day_MA_annualized_return"]),
    )
    r1c3.metric(
        "7-day Rolling APY (%)",
        round(nav_per_share_df["7_day_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["7_day_annualized_return"]),
    )

    # Row 2
    r2c1, r2c2, r2c3 = st.columns(3)
    r2c1.metric(
        "7-day MA APY (%)",
        round(nav_per_share_df["7_day_MA_annualized_return"].iloc[-1], 2),
        _diffReturn(nav_per_share_df["7_day_MA_annualized_return"]),
    )

    percent_deployed_today, percent_deployed_yesterday = _compute_percent_deployed(
        portion_allocation_by_destination_df, autopool
    )

    r2c2.metric(
        "Expected Annual Return (%)",
        round(expected_return_series.iloc[-1], 2),
        _diffReturn(expected_return_series),
    )
    r2c3.metric(
        "Percent Deployed",
        round(percent_deployed_today, 2),
        round(percent_deployed_today - percent_deployed_yesterday, 2),
    )

    # Row 3
    r3c1, r3c2, r3c3 = st.columns(3)
    # r3c1.metric(
    #     "Latest Rebalance Plan",
    #     _simple_timedelta(now - latest_rebalance_plan_datetime_generated),
    # )

    r3c1.metric(
        "Time Since Last Rebalance",
        _simple_timedelta(now - latest_rebalance_event_datetime),
    )

    r3c2.metric(" ", " ", " ")  # Spacer to complete the 3x3 grid
    r3c3.metric(" ", " ", " ")


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


def fetch_and_render_key_metrics_data(autopool: AutopoolConstants):
    (
        nav_per_share_df,
        total_nav_series,
        expected_return_series,
        portion_allocation_by_destination_df,
        highest_block_and_datetime,
        price_return_series,
    ) = fetch_key_metrics_data(autopool)

    latest_rebalance_event_datetime = get_latest_rebalance_event_datetime_for_autopool(autopool)
    # autopool_price_return = 100 * (autopool_backing_value - autopool_safe_value) / autopool_backing_value
    # weighted_price_return_series = _fetch_price_return(autopool)

    st.header(f"{autopool.name} Key Metrics")

    _render_top_level_stats(
        nav_per_share_df,
        expected_return_series,
        portion_allocation_by_destination_df,
        autopool,
        latest_rebalance_event_datetime,
    )
    _render_top_level_charts(nav_per_share_df, autopool, total_nav_series, expected_return_series, price_return_series)

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
        **Highest Block Used**: `{highest_block_and_datetime.iloc[0]}`  
        **Timestamp**: `{highest_block_and_datetime.iloc[1]}`  
        **Chain**: `{autopool.chain.name}`
        """
    )

    memory_used = psutil.Process().memory_info().rss / (1024**2)
    st.write(f"Memory Usage: {memory_used:.2f} MB")


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


if __name__ == "__main__":
    from mainnet_launch.constants import *

    fetch_and_render_key_metrics_data(ARB_USD)

    # from mainnet_launch.app.profiler import profile_function

    # # fetch_and_render_key_metrics_data(AUTO_ETH)

    # # profile_function(fetch_and_render_key_metrics_data, AUTO_USD)
    # pass
