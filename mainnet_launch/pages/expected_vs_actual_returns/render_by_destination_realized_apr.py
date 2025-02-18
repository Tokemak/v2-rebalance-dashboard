import pandas as pd
import streamlit as st
import numpy as np
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.data_fetching.add_info_to_dataframes import add_timestamp_to_df_with_block_column
from mainnet_launch.destinations import get_destination_details
from mainnet_launch.pages.expected_vs_actual_returns.fetch_by_destination_realized_apr import (
    _fetch_by_destination_actualized_apr_raw_data_from_external_source,
)


def _compute_n_day_realized_base_and_fee_apr(long_df: pd.DataFrame, n_day: int) -> pd.DataFrame:
    # the annualized change in virtual price == base apr and fee apr
    current_virtual_price = (
        long_df.reset_index().pivot(columns="vault_name", values="virtual_price", index="timestamp").replace(0, np.nan)
    )
    virtual_price_after_n_days = current_virtual_price.shift(-n_day)

    annualized_change_in_virtual_price = (365 / n_day) * (
        (virtual_price_after_n_days - current_virtual_price) / current_virtual_price
    )
    annualized_change_in_virtual_price = annualized_change_in_virtual_price.reset_index()
    annualized_change_in_virtual_price = annualized_change_in_virtual_price.melt(
        id_vars="timestamp", var_name="vault_name", value_name="realized_base_and_fee_apr"
    ).set_index(["timestamp", "vault_name"])

    return annualized_change_in_virtual_price


def _compute_realized_incentive_apr(long_df: pd.DataFrame, n_day_window: int, forward_shift: int) -> pd.DataFrame:
    """computes the realized Incentive APR by looking at the reward tokens sold, and the TVL in the destination"""
    next_n_days_rolling_avg_tvl = (
        long_df.reset_index()
        .pivot(columns="vault_name", values="TVL", index="timestamp")[::-1]
        .rolling(window=n_day_window, min_periods=n_day_window)
        .mean()[::-1]
        .shift(forward_shift)
    )
    next_n_days_rolling_total_incentives_sold = (
        long_df.reset_index()
        .pivot(columns="vault_name", values="incentives_sold", index="timestamp")[::-1]
        .rolling(window=n_day_window, min_periods=n_day_window)
        .sum()[::-1]
        .shift(forward_shift)
    )
    realized_incentive_apr = (
        (365 / n_day_window) * next_n_days_rolling_total_incentives_sold / next_n_days_rolling_avg_tvl
    )
    realized_incentive_apr = (
        realized_incentive_apr.reset_index()
        .melt(id_vars="timestamp", var_name="vault_name", value_name="realized_incentive_apr")
        .set_index(["timestamp", "vault_name"])
    )
    # infinite can happen where we earn some rewards, after we pulled out all the liquidity already
    realized_incentive_apr = realized_incentive_apr.replace(np.inf, np.nan)
    return realized_incentive_apr


def _compute_n_day_realized_base_and_fee_apr(long_df: pd.DataFrame, base_and_fee_n_day_window: int) -> pd.DataFrame:
    # the annualized change in virtual price == base apr and fee apr
    current_virtual_price = (
        long_df.reset_index().pivot(columns="vault_name", values="virtual_price", index="timestamp").replace(0, np.nan)
    )
    virtual_price_after_n_days = current_virtual_price.shift(-base_and_fee_n_day_window)

    annualized_change_in_virtual_price = (365 / base_and_fee_n_day_window) * (
        (virtual_price_after_n_days - current_virtual_price) / current_virtual_price
    )
    annualized_change_in_virtual_price = annualized_change_in_virtual_price.reset_index()
    annualized_change_in_virtual_price = annualized_change_in_virtual_price.melt(
        id_vars="timestamp", var_name="vault_name", value_name="realized_base_and_fee_apr"
    ).set_index(["timestamp", "vault_name"])

    return annualized_change_in_virtual_price


def combine_raw_data_into_projected_and_realized_apr_df(
    long_df: pd.DataFrame,
    base_and_fee_n_day_window: int,
    incentive_apr_n_day_window: int,
    incentive_apr_forward_shift: int,
    incentive_apr_weight: float = 0.9,
) -> pd.DataFrame:

    long_df["realized_base_and_fee_apr"] = 100 * _compute_n_day_realized_base_and_fee_apr(
        long_df, base_and_fee_n_day_window
    )
    long_df["realized_incentive_apr"] = 100 * _compute_realized_incentive_apr(
        long_df, incentive_apr_n_day_window, incentive_apr_forward_shift
    )
    long_df["realized_base_plus_fee_plus_incentive_apr"] = (
        long_df["realized_base_and_fee_apr"] + long_df["realized_incentive_apr"]
    )

    long_df["projected_base_and_fee"] = long_df["baseApr"] + long_df["feeApr"]
    long_df["projected_apr_out"] = (
        long_df["baseApr"] + long_df["feeApr"] + (incentive_apr_weight * long_df["incentiveAprOut"])
    )
    long_df["projected_apr_in"] = (
        long_df["baseApr"] + long_df["feeApr"] + (incentive_apr_weight * long_df["incentiveAprIn"])
    )

    return long_df.reset_index()


# plots


def _make_projected_vs_actual_scatter_plot(long_df: pd.DataFrame, x_col, y_col, title):
    fig = px.scatter(long_df, x=x_col, y=y_col, color="vault_name", title=title)

    max_value = long_df[[x_col, y_col]].max().max() * 1.2

    fig.add_shape(type="line", x0=0, y0=0, x1=max_value, y1=max_value, line=dict(color="black", dash="dash"))

    fig.update_xaxes(range=[0, max_value], scaleanchor="y", scaleratio=1)
    fig.update_yaxes(range=[0, max_value])

    return fig


def _render_plots(long_df: pd.DataFrame, incentive_apr_weight: float):
    long_df_copy = long_df.dropna()
    long_df_copy["Realized Incentive APR - Projected Incentive APR Out"] = long_df_copy["realized_incentive_apr"] - (
        long_df_copy["incentiveAprOut"] * incentive_apr_weight
    )
    long_df_copy["Realized Incentive APR - Projected Incentive APR In"] = long_df_copy["realized_incentive_apr"] - (
        long_df_copy["incentiveAprIn"] * incentive_apr_weight
    )
    long_df_copy["Realized (Fee + Base APR) - Projected (Base + Fee APR)"] = (
        long_df_copy["realized_base_and_fee_apr"] - long_df_copy["projected_base_and_fee"]
    )

    long_df_copy["Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)"] = (
        long_df_copy["realized_base_plus_fee_plus_incentive_apr"] - long_df_copy["projected_apr_in"]
    )
    col1, col2, col3 = st.columns(3)

    with col1:

        st.plotly_chart(
            _make_projected_vs_actual_scatter_plot(
                long_df_copy,
                "incentiveAprOut",
                "realized_incentive_apr",
                "Projected Incentive APR Out vs Realized Incentive APR",
            ),
            use_container_width=True,
        )
        st.plotly_chart(
            px.ecdf(
                long_df_copy,
                x="Realized Incentive APR - Projected Incentive APR Out",
                title="Realized Incentive APR - Projected Incentive APR Out",
                color="vault_name",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.histogram(
                long_df_copy,
                x="Realized Incentive APR - Projected Incentive APR Out",
                title="Realized Incentive APR - Projected Incentive APR Out",
                color="vault_name",
            ),
            use_container_width=True,
        )

    with col2:
        st.plotly_chart(
            _make_projected_vs_actual_scatter_plot(
                long_df_copy,
                "projected_base_and_fee",
                "realized_base_and_fee_apr",
                "Projected Base + Fee APR In vs Realized Base and Fee",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.ecdf(
                long_df_copy,
                x="Realized (Fee + Base APR) - Projected (Base + Fee APR)",
                title="Realized (Fee + Base APR) - Projected (Base + Fee APR)",
                color="vault_name",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.histogram(
                long_df_copy,
                x="Realized (Fee + Base APR) - Projected (Base + Fee APR)",
                title="Realized (Fee + Base APR) - Projected (Base + Fee APR)",
                color="vault_name",
            ),
            use_container_width=True,
        )

    with col3:
        st.plotly_chart(
            _make_projected_vs_actual_scatter_plot(
                long_df_copy,
                "projected_apr_in",
                "realized_base_plus_fee_plus_incentive_apr",
                "Projected Base + Fee + Incentive APR Out vs Realized Base + Fee + Incentive",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.ecdf(
                long_df_copy,
                x="Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)",
                title="Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)",
                color="vault_name",
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.histogram(
                long_df_copy,
                x="Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)",
                title="Realized (Fee + Base APR) - Projected (Base + Fee APR)",
                color="vault_name",
            ),
            use_container_width=True,
        )


def _display_error_stats(long_df: pd.DataFrame):

    long_df_copy = long_df.dropna().copy()
    long_df_copy["Realized (incentive + base + fee) - Projected (incentive in + base + fee)"] = (
        long_df_copy["realized_base_plus_fee_plus_incentive_apr"] - long_df_copy["projected_apr_in"]
    )
    df_copy = long_df_copy.copy()
    df_copy["vault_name"] = "all_destinations"
    long_df_copy = pd.concat([long_df_copy, df_copy], ignore_index=True)

    apr_diff_col = "Realized (incentive + base + fee) - Projected (incentive in + base + fee)"
    error_metrics = ["count", "mean", "median", "max", "min"]

    # --- By Destination Error Stats ---
    def _make_by_destination_error_stats(df: pd.DataFrame):
        negative_error_stats = df[df[apr_diff_col] < 0].groupby("vault_name")[apr_diff_col].agg(error_metrics)
        positive_error_stats = df[df[apr_diff_col] > 0].groupby("vault_name")[apr_diff_col].agg(error_metrics)
        all_error_stats = df.groupby("vault_name")[apr_diff_col].agg(error_metrics)
        return negative_error_stats, positive_error_stats, all_error_stats

    neg_stats_dest, pos_stats_dest, all_stats_dest = _make_by_destination_error_stats(long_df_copy)

    st.subheader("Error Metrics")
    for stats, label in zip(
        [neg_stats_dest, pos_stats_dest, all_stats_dest],
        ["Negative (Realized < Projected)", "Positive (Realized > Projected)", "All"],
    ):
        st.markdown(f"**{label}**")
        st.write(stats.round(2))


def fetch_and_render_by_destination_expected_apr(autopool: AutopoolConstants):
    long_df = _fetch_by_destination_actualized_apr_raw_data_from_external_source(
        autopool, autopool.chain.block_autopool_first_deployed
    )

    options_1_to_60 = list(range(1, 61))

    base_and_fee_n_day_window = st.selectbox(
        "Base and Fee n-day window", options=options_1_to_60, index=options_1_to_60.index(7)
    )

    incentive_apr_n_day_window = st.selectbox(
        "Incentive APR n-day window", options=options_1_to_60, index=options_1_to_60.index(7)
    )

    incentive_apr_forward_shift = st.selectbox(
        "Incentive APR n-day forward shift", options=options_1_to_60, index=options_1_to_60.index(1)
    )

    incentive_apr_weight_options = [round(i * 0.05, 2) for i in range(21)]

    incentive_apr_weight = st.selectbox(
        "Incentive APR weight",
        options=incentive_apr_weight_options,
        index=incentive_apr_weight_options.index(0.9),  # default value 0.9
    )

    long_df = combine_raw_data_into_projected_and_realized_apr_df(
        long_df,
        base_and_fee_n_day_window=base_and_fee_n_day_window,
        incentive_apr_n_day_window=incentive_apr_n_day_window,
        incentive_apr_forward_shift=incentive_apr_forward_shift,
        incentive_apr_weight=incentive_apr_weight,
    )
    # only look at destinations where the tvl was > 0
    max_tvl_by_vault = long_df.groupby("vault_name")["TVL"].max()
    vaults_with_positive_tvl = max_tvl_by_vault[max_tvl_by_vault > 0]
    vaults_with_some_tvl = vaults_with_positive_tvl.index.tolist()
    long_df = long_df[long_df["vault_name"].isin(vaults_with_some_tvl)]

    _render_plots(long_df, incentive_apr_weight)
    _display_error_stats(long_df)


if __name__ == "__main__":

    from mainnet_launch.app.ui_config_setup import (
        config_plotly_and_streamlit,
        STREAMLIT_MARKDOWN_HTML,
        format_timedelta,
    )

    config_plotly_and_streamlit()
    from mainnet_launch.constants import AutopoolConstants, AUTO_ETH

    fetch_and_render_by_destination_expected_apr(AUTO_ETH)
