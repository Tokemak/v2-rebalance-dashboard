import pandas as pd
import streamlit as st
import numpy as np
import plotly.express as px

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.pages.expected_vs_actual_returns.fetch_by_destination_realized_apr import (
    fetch_by_destination_actualized_and_projected_apr,
)


def _compute_n_day_realized_base_and_fee_apr(long_df: pd.DataFrame, n_day: int) -> pd.DataFrame:
    # the annualized change in virtual price == base apr and fee apr
    df = long_df.reset_index()
    current_virtual_price = df.pivot(columns="vault_name", values="virtual_price", index="timestamp").replace(0, np.nan)
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

    long_df["incentive_apr_out_weight"] = incentive_apr_weight * long_df["incentiveAprOut"]
    long_df["incentive_apr_in_weight"] = incentive_apr_weight * long_df["incentiveAprIn"]

    long_df["projected_base_and_fee"] = long_df["baseApr"] + long_df["feeApr"]

    long_df["projected_apr_out"] = long_df["baseApr"] + long_df["feeApr"] + long_df["incentive_apr_out_weight"]
    long_df["projected_apr_in"] = long_df["baseApr"] + long_df["feeApr"] + long_df["incentive_apr_in_weight"]

    return long_df.reset_index()


# plots


def _make_projected_vs_actual_scatter_plot(long_df: pd.DataFrame, x_col, y_col, title):
    fig = px.scatter(long_df, x=x_col, y=y_col, color="vault_name", title=title, hover_data="timestamp")
    max_value = long_df[[x_col, y_col]].max().max() * 1.2
    fig.add_shape(type="line", x0=0, y0=0, x1=max_value, y1=max_value, line=dict(color="black", dash="dash"))
    return fig


def _render_one_kind_of_error(
    long_df: pd.DataFrame,
    predicted_col: str,
    actual_col: str,
    error_col: str,
) -> None:
    # Prepare the DataFrame by sorting and reordering columns
    local_long_df = long_df.sort_values(by="vault_name").reindex(
        columns=["vault_name", predicted_col, actual_col, error_col, "timestamp"]
    )

    # Render error metrics tables first
    (
        negative_error_stats,
        positive_error_stats,
        all_error_stats,
        abs_error_stats,
    ) = _render_error_metrics(local_long_df, error_col)

    st.subheader("Error Metrics")
    st.subheader("All With Direction")
    st.write(all_error_stats.round(2))

    st.subheader("All ABS")
    st.write(abs_error_stats.round(2))

    # Render charts after the tables
    st.plotly_chart(
        _make_projected_vs_actual_scatter_plot(
            local_long_df,
            predicted_col,
            actual_col,
            f"{predicted_col} vs {actual_col}",
        ),
        use_container_width=True,
    )

    st.plotly_chart(
        px.ecdf(
            local_long_df,
            x=error_col,
            title=error_col,
            color="vault_name",
        ),
        use_container_width=True,
    )

    st.plotly_chart(
        px.histogram(
            local_long_df,
            x=error_col,
            title=error_col,
            color="vault_name",
        ),
        use_container_width=True,
    )

# why is decay state sometimes none?

def _render_one_kind_of_error2(
    long_df: pd.DataFrame,
    predicted_col: str,
    actual_col: str,
    error_col: str,
) -> None:
    local_long_df = long_df.sort_values(by="vault_name").reindex(
        columns=["vault_name", predicted_col, actual_col, error_col]
    )
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.plotly_chart(
            _make_projected_vs_actual_scatter_plot(
                local_long_df,
                predicted_col,
                actual_col,
                f"{predicted_col} vs {actual_col}",
            ),
            use_container_width=True,
        )
    with col2:
        st.plotly_chart(
            px.ecdf(
                local_long_df,
                x=error_col,
                title=error_col,
                color="vault_name",
            ),
            use_container_width=True,
        )

    with col3:
        st.plotly_chart(
            px.histogram(
                local_long_df,
                x=error_col,
                title=error_col,
                color="vault_name",
            ),
            use_container_width=True,
        )

    negative_error_stats, positive_error_stats, all_error_stats, abs_error_stats = _render_error_metrics(
        local_long_df, error_col
    )
    st.subheader("Error Metrics")
    for stats, label, col in zip(
        [negative_error_stats, positive_error_stats, all_error_stats, abs_error_stats],
        [
            "Overestimate (Realized < Projected)",
            "Underestimate (Realized > Projected)",
            "All (with sign)",
            "All Absolute Error",
        ],
        [col1, col2, col3, col4],
    ):
        with col:
            st.markdown(f"**{label}**")
            st.write(stats.round(2))


def _render_error_metrics(long_df: pd.DataFrame, error_col: str):
    # Create a copy and add a row for 'all_destinations'
    long_df_copy = long_df.copy()
    df_copy = long_df_copy.copy()
    df_copy["vault_name"] = "all_destinations"
    long_df_copy = pd.concat([long_df_copy, df_copy], ignore_index=True)

    # Define the metrics and corresponding names
    error_metrics = [
        "count",
        "mean",
        "median",
        "max",
        "min",
        lambda x: np.percentile(x, 10),
        lambda x: np.percentile(x, 90),
    ]
    error_metric_names = ["count", "mean", "median", "max", "min", "10th", "90th"]

    negative_error_stats = long_df_copy[long_df_copy[error_col] < 0].groupby("vault_name")[error_col].agg(error_metrics)
    negative_error_stats.columns = error_metric_names

    names = sorted(long_df["vault_name"].unique().tolist())
    index_order = [idx for idx in ["all_destinations", *names] if idx in negative_error_stats.index]

    negative_error_stats = negative_error_stats.loc[index_order]

    positive_error_stats = long_df_copy[long_df_copy[error_col] > 0].groupby("vault_name")[error_col].agg(error_metrics)

    positive_error_stats.columns = error_metric_names
    index_order = [idx for idx in ["all_destinations", *names] if idx in positive_error_stats.index]
    positive_error_stats = positive_error_stats.loc[index_order]

    all_error_stats = long_df_copy.groupby("vault_name")[error_col].agg(error_metrics)
    all_error_stats.columns = error_metric_names
    all_error_stats = all_error_stats.loc[index_order]

    long_df_copy[error_col] = long_df_copy[error_col].abs()
    abs_error_stats = long_df_copy.groupby("vault_name")[error_col].agg(error_metrics)
    abs_error_stats.columns = error_metric_names
    index_order = [idx for idx in ["all_destinations", *names] if idx in abs_error_stats.index]
    abs_error_stats = abs_error_stats.loc[index_order]

    return negative_error_stats, positive_error_stats, all_error_stats, abs_error_stats


def _render_plots(raw_long_df: pd.DataFrame, incentive_apr_weight: float):

    long_df = raw_long_df.dropna()

    long_df["Realized (Fee + Base APR) - Projected (Base + Fee APR)"] = (
        long_df["realized_base_and_fee_apr"] - long_df["projected_base_and_fee"]  # 1 -0 = positive over estiamtes,
    )

    long_df["Realized Incentive APR - Projected Incentive APR Out"] = long_df["realized_incentive_apr"] - (
        long_df["incentiveAprOut"] * incentive_apr_weight
    )

    long_df["Realized Incentive APR - Projected Incentive APR In"] = long_df["realized_incentive_apr"] - (
        long_df["incentiveAprIn"] * incentive_apr_weight
    )

    long_df["Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR Out)"] = (
        long_df["realized_base_plus_fee_plus_incentive_apr"] - long_df["projected_apr_out"]
    )

    long_df["Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)"] = (
        long_df["realized_base_plus_fee_plus_incentive_apr"] - long_df["projected_apr_in"]
    )

    error_metrics_options = [
        "Projected Fee + Base vs Actual Fee + Base",
        "Projected Incentive Out vs Acutal Incentive",
        "Projected Incentive In vs Acutal Incentive",
        "Projected Fee + Base + Incentive Out vs Actual Fee + Base + Incentive",
        "Projected Fee + Base + Incentive In vs Actual Fee + Base + Incentive",
    ]
    tab = st.selectbox("Error Metric", options=error_metrics_options, index=2)

    if tab == "Projected Fee + Base vs Actual Fee + Base":
        _render_one_kind_of_error(
            long_df,
            "projected_base_and_fee",
            "realized_base_and_fee_apr",
            "Realized (Fee + Base APR) - Projected (Base + Fee APR)",
        )
    elif tab == "Projected Incentive Out vs Acutal Incentive":
        _render_one_kind_of_error(
            long_df,
            "incentive_apr_out_weight",
            "realized_incentive_apr",
            "Realized Incentive APR - Projected Incentive APR Out",
        )

    elif tab == "Projected Incentive In vs Acutal Incentive":
        _render_one_kind_of_error(
            long_df,
            "incentive_apr_in_weight",
            "realized_incentive_apr",
            "Realized Incentive APR - Projected Incentive APR In",
        )

    elif tab == "Projected Fee + Base + Incentive Out vs Actual Fee + Base + Incentive":
        _render_one_kind_of_error(
            long_df,
            "projected_apr_out",
            "realized_base_plus_fee_plus_incentive_apr",
            "Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR Out)",
        )

    elif tab == "Projected Fee + Base + Incentive In vs Actual Fee + Base + Incentive":
        _render_one_kind_of_error(
            long_df,
            "projected_apr_out",
            "realized_base_plus_fee_plus_incentive_apr",
            "Realized (Fee + Base + Incentive APR) - Projected (Base + Fee + Incentive APR In)",
        )


def _render_one_destination_line_plots(long_df: pd.DataFrame):
    # Let the user pick one vault_name from a dropdown
    dest = st.selectbox("Choose a vault", sorted(long_df["vault_name"].unique()))

    pairs = [
        ["realized_base_and_fee_apr", "projected_base_and_fee"],
        ["realized_incentive_apr", "incentive_apr_in_weight", "incentive_apr_out_weight"],
        ["realized_base_plus_fee_plus_incentive_apr", "projected_apr_in", "projected_apr_out"],
    ]

    sub_df = long_df[long_df["vault_name"] == dest]

    for cols in pairs:
        st.plotly_chart(px.line(sub_df, x="timestamp", y=cols))


def fetch_and_render_by_destination_expected_apr(autopool: AutopoolConstants):
    long_df = fetch_by_destination_actualized_and_projected_apr(autopool)

    col1, col2, col3, col4 = st.columns(4)

    options_10to_60 = list(range(0, 61))
    with col1:
        # could be seperate but I think it makes more sense like this
        n_day_window = st.selectbox(
            "T(0) Projected vs T(0, N) actual window", options=options_10to_60, index=options_10to_60.index(7)
        )
    with col2:
        incentive_apr_forward_shift = st.selectbox(
            "Incentive APR n-day forward shift", options=options_10to_60, index=options_10to_60.index(1)
        )
    with col3:
        incentive_apr_weight_options = [round(i * 0.05, 2) for i in range(41)]
        incentive_apr_weight = st.selectbox(
            "Incentive APR weight",
            options=incentive_apr_weight_options,
            index=incentive_apr_weight_options.index(0.9),  # default value 0.9
        )
    with col4:
        decay_option = st.radio(
            "Select Decay State Filter", options=["Only decay state True", "Only decay state False", "Both"]
        )

    if decay_option == "Only decay state True":
        long_df = long_df[long_df["decay_state"] == True].copy()
    elif decay_option == "Only decay state False":
        long_df = long_df[long_df["decay_state"] == False].copy()
    elif decay_option == "Both":
        pass
    long_df = combine_raw_data_into_projected_and_realized_apr_df(
        long_df,
        base_and_fee_n_day_window=n_day_window,
        incentive_apr_n_day_window=n_day_window,
        incentive_apr_forward_shift=incentive_apr_forward_shift,
        incentive_apr_weight=incentive_apr_weight,
    )
    # only look at destinations where the tvl was > 0
    max_tvl_by_vault = long_df.groupby("vault_name")["TVL"].max()
    vaults_with_positive_tvl = max_tvl_by_vault[max_tvl_by_vault > 0]
    vaults_with_some_tvl = vaults_with_positive_tvl.index.tolist()
    long_df = long_df[long_df["vault_name"].isin(vaults_with_some_tvl)]

    _render_plots(long_df, incentive_apr_weight)  # TODO add size here, mean size, over period
    _render_one_destination_line_plots(long_df)


if __name__ == "__main__":

    from mainnet_launch.app.ui_config_setup import (
        config_plotly_and_streamlit,
        STREAMLIT_MARKDOWN_HTML,
        format_timedelta,
    )

    config_plotly_and_streamlit()
    from mainnet_launch.constants import AutopoolConstants, AUTO_ETH

    fetch_and_render_by_destination_expected_apr(AUTO_ETH)
    pass
