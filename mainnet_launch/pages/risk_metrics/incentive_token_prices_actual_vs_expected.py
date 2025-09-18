import datetime as dt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import streamlit as st

from mainnet_launch.constants import (
    CHAIN_BASE_ASSET_GROUPS,
    ChainData,
    TokemakAddress,
    AutopoolConstants,
    LIQUIDATION_ROW2,
    LIQUIDATION_ROW,
)
from mainnet_launch.database.views import get_incentive_token_sold_details


INCENTIVE_TOKEN_N_DAYS_OPTIONS = [7, 14, 30, 60, 90, 180, 365]


def pick_chain_base_and_n_days() -> tuple[ChainData, TokemakAddress, dt.date, dt.date]:
    st.subheader("Pick and Chain Base Asset")
    options = list(CHAIN_BASE_ASSET_GROUPS.keys())
    label_map = {f"{c.name} {b.name}": (c, b) for (c, b) in options}
    chosen_label = st.selectbox("Chain • Base Asset", list(label_map.keys()))
    chain, base = label_map[chosen_label]
    n_days = st.selectbox("Last N Days", INCENTIVE_TOKEN_N_DAYS_OPTIONS, index=2)

    return chain, base, n_days


@st.cache_data(show_spinner=False, ttl=60 * 10)
def _load_sales_df() -> pd.DataFrame:
    df = get_incentive_token_sold_details()
    df["label"] = df["sell"] + " -> " + df["buy"]
    df["price_diff_pct"] = ((df["actual_execution"] - df["third_party_price"]) / df["third_party_price"]) * 100.0
    return df


def summarize(filtered_df: pd.DataFrame) -> pd.DataFrame:
    percentiles = filtered_df.groupby("label")["price_diff_pct"]
    buy_quantities = filtered_df.groupby("label")["buy_amount"].sum()
    out = pd.DataFrame(
        {
            "Count": percentiles.size(),
            "Total Base Asset Bought": buy_quantities,
            "5th %": percentiles.quantile(0.05),
            "50th %": percentiles.quantile(0.50),
            "95th %": percentiles.quantile(0.95),
            "mean": percentiles.mean(),
        }
    ).reset_index()
    return out.sort_values(["Total Base Asset Bought"], ascending=False).reset_index(drop=True).round(2)


def _create_ecdf_figure(filtered: pd.DataFrame, title: str) -> go.Figure:
    fig = px.ecdf(
        filtered,
        x="price_diff_pct",
        color="label",
        title=title,
        markers=True,
    )
    return fig


def _render_incentive_token_sales_scatter_plot(filtered_df: pd.DataFrame) -> None:
    fig = px.scatter(filtered_df, x="datetime", y="buy_amount", color="label", hover_data="price_diff_pct")
    fig.update_layout(title="Incentive Token Sales Over", xaxis_title="Date", yaxis_title="Buy Amount")
    st.plotly_chart(fig, use_container_width=True)


def _render_ecdfs(
    filtered_df: pd.DataFrame,
    before_filtered_df: pd.DataFrame,
    chain: ChainData,
    base: TokemakAddress,
    n_days_ago: dt.date,
) -> None:
    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=[
            f"Days {n_days_ago} to today — {chain.name} • {base.name}",
            f"Days {n_days_ago * 2} to {n_days_ago} days ago — {chain.name} • {base.name}",
        ],
    )

    fig1 = _create_ecdf_figure(filtered_df, title=None)
    for trace in fig1.data:
        fig.add_trace(trace, row=1, col=1)

    fig2 = _create_ecdf_figure(before_filtered_df, title=None)
    for trace in fig2.data:
        fig.add_trace(trace, row=1, col=2)

    fig.add_vline(x=0.0, line_width=1, line_dash="dash", line_color="red", row=1, col=1)
    fig.add_vline(x=0.0, line_width=1, line_dash="dash", line_color="red", row=1, col=2)

    fig.update_layout(
        title_text=f"ECDF of Incentive Token Sales vs 3rd-Party Price — {chain.name} • {base.name}", showlegend=True
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_readme():
    with st.expander("Readme"):
        st.write("Compare actual sale price vs the off-chain estimate.")

        st.markdown(
            "- **Positive values**: we received more than expected.\n"
            "- **Negative values**: we received less than expected."
        )

        st.write("Data sources:")
        st.markdown(
            "- Swapped events from each `LiquidationRow` → saved to `IncentiveTokenSwapped`.\n"
            "- For each event `block.timestamp`, fetch chain price from internal API → saved to `IncentiveTokenPrices`."
        )

        st.write("Internal price API: generic-swaps-prices-infra-staging/price  (SystemName: `gen3`)")

        st.write("Math:")
        st.write("actual_execution =")
        st.latex(r"\frac{\text{buy\_amount\_received}}{\text{sell\_amount}}")

        st.write("third_party_price = price from the off-chain API at that block timestamp")

        st.write("price_diff_pct =")
        st.latex(r"\frac{\text{actual\_execution} - \text{third\_party\_price}}{\text{third\_party\_price}} \times 100")

        st.markdown(f"Liquidation Row Addresses:\n\n {LIQUIDATION_ROW}\n\n{LIQUIDATION_ROW2}")


def _extract_filtered_dfs(chain: ChainData, base_asset: TokemakAddress, df: pd.DataFrame, n_days: int):
    today = dt.datetime.now(dt.timezone.utc).date()
    n_days_ago = today - dt.timedelta(days=n_days)
    n_days_ago_prior = today - dt.timedelta(days=n_days * 2)

    filtered_df = df[
        (df["chain_id"] == chain.chain_id)
        & (df["buy"] == base_asset.name)
        & (df["datetime"].dt.date >= n_days_ago)
        & (df["datetime"].dt.date <= today)
    ].copy()

    before_filtered_df = df[
        (df["chain_id"] == chain.chain_id)
        & (df["buy"] == base_asset.name)
        & (df["datetime"].dt.date >= n_days_ago_prior)
        & (df["datetime"].dt.date <= n_days_ago)
    ].copy()

    return filtered_df, before_filtered_df


def _render_tables(filtered_df: pd.DataFrame):

    with st.expander("Summary stats (by pair)"):
        stats_df = summarize(filtered_df)
        st.dataframe(stats_df, use_container_width=True)

    with st.expander("Raw filtered rows"):
        st.dataframe(filtered_df.sort_values("datetime", ascending=False), use_container_width=True)


def render_actual_vs_expected_incentive_token_prices():
    st.title("Incentive Token Sales: Actual Price vs Offchain Price")

    df = _load_sales_df()

    chain, base_asset, n_days = pick_chain_base_and_n_days()

    filtered_df, before_filtered_df = _extract_filtered_dfs(chain, base_asset, df, n_days)

    st.caption(
        f"Showing Incentive token sales on **{chain.name}** into **{base_asset.name}** between **{filtered_df.index.min()}** and **{filtered_df.index.max()}**."
    )

    if filtered_df.empty:
        st.info("No incentive tokens found for the chosen filters.")
        return

    _render_incentive_token_sales_scatter_plot(filtered_df)
    _render_ecdfs(filtered_df, before_filtered_df, chain, base_asset, n_days)
    _render_readme()
    _render_tables(filtered_df)


def testing_mock_incentive_token_sales_page(
    chain: ChainData, base_asset: TokemakAddress, valid_autopools: list[AutopoolConstants]
):
    # only for testing, not used in prod

    df = _load_sales_df()
    for n_days in INCENTIVE_TOKEN_N_DAYS_OPTIONS:
        filtered_df, before_filtered_df = _extract_filtered_dfs(chain, base_asset, df, n_days)

        if filtered_df.empty:
            st.info("No sales found for the chosen filters.")
            return

        _render_incentive_token_sales_scatter_plot(filtered_df)
        _render_ecdfs(filtered_df, before_filtered_df, chain, base_asset, n_days)
        _render_readme()
        _render_tables(filtered_df)


if __name__ == "__main__":
    render_actual_vs_expected_incentive_token_prices()
