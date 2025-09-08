import datetime as dt
from typing import Tuple
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from mainnet_launch.constants import (
    CHAIN_BASE_ASSET_GROUPS,
    ChainData,
    TokemakAddress,
)
from mainnet_launch.database.schema.views import get_incentive_token_sold_details


def pick_dates_and_chain_and_asset() -> tuple[ChainData, TokemakAddress, dt.date, dt.date]:
    """
    Render sidebar selectors for (chain, base) and date range.
    """
    st.subheader("Pick s and Chain Base Asset")
    options: list[Tuple[ChainData, TokemakAddress]] = list(CHAIN_BASE_ASSET_GROUPS.keys())
    options_labels = [f"{chain.name} {base.name}" for (chain, base) in options]
    chosen_label = st.selectbox("Chain • Base Asset", options_labels)
    chain, base = options[options_labels.index(chosen_label)]

    n_days = st.selectbox("Last N Days", [7, 14, 30, 60, 90], index=2)

    today = dt.datetime.now(dt.timezone.utc).date()
    n_days_ago = today - dt.timedelta(days=n_days)
    return chain, base, n_days_ago, today


def ecdf_figure(filtered: pd.DataFrame, title: str) -> go.Figure:
    fig = px.ecdf(
        filtered,
        x="price_diff_pct",
        color="label",
        title=title,
        markers=True,
    )
    fig.add_vline(x=0, line_width=1, line_dash="dash", line_color="red")
    return fig


def summarize(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame(columns=["label", "count", "p05", "p25", "p50", "p75", "p95", "mean"])
    percentiles = filtered.groupby("label")["price_diff_pct"]
    buy_quantities = filtered.groupby("label")["buy_amount"].sum()
    out = pd.DataFrame(
        {
            "Count": percentiles.size(),
            "Total Base Asset Bought": buy_quantities,
            "5th percentile": percentiles.quantile(0.05),
            "25th percentile": percentiles.quantile(0.25),
            "50th percentile": percentiles.quantile(0.50),
            "75th percentile": percentiles.quantile(0.75),
            "95th percentile": percentiles.quantile(0.95),
            "mean": percentiles.mean(),
        }
    ).reset_index()

    return out.sort_values(["label"]).reset_index(drop=True)


# ---------- Page ----------
@st.cache_data(show_spinner=False, ttl=60 * 10)
def _load_sales_df() -> pd.DataFrame:
    """
    Cached fetch; the underlying view should already join the needed pieces.
    """
    df = get_incentive_token_sold_details()
    df["label"] = df.apply(lambda row: f"{row['sell']} -> {row['buy']}", axis=1)
    df["price_diff_pct"] = (df["actual_execution"] - df["third_party_price"]) / df["third_party_price"] * 100.0
    return df


def render_page():
    st.title("Incentive Token Sales — Execution vs Third-Party Price (ECDF)")
    # todo add markdown explanation

    df = _load_sales_df()

    chain, base, n_days_ago, today = pick_dates_and_chain_and_asset()

    st.caption(f"Showing sales on **{chain.name}** into **{base.name}** between **{n_days_ago}** and **{today}**.")

    filtered_df = df[
        (df["chain_id"] == chain.chain_id)
        & (df["buy"] == base.name)
        & (df["datetime"].dt.date >= n_days_ago)
        & (df["datetime"].dt.date <= today)
    ].copy()

    if filtered_df.empty:
        st.info("No sales found for the chosen filters.")
        return

    fig = ecdf_figure(
        filtered_df, title=f"ECDF of Incentive Token Sales vs 3rd-Party Price — {chain.name} • {base.name}"
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Summary stats (by pair)"):
        stats = summarize(filtered_df)
        st.dataframe(stats, use_container_width=True)

    # Optional: show raw data
    with st.expander("Raw filtered rows"):
        st.dataframe(filtered_df.sort_values("datetime", ascending=False), use_container_width=True)


if __name__ == "__main__":
    render_page()
