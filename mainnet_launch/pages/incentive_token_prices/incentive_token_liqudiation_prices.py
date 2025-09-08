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


# ---------- Helpers ----------
def get_chain_base_options() -> list[Tuple[ChainData, TokemakAddress]]:
    """
    Return (chain, base_asset) tuples that are valid according to CHAIN_BASE_ASSET_GROUPS.
    """
    # keys are tuples (ChainData, base_asset_const)
    return list(CHAIN_BASE_ASSET_GROUPS.keys())


def format_chain_base_label(chain: ChainData, base: TokemakAddress) -> str:
    """
    Human-readable label for the (chain, base asset) combo.
    """
    # TokemakAddress has .name we can use
    base_name = getattr(base, "name", "BASE_ASSET")
    return f"{chain.name} • {base_name}"


def _infer_min_max_dates(df: pd.DataFrame) -> tuple[dt.date, dt.date]:
    """
    Safely infer min/max dates from df['datetime'] or fallback to 'block' if needed.
    """
    if "datetime" in df.columns:
        s = pd.to_datetime(df["datetime"], errors="coerce")
        s = s.dropna()
        if not s.empty:
            return s.min().date(), s.max().date()

    # Fallback: if no datetime, just pick a 90-day window ending today
    today = dt.date.today()
    return today - dt.timedelta(days=90), today


def sidebar_selectors(df: pd.DataFrame) -> tuple[Tuple[ChainData, TokemakAddress], dt.date, dt.date]:
    """
    Render sidebar selectors for (chain, base) and date range.
    """
    st.sidebar.header("Filters")

    options = get_chain_base_options()
    options_labels = [format_chain_base_label(chain, base) for (chain, base) in options]
    default_index = 0 if options else None
    chosen_label = st.sidebar.selectbox("Chain • Base Asset", options_labels, index=default_index)
    chosen_idx = options_labels.index(chosen_label)
    chain, base = options[chosen_idx]

    min_date, max_date = _infer_min_max_dates(df)
    start_date, end_date = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Streamlit returns either a single date or a tuple depending on widget usage
    if isinstance(start_date, tuple):
        start_date, end_date = start_date

    # Safety: enforce ordering
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    return (chain, base), start_date, end_date


def ensure_datetime_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee a 'datetime' column. If present but not datetime64, coerce.
    If absent, just return df (we'll rely on a default range).
    """
    if "datetime" in df.columns:
        if not pd.api.types.is_datetime64_any_dtype(df["datetime"]):
            df = df.copy()
            df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    return df


def _token_symbol_match(token_value, target_symbol: str) -> bool:
    """
    Best-effort match for token symbols in df rows.
    We assume df['sell'] and df['buy'] are human-readable symbols (e.g., 'USDC', 'WETH', 'AERO', ...).
    """
    if pd.isna(token_value):
        return False
    return str(token_value).upper() == str(target_symbol).upper()


def filter_sales_df(
    df: pd.DataFrame,
    chain: ChainData,
    base: TokemakAddress,
    start: dt.date,
    end: dt.date,
) -> pd.DataFrame:
    """
    Filter sales:
      - chain_id == chain.chain_id
      - buy token symbol == base.name
      - datetime in [start, end]
    Compute price_diff_pct and build 'label' == 'sell -> buy'.
    """
    df = ensure_datetime_column(df)

    # Filter chain
    out = df[df["chain_id"] == chain.chain_id].copy()
    if out.empty:
        return out

    # Filter by datetime if present
    if "datetime" in out.columns:
        mask = (out["datetime"] >= pd.Timestamp(start)) & (out["datetime"] <= pd.Timestamp(end) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))
        out = out[mask].copy()

    # Make the label; assume 'sell' and 'buy' are textual symbols
    if "sell" in out.columns and "buy" in out.columns:
        out["label"] = out.apply(lambda row: f"{row['sell']} -> {row['buy']}", axis=1)
    else:
        out["label"] = "Unknown pair"

    # Keep only sales where BUY token equals the selected base asset symbol
    base_symbol = getattr(base, "name", "USDC")
    if "buy" in out.columns:
        out = out[out["buy"].apply(_token_symbol_match, args=(base_symbol,))].copy()

    # Compute price diff pct
    if {"actual_execution", "third_party_price"}.issubset(out.columns):
        out["price_diff_pct"] = (
            (pd.to_numeric(out["actual_execution"]) - pd.to_numeric(out["third_party_price"]))
            / pd.to_numeric(out["third_party_price"])
            * 100.0
        )
    else:
        out["price_diff_pct"] = pd.NA

    # Drop nulls for the ECDF
    out = out.dropna(subset=["price_diff_pct"])
    return out


def _apply_default_style(fig: go.Figure) -> None:
    fig.update_traces(line=dict(width=3))
    fig.update_layout(
        title_x=0.5,
        margin=dict(l=40, r=40, t=40, b=80),
        height=600,
        width=1200,
        font=dict(size=16),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="lightgray", title="Price Diff vs 3rd Party (%)"),
        yaxis=dict(showgrid=True, gridcolor="lightgray", title="ECDF"),
        colorway=px.colors.qualitative.Set2,
    )


def ecdf_figure(filtered: pd.DataFrame, title: str) -> go.Figure:
    fig = px.ecdf(
        filtered,
        x="price_diff_pct",
        color="label",
        title=title,
        markers=True,
    )
    _apply_default_style(fig)
    fig.add_vline(x=0, line_width=1, line_dash="dash", line_color="red")
    return fig


def summarize(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame(columns=["label", "count", "p05", "p25", "p50", "p75", "p95", "mean"])
    g = filtered.groupby("label")["price_diff_pct"]
    out = pd.DataFrame({
        "count": g.size(),
        "p05": g.quantile(0.05),
        "p25": g.quantile(0.25),
        "p50": g.quantile(0.50),
        "p75": g.quantile(0.75),
        "p95": g.quantile(0.95),
        "mean": g.mean(),
    }).reset_index()
    return out.sort_values(["label"]).reset_index(drop=True)


# ---------- Page ----------
@st.cache_data(show_spinner=False, ttl=60 * 10)
def _load_sales_df() -> pd.DataFrame:
    """
    Cached fetch; the underlying view should already join the needed pieces.
    """
    df = get_incentive_token_sold_details()
    return df


def render_page():
    st.title("Incentive Token Sales — Execution vs Third-Party Price (ECDF)")

    df = _load_sales_df()

    # UI
    (chain, base), start_date, end_date = sidebar_selectors(df)
    base_symbol = getattr(base, "name", "BASE")

    st.caption(
        f"Showing sales on **{chain.name}** into **{base_symbol}** between **{start_date}** and **{end_date}**."
    )

    # Filtering
    filtered = filter_sales_df(df, chain, base, start_date, end_date)

    if filtered.empty:
        st.info("No sales found for the chosen filters.")
        return

    # Figure
    fig_title = f"ECDF of Incentive Token Sales vs 3rd-Party Price — {chain.name} • {base_symbol}"
    fig = ecdf_figure(filtered, fig_title)
    st.plotly_chart(fig, use_container_width=True)

    # Summary
    with st.expander("Summary stats (by pair)"):
        stats = summarize(filtered)
        st.dataframe(stats, use_container_width=True)

    # Optional: show raw data
    with st.expander("Raw filtered rows"):
        show_cols = [c for c in ["datetime", "sell", "buy", "actual_execution", "third_party_price", "price_diff_pct", "label", "chain_id"] if c in filtered.columns]
        st.dataframe(filtered.sort_values("datetime", ascending=False)[show_cols], use_container_width=True)


if __name__ == "__main__":
    render_page()
