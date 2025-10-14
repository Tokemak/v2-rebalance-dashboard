import pandas as pd
import plotly.express as px
import numpy as np
from mainnet_launch.constants import *
from mainnet_launch.database.views import get_token_details_dict

from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_many_requests_to_3rd_party,
    make_single_request_to_3rd_party,
    THIRD_PARTY_SUCCESS_KEY,
)

import streamlit as st


def add_actual_prices(df: pd.DataFrame) -> pd.DataFrame:
    token_to_decimals, token_to_symbol = get_token_details_dict()
    chain_ids_to_names = {c.chain_id: c.name for c in ALL_CHAINS}

    # normalize amounts safely (skip NaN)
    def norm(amount, token):
        if pd.isna(amount) or token not in token_to_decimals:
            return np.nan
        try:
            return int(amount) / (10 ** token_to_decimals[token])
        except Exception:
            return np.nan

    df["buy_amount_norm"] = df.apply(lambda r: norm(r["buyAmount"], r["buyToken"]), axis=1)
    df["sell_amount_norm"] = df.apply(lambda r: norm(r["sellAmount"], r["sellToken"]), axis=1)

    df["buy_symbol"] = df["buyToken"].map(token_to_symbol)
    df["sell_symbol"] = df["sellToken"].map(token_to_symbol)

    # ratio of normalized buy/sell
    df["buy_amount_price"] = df["buy_amount_norm"] / df["sell_amount_norm"]
    df["label"] = df["sell_symbol"] + " -> " + df["buy_symbol"]

    # handle missing price columns gracefully
    for col in ["buy_token_price", "sell_token_price"]:
        if col not in df.columns:
            df[col] = np.nan

    df["safe_value_bought"] = df["buy_token_price"] * df["buy_amount_norm"]
    df["safe_value_sold"] = df["sell_token_price"] * df["sell_amount_norm"]

    df["safe_value_slippage_bps"] = 1000 * (df["safe_value_sold"] - df["safe_value_bought"]) / df["safe_value_sold"]
    df["chain_name"] = df["chainId"].map(chain_ids_to_names)
    df["long_label"] = (
        df["label"] + " " + df["sell_amount_norm"].fillna(0).astype(int).astype(str) + " " + df["autopool_name"]
    )
    return df


@st.cache_data(show_spinner="Loading recent swap matrix data...")
def load_df():
    save_path = "/Users/pb/Documents/Github/Tokemak/v2-rebalance-dashboard/working_data/swap_matrix_prices2/all_autopools_full_swap_matrix_with_prices2.csv"
    df = add_actual_prices(pd.read_csv(save_path, low_memory=False))
    recent_df = df[df["datetime_received"] > "2025-10-13 19:20:09"]
    return recent_df


def render_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Filters")

    # --- helpers ---
    def uniq_vals(frame: pd.DataFrame, col: str):
        if col not in frame.columns:
            return []
        vals = frame[col].dropna().unique()
        try:
            vals = np.sort(vals)
        except Exception:
            vals = sorted(vals.astype(str))
        return list(vals)

    def coerce_valid_multi(selection, valid_opts):
        if not selection:
            return []
        valid = set(valid_opts)
        return [v for v in selection if v in valid]

    def coerce_valid_single(selection, valid_opts, default="All"):
        return selection if (selection in valid_opts or selection == "All") else default

    # Session state defaults
    ss = st.session_state
    ss.setdefault("autopools", [])
    ss.setdefault("sell_amt_choice", "All")
    ss.setdefault("buy_syms", [])
    ss.setdefault("sell_syms", [])

    # Columns in desired order
    c1, c2, c3, c4 = st.columns(4)

    # ---- Step 1: autopool_name (first filter) ----
    autopool_opts = uniq_vals(df, "autopool_name") if "autopool_name" in df.columns else []
    ss.autopools = coerce_valid_multi(ss.autopools, autopool_opts)
    with c1:
        autopools = st.multiselect(
            "autopool_name",
            options=autopool_opts,
            default=ss.autopools,
            key="autopools",
            disabled=len(autopool_opts) == 0,
        )

    mask = pd.Series(True, index=df.index)
    if autopools:
        mask &= df["autopool_name"].isin(autopools)
    df_lvl1 = df[mask]

    # ---- Step 2: sell_amount_norm (second) ----
    sell_amt_opts = ["All"] + uniq_vals(df_lvl1, "sell_amount_norm")
    ss.sell_amt_choice = coerce_valid_single(ss.sell_amt_choice, sell_amt_opts)
    with c2:
        sell_amt_choice = st.selectbox(
            "sell_amount_norm (exact match)",
            options=sell_amt_opts,
            index=sell_amt_opts.index(ss.sell_amt_choice),
            help="Select an exact normalized sell amount or choose All.",
            key="sell_amt_choice",
        )

    if sell_amt_choice != "All":
        mask &= df["sell_amount_norm"] == sell_amt_choice
    df_lvl2 = df[mask]

    # ---- Step 3: buy_symbol (third) ----
    buy_symbol_opts = uniq_vals(df_lvl2, "buy_symbol")
    ss.buy_syms = coerce_valid_multi(ss.buy_syms, buy_symbol_opts)
    with c3:
        buy_syms = st.multiselect(
            "buy_symbol",
            options=buy_symbol_opts,
            default=ss.buy_syms,
            key="buy_syms",
        )

    if buy_syms:
        mask &= df["buy_symbol"].isin(buy_syms)
    df_lvl3 = df[mask]

    # ---- Step 4: sell_symbol (fourth) ----
    sell_symbol_opts = uniq_vals(df_lvl3, "sell_symbol")
    ss.sell_syms = coerce_valid_multi(ss.sell_syms, sell_symbol_opts)
    with c4:
        sell_syms = st.multiselect(
            "sell_symbol",
            options=sell_symbol_opts,
            default=ss.sell_syms,
            key="sell_syms",
        )

    if sell_syms:
        mask &= df["sell_symbol"].isin(sell_syms)

    filtered = df[mask].copy()
    st.caption(f"Showing {len(filtered):,} of {len(df):,} rows.")
    return filtered



def _extract_chunk_slippage_values(_df: pd.DataFrame, minutes: str) -> pd.DataFrame:
    _df_copy = _df.copy()
    _df_copy[minutes] = pd.to_datetime(_df_copy["datetime_received"]).dt.floor(minutes)
    n_minute_slippage_values = (
        _df_copy.sort_values("datetime_received")  # make sure earliest is first
        .groupby([minutes, "long_label"], as_index=False)
        .first()  # keep the earliest row per group
        .pivot(index=minutes, columns="long_label", values="safe_value_slippage_bps")
        .sort_index()
    )
    return n_minute_slippage_values


def _build_diff_df(_df: pd.DataFrame, exclude_threshold: float, minutes: str, max_lag: int) -> pd.DataFrame:
    n_minutes_slippage_values = _extract_chunk_slippage_values(_df, minutes)
    non_outlier_hourly_slippage_values = n_minutes_slippage_values[n_minutes_slippage_values.abs() <= exclude_threshold]

    total_non_outlier_slippage_values = int((~non_outlier_hourly_slippage_values.isna()).sum().sum())
    diffs = []
    for i in range(1, max_lag + 1):
        # shift 1 says, get the value from 1 hour ahead
        i_hours_ahead = non_outlier_hourly_slippage_values.shift(i)
        diff_t = (non_outlier_hourly_slippage_values - i_hours_ahead).stack(dropna=True).rename("diff")
        count = diff_t.count()
        temp = pd.DataFrame({"diff": diff_t, "lag": f"t-{i} {count} samples"})
        diffs.append(temp)

    diffs.append(
        non_outlier_hourly_slippage_values.stack(dropna=True)
        .rename("diff")
        .to_frame()
        .assign(lag=f"Raw values {total_non_outlier_slippage_values} samples")
    )

    all_diffs = pd.concat(diffs, ignore_index=True)
    return all_diffs


def _make_ecdf_slippage_diff_plot(
    all_diffs: pd.DataFrame,
    plot_title: str,
    minutes: str,
    exclude_threshold: float,
):
    fig = px.ecdf(
        all_diffs,
        x="diff",
        color="lag",
        title=f"{plot_title} | Minutes: {minutes},  Exclude Threshold: {exclude_threshold} bps ",
        labels={"diff": "Slippage Difference (bps)", "lag": "Lag"},
    )
    fig.add_vline(x=1, line_dash="dash", line_color="red", annotation_text="+1 bps", annotation_position="top right")
    fig.add_vline(x=-1, line_dash="dash", line_color="red", annotation_text="-1 bps", annotation_position="top left")
    return fig


def main():

    st.title("Swap Matrix Analysis")
    df = load_df()
    filtered_df = render_filters(df)

    filter_outliers = st.checkbox("Filter out slippage outliers (>100 bps)", value=True)
    if filter_outliers:
        filtered_df = filtered_df[filtered_df["safe_value_slippage_bps"].abs() <= 100]

    st.write(f"Filtered to {len(filtered_df):,} / {len(df):,} total rows.")
    
    if st.button("Go"):
        with st.expander("See raw data table"):
            st.dataframe(filtered_df[['autopool_name', 'buy_symbol', 'sell_symbol', 'aggregatorName', 'excludeSources', 'includeSources',
        'datetime_received',  'buy_amount_norm', 'sell_amount_norm',  'safe_value_bought', 'safe_value_sold',
        'safe_value_slippage_bps',]], use_container_width=True)

        diff_df = _build_diff_df(
            filtered_df,
            exclude_threshold=100,
            minutes="60min",
            max_lag=3,
        )

        st.plotly_chart(_make_ecdf_slippage_diff_plot(
            diff_df,
            plot_title="ECDF of Slippage Differences",
            minutes="60min",
            exclude_threshold=100,
        ), use_container_width=True)

        histogram = px.histogram(
            diff_df,
            x="diff",
            color="lag",
            title=f"Histogram of Slippage Differences | Minutes: {'60min'},  Exclude Threshold: {100} bps ",
            labels={"diff": "Slippage Difference (bps)", "lag": "Lag"},
        )

        st.plotly_chart(histogram, use_container_width=True)
        st.plotly_chart(
            px.scatter(
                filtered_df,
                x="datetime_received",
                y="safe_value_slippage_bps",
                title="Slippage Over Time (long label)",
                color='long_label',
                labels={"safe_value_slippage_bps": "Slippage (bps)"},
            ),
            use_container_width=True,
        )

        st.plotly_chart(
            px.scatter(
                filtered_df,
                x="datetime_received",
                y="safe_value_slippage_bps",
                title="Slippage Over Time by aggregator",
                color='aggregatorName',
                labels={"safe_value_slippage_bps": "Slippage (bps)"},
            ),
            use_container_width=True,
        )


        with st.expander("Slippage by Buy token, sell token, sell amount"):
            st.dataframe(filtered_df.groupby('long_label')['safe_value_slippage_bps'].describe())
        
        with st.expander('Slippage by aggregator'):
            st.dataframe(filtered_df.groupby('aggregatorName')['safe_value_slippage_bps'].describe())


if __name__ == "__main__":
    main()
