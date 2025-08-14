import plotly.express as px
import streamlit as st
import plotly.io as pio
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime


from mainnet_launch.constants import *

from mainnet_launch.database.schema.full import Destinations, AutopoolDestinations, Tokens, SwapQuote, AssetExposure
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    get_highest_value_in_field_where,
    _exec_sql_and_cache,
)
import plotly.express as px
import plotly.io as pio


@st.cache_data(ttl=60 * 5, show_spinner=False)
def _load_full_swap_quote_df():
    """
    Load the full swap quote DataFrame from the database.
    """
    df = get_full_table_as_df(SwapQuote)
    df["effective_price"] = df["scaled_amount_out"] / df["scaled_amount_in"]
    tokens_df = get_full_table_as_df(Tokens)
    token_address_to_symbol = dict(zip(tokens_df["token_address"], tokens_df["symbol"]))
    df["buy_token_symbol"] = df["buy_token_address"].map(token_address_to_symbol)
    df["sell_token_symbol"] = df["sell_token_address"].map(token_address_to_symbol)

    return df


# streamlit pick a day


def pick_day():
    today = datetime.date.today()
    one_week_ago = today - datetime.timedelta(days=7)

    selected_date = st.date_input("Date", today, min_value=one_week_ago, max_value=today)
    return selected_date


def _render_intra_day_quote_spread(df: pd.DataFrame) -> None:
    sell_token_choices = sorted(df["sell_token_symbol"].dropna().unique().tolist())
    buy_token_choices = sorted(df["buy_token_symbol"].dropna().unique().tolist())
    api_name_choices = sorted(df["api_name"].dropna().unique().tolist())
    chain_id_choices = sorted(df["chain_id"].dropna().unique().tolist())

    smallest_num = 3

    def third_min(x: pd.Series):
        s = pd.Series(x).dropna().nsmallest(smallest_num)
        return s.iloc[smallest_num - 1] if len(s) >= smallest_num else None

    def third_max(x: pd.Series):
        s = pd.Series(x).dropna().nlargest(3)
        return s.iloc[smallest_num - 1] if len(s) >= smallest_num else None

    for base_asset_symbol in buy_token_choices:
        for chain_id in chain_id_choices:
            for api_name in api_name_choices:
                all_summaries = []

                for sell_token_symbol in sell_token_choices:
                    this_token_df = df[
                        (df["sell_token_symbol"] == sell_token_symbol)
                        & (df["api_name"] == api_name)
                        & (df["chain_id"] == chain_id)
                        & (df["buy_token_symbol"] == base_asset_symbol)
                    ].copy()
                    if this_token_df.empty:
                        continue

                    summary = (
                        this_token_df.groupby("scaled_amount_in", as_index=False)
                        .agg(
                            min_price=("effective_price", third_min),  # 3rd smallest
                            max_price=("effective_price", third_max),  # 3rd largest
                            batch_count=("quote_batch", "count"),
                        )
                        .sort_values("scaled_amount_in")
                        .reset_index(drop=True)
                    )

                    summary = summary.dropna(subset=["min_price", "max_price"]).copy()
                    if summary.empty:
                        continue

                    summary["bps_spread"] = (
                        10_000 * (summary["max_price"] - summary["min_price"]) / summary["min_price"]
                    )
                    summary["sell_token_symbol"] = sell_token_symbol
                    all_summaries.append(summary)

                if not all_summaries:
                    # st.warning(f"No data available for {api_name} on chain {chain_id} for {base_asset_symbol}.")
                    continue
                plot_df = pd.concat(all_summaries, ignore_index=True)

                smallest_amount_in = plot_df["scaled_amount_in"].min()
                # exclude the smallest amount_in to avoid skewing the plot
                plot_df = plot_df[plot_df["scaled_amount_in"] > smallest_amount_in].reset_index(drop=True)

                fig = px.line(
                    plot_df,
                    x="scaled_amount_in",
                    y="bps_spread",
                    color="sell_token_symbol",
                    markers=True,
                    title=f"BPS Spread (3rd-max vs 3rd-min) — {api_name} on chain {chain_id} for {base_asset_symbol}",
                    labels={
                        "scaled_amount_in": "Amount In (scaled)",
                        "bps_spread": "Spread (bps)",
                    },
                    hover_data={"batch_count": True, "bps_spread": ":.1f", "scaled_amount_in": ":.4g"},
                )

                fig.update_layout(legend_title_text="Symbol")
                st.plotly_chart(fig, use_container_width=True)


def fetch_and_render_intra_day_volitlity():
    """"""
    df = _load_full_swap_quote_df()
    selected_date = pick_day()
    df = df[df["datetime_received"].dt.date == selected_date].reset_index(drop=True)
    # _render_intra_day_quote_spread(df)
    _render_intra_day_effective_price(df)


def _render_intra_day_effective_price(df: pd.DataFrame) -> None:
    """
    For each (api_name, chain_id, buy_token_symbol), plot effective price per sell_token_symbol
    using error bars that span from the 3rd-smallest to the 3rd-largest effective_price
    at each scaled_amount_in.
    """
    sell_token_choices = sorted(df["sell_token_symbol"].dropna().unique().tolist())
    buy_token_choices = sorted(df["buy_token_symbol"].dropna().unique().tolist())
    api_name_choices = sorted(df["api_name"].dropna().unique().tolist())
    chain_id_choices = sorted(df["chain_id"].dropna().unique().tolist())

    smallest_num = 3

    def third_min(x: pd.Series):
        s = pd.Series(x).dropna().nsmallest(smallest_num)
        return s.iloc[smallest_num - 1] if len(s) >= smallest_num else None

    def third_max(x: pd.Series):
        s = pd.Series(x).dropna().nlargest(smallest_num)
        return s.iloc[smallest_num - 1] if len(s) >= smallest_num else None

    for base_asset_symbol in buy_token_choices:
        for chain_id in chain_id_choices:
            for api_name in api_name_choices:
                all_rows = []

                for sell_token_symbol in sell_token_choices:
                    this_token_df = df[
                        (df["sell_token_symbol"] == sell_token_symbol)
                        & (df["api_name"] == api_name)
                        & (df["chain_id"] == chain_id)
                        & (df["buy_token_symbol"] == base_asset_symbol)
                    ].copy()
                    if this_token_df.empty:
                        continue

                    # ensure numeric x
                    this_token_df["scaled_amount_in"] = pd.to_numeric(
                        this_token_df["scaled_amount_in"], errors="coerce"
                    )

                    summary = (
                        this_token_df.groupby("scaled_amount_in", as_index=False)
                        .agg(
                            min_price=("effective_price", third_min),  # 3rd smallest
                            max_price=("effective_price", third_max),  # 3rd largest
                            median_price=("effective_price", "median"),
                            batch_count=("quote_batch", "count"),
                        )
                        .sort_values("scaled_amount_in")
                        .reset_index(drop=True)
                    )

                    # must have both ends
                    summary = summary.dropna(subset=["min_price", "max_price"]).copy()
                    if summary.empty:
                        continue

                    summary["price_spread"] = summary["max_price"] - summary["min_price"]
                    summary["sell_token_symbol"] = sell_token_symbol

                    all_rows.append(
                        summary[
                            [
                                "scaled_amount_in",
                                "price_spread",
                                "median_price",
                                "min_price",
                                "max_price",
                                "batch_count",
                                "sell_token_symbol",
                            ]
                        ]
                    )

                if not all_rows:
                    continue

                plot_df = pd.concat(all_rows, ignore_index=True)

                # Optional: drop the absolute smallest x-bin to avoid skew
                if not plot_df.empty:
                    smallest_amount_in = plot_df["scaled_amount_in"].min()
                    plot_df = plot_df[plot_df["scaled_amount_in"] > smallest_amount_in].reset_index(drop=True)

                if plot_df.empty:
                    continue

                fig = px.line(
                    plot_df,
                    x="scaled_amount_in",
                    y="median_price",
                    color="sell_token_symbol",
                    error_y="price_spread",  # error bars cover 3rd-min ↔ 3rd-max
                    markers=True,
                    title=(
                        f"Effective Price with Error Bars (3rd-min ↔ 3rd-max) — "
                        f"{api_name} on chain {chain_id} for {base_asset_symbol}"
                    ),
                    labels={
                        "scaled_amount_in": "Amount In (scaled)",
                    },
                    hover_data={
                        "min_price": ":.6g",
                        "max_price": ":.6g",
                        "price_spread": ":.6g",
                        "batch_count": True,
                        "scaled_amount_in": ":.4g",
                    },
                )
                fig.update_layout(legend_title_text="Symbol")
                st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    st.title("Intra-Day Quote Spread Analysis")
    st.write("Compare the spead of quotes at differen prices for a given day")

    fetch_and_render_intra_day_volitlity()


# Goal:
# how frequently do we need to update the swap matrix?
# what is the shape of the swap matrix?

# buy_token, sell_token, datetime_received, sell_token_amount, datetime_received?
