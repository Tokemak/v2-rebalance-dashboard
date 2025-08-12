import plotly.express as px
import streamlit as st
import plotly.io as pio
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


from mainnet_launch.constants import *

from mainnet_launch.database.schema.full import Destinations, AutopoolDestinations, Tokens, SwapQuote
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    get_highest_value_in_field_where,
    _exec_sql_and_cache,
)

from mainnet_launch.pages.risk_metrics.drop_down import render_pick_chain_and_base_asset_dropdown


# two charts
# one for odos quotes, one for tokemak quotes

# same X axix

# ignore the fees from odos




def _display_readme() -> None:
    with st.expander("Readme", expanded=False):
        st.markdown(
            """
            Odos quotes should always be worse than Tokemak quotes, because they exclude pools we have a large ownership in.
            excluding options can only make thing worse. 

            open questions::
            - buy amount and min buy amount?
            - what do we use?
            """
        )


@st.cache_data(ttl=60 * 10)
def _load_quote_batch_options_from_db() -> list[dict]:
    df = _exec_sql_and_cache(
        """
        SELECT 
            quote_batch, 
            ARRAY_AGG(DISTINCT size_factor) AS unique_size_factors,
            MIN(datetime_received) AS first_datetime_received,
            MIN(percent_exclude_threshold) AS percent_exclude_threshold
        FROM swap_quote
        GROUP BY quote_batch
        HAVING 
            bool_and(size_factor IN ('portion', 'absolute'))
            AND COUNT(DISTINCT size_factor) = 2
        ORDER BY first_datetime_received DESC;
        """
    )

    df = df.drop(columns=["unique_size_factors"])
    options = df.to_dict("records")

    # options = [o for o in options if o["quote_batch"] >= 18]
    return options


def _pick_a_quote_batch() -> int:
    options = _load_quote_batch_options_from_db()

    selected = st.selectbox(
        "Pick a quote batch",
        options,
        format_func=lambda r: f"Batch {r['quote_batch']} — {r['first_datetime_received']:%Y-%m-%d %H:%M:%S} Percent Exclude Threshold {r['percent_exclude_threshold']}%",
    )

    return selected["quote_batch"]


def fetch_and_render_exit_liquidity_from_quotes() -> pd.DataFrame:
    st.subheader("Exit Liquidity Quotes")

    quote_batch_number = _pick_a_quote_batch()
    chain, base_asset, _ = render_pick_chain_and_base_asset_dropdown()
    full_quote_batch_df = _load_full_quote_batch_df(quote_batch_number)

    # st.dataframe(full_quote_batch_df.groupby(["chain_id", "base_asset"]).size().reset_index(name="count"))

    swap_quotes_df = full_quote_batch_df[
        (full_quote_batch_df["chain_id"] == chain.chain_id) & (full_quote_batch_df["base_asset"] == base_asset(chain))
    ].reset_index(drop=True)

    _augment_swap_quotes_df(swap_quotes_df)

    if swap_quotes_df.empty:
        st.warning("No exit liquidity quotes found for the selected chain and base asset.")
        return pd.DataFrame()

    _display_asset_allocation(swap_quotes_df)
    display_swap_quotes_batch_meta_data(swap_quotes_df)
    display_scatter_plot(swap_quotes_df, "effective_price")
    display_scatter_plot(swap_quotes_df, "slippage_bps")

    st.download_button(
        label="Download Exit Liquidity Quotes Full Data",
        data=full_quote_batch_df.to_csv(index=False).encode("utf-8"),
        file_name=f"exit_liquidity_quotes_{quote_batch_number}.csv",
        mime="text/csv",
    )


# cache the data for speed reasons
@st.cache_data(ttl=60 * 10)
def _load_full_quote_batch_df(quote_batch_number: int) -> pd.DataFrame:
    """
    Load the full quote batch DataFrame for the given quote batch number, chain, and base asset.
    """
    full_df = get_full_table_as_df(
        SwapQuote,
        where_clause=(SwapQuote.quote_batch == quote_batch_number),
    )
    tokens_df = get_full_table_as_df(Tokens)
    token_address_to_symbol = dict(zip(tokens_df["token_address"], tokens_df["symbol"]))
    full_df["buy_token_symbol"] = full_df["buy_token_address"].map(token_address_to_symbol)
    full_df["sell_token_symbol"] = full_df["sell_token_address"].map(token_address_to_symbol)
    return full_df


def _add_reference_price_column(swap_quotes_df: pd.DataFrame) -> None:
    # TODO include this in the readme
    # For each token, it is the min effective price for that swap size
    # the refernce price is the price of selling a non-trivial amount for the base asset
    # for stable coins it is 10_000 tokens, for ETH based assets it is 5 ETH
    # we get the median price for these swap sizes from the tokemak api at this batch

    # note you need to refrech the quotes if you want to exclude different pools
    # right now excluding all pools we have 10% ownership in

    smallest_amount_in = swap_quotes_df[swap_quotes_df["size_factor"] == "absolute"]["scaled_amount_in"].min()
    sell_token_to_reference_price = (
        swap_quotes_df[
            (swap_quotes_df["size_factor"] == "absolute")
            & (swap_quotes_df["scaled_amount_in"] == smallest_amount_in)
            & (swap_quotes_df["api_name"] == "tokemak")
        ]
        .groupby("sell_token_symbol")["effective_price"]
        .median()
        .to_dict()
    )
    swap_quotes_df["reference_price"] = swap_quotes_df["sell_token_symbol"].map(sell_token_to_reference_price)
    return sell_token_to_reference_price


def _augment_swap_quotes_df(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    # how much of the base asset we we get from selling X amoutn of the sell token
    # eg 101 sDAI / 100 USDC  -> effective price of 1.01 sDAI per USDC
    swap_quotes_df["effective_price"] = swap_quotes_df["scaled_amount_out"] / swap_quotes_df["scaled_amount_in"]
    _add_reference_price_column(swap_quotes_df)
    swap_quotes_df["label"] = swap_quotes_df["sell_token_symbol"] + " - " + swap_quotes_df["api_name"]

    # - Sell a larger quantity (e.g., 100 stETH → receive 97.5 ETH)
    # - New price = 97.5 ETH ÷ 100 stETH = 0.975 ETH/stETH

    # - Excess slippage in basis points (bps):

    # `slippage_bps = 10 000 * (0.98 - 0.975) ÷ 0.98 ≈ 51 bps`

    swap_quotes_df["slippage_bps"] = (
        10_000
        * (swap_quotes_df["effective_price"] - swap_quotes_df["reference_price"])
        / swap_quotes_df["reference_price"]
    ).round(2)

    return swap_quotes_df


def _display_asset_allocation(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    asset_allocation_series = (
        swap_quotes_df[swap_quotes_df["size_factor"] == "portion"]
        .groupby("sell_token_symbol")["scaled_amount_in"]
        .max()
    )

    st.write("Allocation")

    smallest_amount_in = swap_quotes_df[swap_quotes_df["size_factor"] == "absolute"]["scaled_amount_in"].min()

    sell_token_to_reference_price = (
        swap_quotes_df[
            (swap_quotes_df["size_factor"] == "absolute")
            & (swap_quotes_df["scaled_amount_in"] == smallest_amount_in)
            & (swap_quotes_df["api_name"] == "tokemak")
        ]
        .groupby("sell_token_symbol")["effective_price"]
        .median()
    )
    summary_df = pd.concat(
        [
            asset_allocation_series,
            sell_token_to_reference_price,
            (asset_allocation_series * sell_token_to_reference_price).rename("value_in_base_asset"),
        ],
        axis=1,
    )
    summary_df.columns = ["quantity", "reference_price", "value_in_base_asset"]
    summary_df = summary_df.sort_values("value_in_base_asset", ascending=False)
    st.dataframe(summary_df)


def display_scatter_plot(swap_quotes_df: pd.DataFrame, target_column: str) -> dict[str, pd.DataFrame]:

    st.write(f"Scatter for {target_column}")
    symbol_options = sorted(swap_quotes_df["sell_token_symbol"].dropna().unique().tolist())
    factors = ["absolute", "portion"]
    factor_symbol = {"absolute": "circle", "portion": "x"}

    # Stable colors per label across all charts
    all_labels = sorted(swap_quotes_df["label"].dropna().unique().tolist())
    palette = px.colors.qualitative.Plotly
    label_color = {lbl: palette[i % len(palette)] for i, lbl in enumerate(all_labels)}

    for selected_symbol in symbol_options:
        fig = go.Figure()

        for size_factor in factors:
            df_factor = (
                swap_quotes_df[
                    (swap_quotes_df["sell_token_symbol"] == selected_symbol)
                    & (swap_quotes_df["size_factor"] == size_factor)
                ]
                .groupby(["label", "scaled_amount_in"], as_index=False)[target_column]
                .median()
            )
            if df_factor.empty:
                continue

            # One trace per (label, factor) to overlay both factors
            for label, df_lab in df_factor.groupby("label", sort=True):
                fig.add_trace(
                    go.Scatter(
                        x=df_lab["scaled_amount_in"],
                        y=df_lab[target_column],
                        mode="markers",
                        name=f"{label} ({size_factor})",
                        legendgroup=label,  # group legend entries by label
                        marker=dict(
                            symbol=factor_symbol.get(size_factor, "circle"),
                            size=7,
                            color=label_color.get(label),
                            line=dict(width=0.5),
                        ),
                       hovertemplate = (
                        f"label=%{{text}}<br>"
                        f"factor={size_factor}<br>"
                        f"scaled_amount_in=%{{x}}<br>"
                        f"{target_column}=%{{y}}<extra></extra>"
                    ),
                        text=[label] * len(df_lab),
                    )
                )

        fig.update_xaxes(title_text="scaled_amount_in")
        fig.update_yaxes(title_text=target_column)
        fig.update_layout(
            title_text=f"{target_column} vs Amount — {selected_symbol} (absolute & portion overlaid)",
            margin=dict(t=60, r=20, b=20, l=50),
            legend_title_text="label (factor)",
        )
        st.plotly_chart(fig, use_container_width=True)


def display_swap_quotes_batch_meta_data(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    # compute metrics
    quote_count = len(swap_quotes_df)
    batch_number = swap_quotes_df["quote_batch"].unique()[0]
    start_time = swap_quotes_df["datetime_received"].min()
    end_time = swap_quotes_df["datetime_received"].max()
    window = end_time - start_time
    total_seconds = int(window.total_seconds())
    hours, rem = divmod(total_seconds, 3600)
    minutes, sec = divmod(rem, 60)
    formatted_window = f"{hours}h {minutes}m {sec}s" if hours else f"{minutes}m {sec}s"

    # lay out 4 columns
    c1, c2, c3, c4 = st.columns(4)

    # for each col: small, grey label via caption + plain value via write
    for col, label, value in zip(
        (c1, c2, c3, c4),
        ("Quotes count", "Quote Batch #", "Duration", "First Quote Received"),
        (quote_count, batch_number, formatted_window, start_time.strftime("%Y-%m-%d %H:%M:%S")),
    ):
        col.caption(label)
        col.write(value)


if __name__ == "__main__":
    fetch_and_render_exit_liquidity_from_quotes()


# open questions:
# why
