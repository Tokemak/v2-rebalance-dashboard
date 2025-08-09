import plotly.express as px
import streamlit as st
import plotly.io as pio
import pandas as pd


from mainnet_launch.constants import *

from mainnet_launch.database.schema.full import Destinations, AutopoolDestinations, Tokens, SwapQuote
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    get_highest_value_in_field_where,
)

from mainnet_launch.pages.risk_metrics.drop_down import render_pick_chain_and_base_asset_dropdown


def fetch_and_render_exit_liquidity_from_quotes() -> pd.DataFrame:

    st.subheader("Exit Liquidity Quotes")

    chain, base_asset, _ = render_pick_chain_and_base_asset_dropdown()
    swap_quotes_df = _load_latest_exit_liquidity_quotes(chain, base_asset)

    display_swap_quotes_batch_meta_data(swap_quotes_df)
    display_slippage_scatter(swap_quotes_df)


# inclined to put this in a separate file
# might want to cache for 5 mintues?
@st.cache_data(ttl=60 * 5)
def _load_latest_exit_liquidity_quotes(chain: ChainData, base_asset: TokemakAddress) -> pd.DataFrame:
    """
    Load the latest exit liquidity quotes from the database.
    """

    latest_quote_batch = get_highest_value_in_field_where(
        SwapQuote,
        SwapQuote.quote_batch,
        where_clause=(SwapQuote.base_asset == base_asset(chain)) & (SwapQuote.chain_id == chain.chain_id),
    )

    swap_quotes_df = get_full_table_as_df(SwapQuote, where_clause=SwapQuote.quote_batch == latest_quote_batch)
    swap_quotes_df = _augment_swap_quotes_df(swap_quotes_df)

    with st.expander("Exit Liquidity Quotes Full Data", expanded=False):
        st.dataframe(swap_quotes_df, use_container_width=True)

    return swap_quotes_df


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


def _add_token_symbols_columns(swap_quotes_df: pd.DataFrame) -> None:
    tokens_df = get_full_table_as_df(Tokens, where_clause=Tokens.chain_id == swap_quotes_df["chain_id"].iloc[0])
    token_address_to_symbol = dict(zip(tokens_df["token_address"], tokens_df["symbol"]))
    swap_quotes_df["buy_token_symbol"] = swap_quotes_df["buy_token_address"].map(token_address_to_symbol)
    swap_quotes_df["sell_token_symbol"] = swap_quotes_df["sell_token_address"].map(token_address_to_symbol)


def _augment_swap_quotes_df(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    # how much of the base asset we we get from selling X amoutn of the sell token
    # eg 101 sDAI / 100 USDC  -> effective price of 1.01 sDAI per USDC
    swap_quotes_df["effective_price"] = swap_quotes_df["scaled_amount_out"] / swap_quotes_df["scaled_amount_in"]
    _add_token_symbols_columns(swap_quotes_df)
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


def display_slippage_scatter(swap_quotes_df: pd.DataFrame):
    """
    Render a scatter plot of scaled_amount_in vs slippage_bps,
    filtered by sell_token_symbol and size_factor.
    """
    # Select sell token symbol
    symbol_options = swap_quotes_df["sell_token_symbol"].unique().tolist()
    selected_symbol = st.selectbox("Select sell token symbol", symbol_options)

    # Select size factor (absolute / portion)
    factor_options = swap_quotes_df["size_factor"].unique().tolist()
    selected_factor = st.selectbox("Select size factor", factor_options)

    # Filter the DataFrame
    filtered_df = (
        swap_quotes_df[
            (swap_quotes_df["sell_token_symbol"] == selected_symbol)
            & (swap_quotes_df["size_factor"] == selected_factor)
        ]
        .groupby(["label", "scaled_amount_in"])["slippage_bps"]
        .median()
        .reset_index()
    )

    # Plot
    fig = px.scatter(
        filtered_df,
        x="scaled_amount_in",
        y="slippage_bps",
        color="label",
        title=f"Slippage vs Amount for {selected_symbol} ({selected_factor})",
    )
    st.plotly_chart(fig, use_container_width=True)

    return filtered_df


def display_swap_quotes_batch_meta_data(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Display the swap quotes DataFrame with:
      • a one-line summary above (count, batch, duration, first quote time)
      • four “mini‐metrics” using caption + write instead of st.metric
      • the interactive table below
    """
    # compute metrics
    quote_count = len(swap_quotes_df)
    batch_number = swap_quotes_df["quote_batch"].iat[0] if quote_count else None
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


# 1440 (on 2 min sleeping)
