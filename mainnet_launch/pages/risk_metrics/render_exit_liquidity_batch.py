import plotly.express as px
import streamlit as st
import plotly.io as pio
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


from mainnet_launch.constants import *

from mainnet_launch.database.schema.full import Tokens, SwapQuote, AssetExposure
from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df,
    _exec_sql_and_cache,
)

from mainnet_launch.pages.risk_metrics.drop_down import render_pick_chain_and_base_asset_dropdown


WETH_SLIPPAGE_WARNING_THESHOLD_BPS = 50
STABLE_COINS_SLIPPAGE_WARNING_THESHOLD_BPS = 25


def _display_readme() -> None:
    with st.expander("Readme", expanded=False):
        st.markdown(
            """
            # Exit Liquidity Quotes Slippage

            This is a tool for telling the impact of size on the price of a token using odos, and our internal API

            ## Terms:

            - Reference Price: The price of a token, as defined by the median price of 10_000 stable coins, or 5 LSTs, using the Tokemak Swaps API.  
            - Effective Price: buyAmount / sellAmount
            - Slippage Bps: The difference between the effective price and the reference price, expressed in basis points (bps). 

            round(10000 * (reference_price - effective_price) / reference_price, 2)

            Quotes are made within a batch, typically ~20 minutes within a single batch.

            Assumptions:

            We need to make many quotes, ~1k from Tokemak and Odos. The quotes are within the same ~25 minute window.

            - A quote at minute 0, is comparable to the identical quote up to 25 minutes later

            """
        )


@st.cache_data(ttl=60 * 60)
def _load_full_quote_batch_df(quote_batch_number: int) -> pd.DataFrame:
    full_df = get_full_table_as_df(
        SwapQuote,
        where_clause=(SwapQuote.quote_batch == quote_batch_number),
    )
    tokens_df = get_full_table_as_df(Tokens)
    token_address_to_symbol = dict(zip(tokens_df["token_address"], tokens_df["symbol"]))
    full_df["buy_token_symbol"] = full_df["buy_token_address"].map(token_address_to_symbol)
    full_df["sell_token_symbol"] = full_df["sell_token_address"].map(token_address_to_symbol)

    # use .loc to add a series that add 10_000, for each row where sell_token is USDC or DOLA
    full_df.loc[full_df["buy_token_symbol"].isin(["USDC", "DOLA"]), "reference_quantity"] = (
        STABLE_COINS_REFERENCE_QUANTITY
    )
    full_df.loc[full_df["buy_token_symbol"] == "WETH", "reference_quantity"] = ETH_REFERENCE_QUANTITY
    # how much we got / how much we sold
    full_df["effective_price"] = full_df["scaled_amount_out"] / full_df["scaled_amount_in"]

    full_df["chain_sell_buy_token_symbols"] = (
        full_df["chain_id"].astype(str) + " " + full_df["sell_token_symbol"] + " " + full_df["buy_token_symbol"]
    )

    reference_price_df = full_df[
        (full_df["scaled_amount_in"].isin([ETH_REFERENCE_QUANTITY, STABLE_COINS_REFERENCE_QUANTITY]))
        & (full_df["api_name"] == "tokemak")
    ]

    median_reference_prices = (
        reference_price_df.groupby("chain_sell_buy_token_symbols")["effective_price"].median().to_dict()
    )

    full_df["reference_price"] = full_df["chain_sell_buy_token_symbols"].map(median_reference_prices)

    full_df["label"] = full_df["sell_token_symbol"] + " - " + full_df["api_name"]
    full_df["slippage_bps"] = (
        10_000 * ((full_df["reference_price"] - full_df["effective_price"]) / full_df["reference_price"])
    ).round(2)

    return full_df, median_reference_prices


@st.cache_data(ttl=60 * 60)
def _fetch_asset_allocation_from_db(quote_batch_number: int) -> pd.DataFrame:
    asset_exposure_df = get_full_table_as_df(
        AssetExposure,
        where_clause=(AssetExposure.quote_batch == quote_batch_number),
    )
    tokens_df = get_full_table_as_df(Tokens)
    token_address_to_symbol = dict(zip(tokens_df["token_address"], tokens_df["symbol"]))
    asset_exposure_df["token_symbol"] = asset_exposure_df["token_address"].map(token_address_to_symbol)
    asset_exposure_df["reference_symbol"] = asset_exposure_df["reference_asset"].map(token_address_to_symbol)
    return asset_exposure_df


@st.cache_data(ttl=60 * 10)
def _load_quote_batch_options_from_db() -> list[dict]:
    df = _exec_sql_and_cache(
        """
        SELECT 
            quote_batch, 
            MIN(datetime_received) AS first_datetime_received,
            MIN(percent_exclude_threshold) AS percent_exclude_threshold
        FROM swap_quotes
        GROUP BY quote_batch
        ORDER BY first_datetime_received DESC;
        """
    )

    options = df.to_dict("records")
    return options


def _pick_a_quote_batch() -> int:
    options = _load_quote_batch_options_from_db()
    selected = st.selectbox(
        "Pick a quote batch",
        options,
        format_func=lambda r: f"Batch {r['quote_batch']} — {r['first_datetime_received']:%Y-%m-%d %H:%M:%S} Percent Exclude Threshold {r['percent_exclude_threshold']}%",
    )
    return selected["quote_batch"]


@st.cache_data(ttl=60 * 10)
def _load_quote_data(chain: ChainData, base_asset: TokemakAddress, quote_batch_number: int) -> pd.DataFrame:
    full_quote_batch_df, median_reference_prices = _load_full_quote_batch_df(quote_batch_number)
    asset_exposure_df = _fetch_asset_allocation_from_db(quote_batch_number)
    asset_exposure_df = asset_exposure_df[
        (asset_exposure_df["chain_id"] == chain.chain_id) & (asset_exposure_df["reference_asset"] == base_asset(chain))
    ].reset_index(drop=True)
    # just this set of chain base asset pairs suspect
    swap_quotes_df = full_quote_batch_df[
        (full_quote_batch_df["chain_id"] == chain.chain_id) & (full_quote_batch_df["base_asset"] == base_asset(chain))
    ].reset_index(drop=True)

    suspect_exit_liqudity_assets_df = identify_suspect_exit_liquidity_quotes(full_quote_batch_df)

    return asset_exposure_df, swap_quotes_df, suspect_exit_liqudity_assets_df


def identify_suspect_exit_liquidity_quotes(swap_quotes_df: pd.DataFrame) -> pd.DataFrame:
    median_df = (
        swap_quotes_df.groupby(["api_name", "buy_token_symbol", "sell_token_symbol", "scaled_amount_in", "chain_id"])[
            "slippage_bps"
        ]
        .median()
        .reset_index()
    )

    stable_coin_df = median_df[median_df["buy_token_symbol"].isin(["USDC", "DOLA"])].copy()
    weth_df = median_df[median_df["buy_token_symbol"] == "WETH"].copy()

    suspect_weth_rows = weth_df[weth_df["slippage_bps"] > WETH_SLIPPAGE_WARNING_THESHOLD_BPS].copy()
    suspect_stablecoin_rows = stable_coin_df[
        stable_coin_df["slippage_bps"] > STABLE_COINS_SLIPPAGE_WARNING_THESHOLD_BPS
    ].copy()
    suspect_df = pd.concat([suspect_weth_rows, suspect_stablecoin_rows], ignore_index=True)

    chain_id_to_name = {c.chain_id: c.name for c in ALL_CHAINS}
    suspect_df["chain"] = suspect_df["chain_id"].map(chain_id_to_name)
    limited_suspect_df = (
        suspect_df[["chain", "sell_token_symbol", "buy_token_symbol"]].drop_duplicates().reset_index(drop=True)
    )
    return limited_suspect_df


def _fetch_and_render_exit_liquidity_from_quotes(
    chain: ChainData,
    base_asset: TokemakAddress,
    valid_autopools: list[AutopoolConstants],
    quote_batch_number: int | None = None,
) -> None:
    if quote_batch_number is None:
        quote_batch_number = _pick_a_quote_batch()

    st.subheader(
        f"Suspect Exit Liquidity Pairs, Stable Coin Slippage > {STABLE_COINS_SLIPPAGE_WARNING_THESHOLD_BPS} bps or WETH Slippage > {WETH_SLIPPAGE_WARNING_THESHOLD_BPS} bps"
    )

    asset_exposure_df, swap_quotes_df, suspect_exit_liqudity_assets_df = _load_quote_data(
        chain, base_asset, quote_batch_number
    )
    st.dataframe(suspect_exit_liqudity_assets_df, use_container_width=True, hide_index=True)

    if swap_quotes_df.empty:
        st.warning(
            f"No exit liquidity quotes found for the {chain.name} chain and {base_asset(chain)} base asset in batch {quote_batch_number}."
        )
        return

    should_trim_slippage_graph = st.checkbox(
        "Trim Slippage Graphs to -50 bps to 100 bps",
        value=True,
    )

    render_slippage_scatter_plots(swap_quotes_df, should_trim_slippage_graph, base_asset)

    token_median_reference_prices = swap_quotes_df.groupby("sell_token_symbol")["reference_price"].first().to_dict()
    _render_asset_exposure_df(asset_exposure_df, token_median_reference_prices, chain, base_asset)
    display_swap_quotes_batch_meta_data(swap_quotes_df)
    _display_readme()

    st.download_button(
        label="Download Exit Liquidity Quotes Full Data",
        data=swap_quotes_df.to_csv(index=False).encode("utf-8"),
        file_name=f"exit_liquidity_quotes_{quote_batch_number}.csv",
        mime="text/csv",
    )


def fetch_and_render_exit_liquidity_from_quotes() -> None:
    st.subheader("Exit Liquidity Quotes")

    quote_batch_number = _pick_a_quote_batch()
    chain, base_asset, _ = render_pick_chain_and_base_asset_dropdown()

    _fetch_and_render_exit_liquidity_from_quotes(
        chain, base_asset, valid_autopools=[], quote_batch_number=quote_batch_number
    )


def _render_asset_exposure_df(
    asset_exposure_df: pd.DataFrame, token_median_reference_prices: dict, chain: ChainData, base_asset: TokemakAddress
) -> pd.DataFrame:
    token_median_reference_prices["WETH"] = 1
    token_median_reference_prices["USDC"] = 1
    token_median_reference_prices["DOLA"] = 1
    this_asset_allocation_df = asset_exposure_df[
        (asset_exposure_df["reference_asset"] == base_asset(chain)) & (asset_exposure_df["chain_id"] == chain.chain_id)
    ][["token_symbol", "quantity"]].reset_index()

    this_asset_allocation_df["reference_price"] = this_asset_allocation_df["token_symbol"].map(
        token_median_reference_prices
    )

    this_asset_allocation_df["value_in_base_asset"] = (
        this_asset_allocation_df["quantity"] * this_asset_allocation_df["reference_price"]
    )
    this_asset_allocation_df = this_asset_allocation_df.sort_values("value_in_base_asset", ascending=False)
    st.subheader("Asset Allocation Summary")
    st.dataframe(
        this_asset_allocation_df[["token_symbol", "quantity", "reference_price", "value_in_base_asset"]],
        use_container_width=True,
    )


def render_slippage_scatter_plots(
    swap_quotes_df: pd.DataFrame, should_trim_slippage_graph: bool, base_asset: TokemakAddress
) -> None:

    if base_asset.name == "WETH":
        dashed_line_slippage_threshold = WETH_SLIPPAGE_WARNING_THESHOLD_BPS
    elif base_asset.name in ["USDC", "DOLA"]:
        dashed_line_slippage_threshold = STABLE_COINS_SLIPPAGE_WARNING_THESHOLD_BPS

    median_df = swap_quotes_df.groupby(["label", "scaled_amount_in"])["slippage_bps"].median().reset_index()

    sell_token_symbols = sorted(swap_quotes_df["sell_token_symbol"].dropna().unique().tolist())
    palette = px.colors.qualitative.Plotly
    color_map = {}
    for api_name in ["odos", "tokemak"]:
        for i, lbl in enumerate(sell_token_symbols):
            color_map[f"{lbl} - {api_name}"] = palette[i % len(palette)]

    fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("ODOS: Slippage vs Amount Sold", "Tokemak: Slippage vs Amount Sold"),
        shared_yaxes=True,
    )

    for i, api_name in enumerate(["odos", "tokemak"]):
        local_df = median_df[median_df["label"].str.contains(api_name)].copy()
        local_df.rename(columns={"scaled_amount_in": "Quantity Sold", "slippage_bps": "Slippage bps"}, inplace=True)

        tmp = px.line(
            local_df,
            x="Quantity Sold",
            y="Slippage bps",
            color="label",
            markers=True,
            color_discrete_map=color_map,
        )
        tmp.update_traces(
            mode="lines+markers",
            line=dict(dash="dash"),
            marker_symbol="x",
            marker_size=6,
            showlegend=True,
        )

        for tr in tmp.data:
            fig.add_trace(tr, row=1, col=i + 1)

    fig.update_xaxes(title_text="Quantity Sold", row=1, col=1)
    fig.update_xaxes(title_text="Quantity Sold", row=1, col=2)
    fig.update_yaxes(title_text="Slippage bps", row=1, col=1)

    if should_trim_slippage_graph:
        fig.update_yaxes(range=[-50, 100], row=1, col=1)
        fig.update_yaxes(range=[-50, 100], row=1, col=2)

    fig.update_layout(title="Slippage vs Amount Sold — ODOS vs Tokemak", legend_title_text="label", height=600)
    fig.add_hline(
        y=dashed_line_slippage_threshold,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Warning Threshold: {dashed_line_slippage_threshold} bps",
        annotation_position="top right",
        annotation_font=dict(color="red"),
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
