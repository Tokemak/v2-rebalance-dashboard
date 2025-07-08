import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd

from pathlib import Path
from datetime import datetime

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes


# UI CHANGES
# user can put in threshold -> answer all the token that are a problem
# include a doc explain the assumptions
# dynamic coloring, as well
# if slippage > X -> then make the cell yellow
# maybe also show it as a table as well
# don't over crowd it


def fetch_and_render_exit_liquidity_from_quotes(autopool: AutopoolConstants, output_dir: Path, timestamp: str) -> None:
    block = autopool.chain.client.eth.block_number
    reserve_df = fetch_raw_amounts_by_destination(block, autopool.chain)
    balances = (
        reserve_df[reserve_df["autopool_symbol"] == autopool.symbol].groupby("token_address")["reserve_amount"].sum()
    ).to_dict()

    quote_df, slippage_df = asyncio.run(fetch_quotes(autopool, balances))

    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="percent_sold",
            y="bps_loss_excess_vs_reference_price",
            color="symbol",
            hover_data={"sell_amount_norm": ":.2f"},
            title="Excess slippage bps by % sold",
        )
    )


if __name__ == "__main__":

    from mainnet_launch.constants import *

    # Streamlit setup
    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    st.title("Exit Liquidity Explorer")

    quotes_dir = Path("mainnet_launch/data_fetching/quotes/local_quote_data/")
    now = int(datetime.now().timestamp())

    # fetch_and_render_exit_liquidity_from_quotes(AUTO_LRT, quotes_dir, now)

    groups = []

    for base_asset in [WETH, DOLA, USDC]:
        for CHAIN
    
        st.header(pool.name)
        fetch_and_render_exit_liquidity_from_quotes(pool, quotes_dir, now)

# streamlit run mainnet_launch/pages/exit_liquidity/estimate_exit_liquidity_from_quotes.py
