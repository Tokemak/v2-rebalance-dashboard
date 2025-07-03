"""
At the current block, 

Get the underlying assets owned by autopool, (eg autoUSD has 5M GHO, 200k crvUSD ... etc)

Get quotes for 1 of those tokens -> autopool.baseAsset

Get quotes for selling [10%, 20% ... 90%, 100%] of each asset for the base token

Currently does not store these values

"""

import asyncio


import streamlit as st
import plotly.express as px
import pandas as pd

from mainnet_launch.constants import AutopoolConstants
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes


def fetch_and_render_exit_liquidity_from_quotes(autopool: AutopoolConstants) -> pd.DataFrame:
    block = autopool.chain.client.eth.block_number
    reserve_token_ownership_df = fetch_raw_amounts_by_destination(block, autopool.chain)
    # need to exclude BPT tokens, not sure the right way to do that
    balances_by_tokens = (
        reserve_token_ownership_df[reserve_token_ownership_df["autopool_symbol"] == autopool.symbol]
        .groupby("token_address")["reserve_amount"]
        .sum()
    ).to_dict()

    quote_df, slippage_df = asyncio.run(fetch_quotes(autopool, balances_by_tokens))
    # quote _df maybe not needed

    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="percent_sold",
            y="bps_excess_loss_vs_1",
            color="symbol",
            title="Excess slippage bps by % of underlying assets sold",
        )
    )

    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="sell_amount_norm",
            y="bps_excess_loss_vs_1",
            color="symbol",
            title="Excess slippage bps by quantity of tokens sold",
        )
    )

    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="sell_amount_norm",
            y="buy_amount_norm",
            color="symbol",
            title="Sell Amount to buy amount",
        )
    )

    csv_quote = quote_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Quotes CSV",
        data=csv_quote,
        file_name=f"{autopool.name}_quotes.csv",
        mime="text/csv",
        help="Raw quote data for each token/percent-sold",
    )

    csv_slippage = slippage_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Slippage CSV",
        data=csv_slippage,
        file_name=f"{autopool.name}_slippage.csv",
        mime="text/csv",
        help="Computed slippage (bps_excess_loss_vs_1) for each token/percent-sold",
    )


if __name__ == "__main__":
    from mainnet_launch.constants import ALL_AUTOPOOLS, AUTO_ETH, AUTO_LRT

    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    st.title("Exit Liquidity Explorer")


    for autopool in ALL_AUTOPOOLS:
        st.header(autopool.name + " Exit Liqudity Quotes")
        fetch_and_render_exit_liquidity_from_quotes(autopool)
