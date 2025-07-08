import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd


from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes


# UI CHANGES
# user can put in threshold -> answer all the token that are a problem
# include a doc explain the assumptions)
# dynamic coloring, as well
# if slippage > X -> then make the cell yellow
# maybe also show it as a table as well
# don't over crowd it


@time_decorator
def fetch_and_render_exit_liquidity_from_quotes(valid_autopools: tuple[AutopoolConstants]) -> None:
    a_valid_autopool = valid_autopools[0]

    block = a_valid_autopool.chain.client.eth.block_number
    reserve_df = fetch_raw_amounts_by_destination(block, a_valid_autopool.chain)
    valid_autopool_symbols = [pool.symbol for pool in valid_autopools]
    reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)

    balances = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()
    quote_df, slippage_df = asyncio.run(
        fetch_quotes(
            a_valid_autopool.chain, a_valid_autopool.base_asset, a_valid_autopool.base_asset_decimals, balances
        )
    )

    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="percent_sold",
            y="bps_loss_excess_vs_reference_price",
            color="symbol",
            hover_data={"Sold Quantity": ":.2f"},
            title="Excess slippage bps by % sold",
        )
    )


if __name__ == "__main__":

    from mainnet_launch.constants import *

    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    st.title("Exit Liquidity Explorer")

    chain_base_asset_groups = {
        (ETH_CHAIN, "WETH"): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, "USDC"): (AUTO_USD,),
        (ETH_CHAIN, "DOLA"): (AUTO_DOLA,),
        (SONIC_CHAIN, "USD"): (SONIC_USD,),
        (BASE_CHAIN, "WETH"): (BASE_ETH,),
        (BASE_CHAIN, "USD"): (BASE_USD,),
    }

    # fetch_and_render_exit_liquidity_from_quotes(chain_base_asset_groups[(ETH_CHAIN, "WETH")])

    options = list(chain_base_asset_groups.keys())
    selected_key = st.selectbox("Pick a chain & token:", options, format_func=lambda k: f"{k[0].name} chain → {k[1]}")
    if st.button("Fetch exit-liquidity"):
        fetch_and_render_exit_liquidity_from_quotes(chain_base_asset_groups[selected_key])

# streamlit run mainnet_launch/pages/exit_liquidity/estimate_exit_liquidity_from_quotes.py
