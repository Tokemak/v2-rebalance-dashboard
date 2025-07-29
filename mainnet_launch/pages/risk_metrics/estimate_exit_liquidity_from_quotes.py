import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd


from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes


# this can be added in the email version
# UI CHANGES
# user can put in threshold -> answer all the token that are a problem
# include a doc explain the assumptions)
# dynamic coloring, as well
# if slippage > X -> then make the cell yellow
# maybe also show it as a table as well
# don't over crowd it


@st.cache_data(ttl=5 * 60)  # cache for 5 minutes
def _fetch_quote_and_slippage_data(valid_autopools: tuple[AutopoolConstants]):
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
    return quote_df, slippage_df


def _render_slippage_plots(slippage_df: pd.DataFrame) -> None:
    slippage_df_not_reference_price = slippage_df[
        slippage_df["reference_quantity"] != slippage_df["Sold Quantity"].astype(int)
    ]

    pivot_df = (
        slippage_df_not_reference_price.pivot(
            index="percent_sold", columns="symbol", values="bps_loss_excess_vs_reference_price"
        )
        .sort_index()
        .dropna(how="any")
    )

    st.subheader("Excess Slippage (bps) by Percent Sold")
    st.dataframe(pivot_df, use_container_width=True)

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


def fetch_and_render_exit_liquidity_from_quotes() -> None:
    st.title("Exit Liquidity Quote Explorer")
    _render_methodology()

    chain_base_asset_groups = {
        (ETH_CHAIN, "WETH"): (AUTO_ETH, AUTO_LRT, BAL_ETH, DINERO_ETH),
        (ETH_CHAIN, "USDC"): (AUTO_USD,),
        (ETH_CHAIN, "DOLA"): (AUTO_DOLA,),
        (SONIC_CHAIN, "USD"): (SONIC_USD,),
        (BASE_CHAIN, "WETH"): (BASE_ETH,),
        (BASE_CHAIN, "USD"): (BASE_USD,),
    }

    options = list(chain_base_asset_groups.keys())
    selected_key = st.selectbox(
        "Pick a Chain & Base Asset:", options, format_func=lambda k: f"{k[0].name} chain → {k[1]}"
    )
    if st.button("Fetch exit-liquidity quotes"):
        quote_df, slippage_df = _fetch_quote_and_slippage_data(chain_base_asset_groups[selected_key])
        _render_slippage_plots(slippage_df)
        _render_download_raw_quote_data_buttons(quote_df, slippage_df)


def _render_download_raw_quote_data_buttons(quote_df: pd.DataFrame, slippage_df: pd.DataFrame) -> None:
    """Adds two Streamlit download buttons for the raw data."""
    csv_quotes = quote_df.to_csv(index=False).encode("utf-8")
    csv_slippage = slippage_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="Download Full Quote Data",
        data=csv_quotes,
        file_name="quote_data.csv",
        mime="text/csv",
    )
    st.download_button(
        label="Download Full Slippage Data",
        data=csv_slippage,
        file_name="slippage_data.csv",
        mime="text/csv",
    )


def _render_methodology():
    with st.expander("See Methodology"):
        st.markdown(
            """
# Estimating Excess Slippage From Price Impact on Asset Exits

This method helps quantify how much extra slippage we incur when selling larger chunks of our assets.

1. Reference Price

- Execute a small “reference” sale

- For stablecoins: sell 10 000 units

- For LSTs/LRTs: sell 5 units

Compute the reference price
- Example: sell 5 stETH → receive 4.9 ETH
- Reference price = 4.9 ETH ÷ 5 stETH = 0.98 ETH/stETH

2. Measuring Excess Slippage

- Sell a larger quantity (e.g., 100 stETH → receive 97.5 ETH)
- New price = 97.5 ETH ÷ 100 stETH = 0.975 ETH/stETH

- Excess slippage in basis points (bps):

`slippage_bps = 10 000 * (0.98 - 0.975) ÷ 0.98 ≈ 51 bps`

- This tells you how far the large-sale price has fallen relative to our reference.

3. Key Details

- The quote data source is our swapper API at https://swaps-pricing.tokemaklabs.com/swap-quote-v2.

- Use buyAmount (not minBuyAmount) in all calculations.

- Percent-based scaling: looks at the current balance across each autopool and sells a percentage of it.

- Additional stablecoin checks at quantities [50 000, 100 000, 200 000].

- Deliberately slow: Because of various DEX-aggregator rate limits we need to be slower to avoid spurious 50-90 % “losses” on large sales.

- Outlier mitigation: for each size, perform three quotes (with 12s then and 24s delays) and report the median.

- No data is saved: all data is fetched live each run.

- Because there is latency between the quote requests, the quotes are for different blocks so they are not 1:1 comparable with each other. Treat them as directionally correct rather than exact.

4. Known Issues

- If we are a large share of the pool (e.g. most of pxETH:ETH liquidity), the large-sale quotes can look artificially better because in the real world we would be effectively trading against ourselves.
"""
        )


if __name__ == "__main__":
    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    fetch_and_render_exit_liquidity_from_quotes()

# streamlit run mainnet_launch/pages/exit_liquidity/estimate_exit_liquidity_from_quotes.py
