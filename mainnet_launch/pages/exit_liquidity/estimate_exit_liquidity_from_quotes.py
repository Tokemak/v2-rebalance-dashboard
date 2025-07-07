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
    # 1) fetch data
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
            hover_data=["sell_amount_norm"],
            title="Excess slippage bps by % sold",
        )
    )
    # # 2) ensure quotes/ exists
    # output_dir.mkdir(parents=True, exist_ok=True)

    # 3) build timestamped filenames
    # quote_path = output_dir / f"{autopool.name}_quotes_{timestamp}.csv"
    # slip_path = output_dir / f"{autopool.name}_slippage_{timestamp}.csv"

    # # 4) save CSVs
    # quote_df.to_csv(quote_path, index=False)
    # slippage_df.to_csv(slip_path, index=False)

    # 5) render charts & download buttons
    # st.subheader(f"{autopool.name} — files in `{output_dir.name}`")

    # st.download_button(
    #     label="📥 Download Quotes CSV",
    #     data=quote_path.read_bytes(),
    #     file_name=quote_path.name,
    #     mime="text/csv",
    #     help="Raw quote data",
    # )
    # st.download_button(
    #     label="📥 Download Slippage CSV",
    #     data=slip_path.read_bytes(),
    #     file_name=slip_path.name,
    #     mime="text/csv",
    #     help="Computed slippage",
    # )


if __name__ == "__main__":

    from mainnet_launch.constants import AUTO_LRT

    # Streamlit setup
    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    st.title("Exit Liquidity Explorer")

    quotes_dir = Path("mainnet_launch/data_fetching/quotes/local_quote_data/")
    now = int(datetime.now().timestamp())

    # fetch_and_render_exit_liquidity_from_quotes(AUTO_LRT, quotes_dir, now)
    
    for pool in ALL_AUTOPOOLS:
        st.header(pool.name)
        fetch_and_render_exit_liquidity_from_quotes(pool, quotes_dir, now)

# streamlit run mainnet_launch/pages/exit_liquidity/estimate_exit_liquidity_from_quotes.py
