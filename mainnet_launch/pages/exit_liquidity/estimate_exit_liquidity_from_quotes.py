import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd

from pathlib import Path
from datetime import datetime

from mainnet_launch.constants import AutopoolConstants, ALL_AUTOPOOLS
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes

def fetch_and_render_exit_liquidity_from_quotes(
    autopool: AutopoolConstants,
    output_dir: Path,
    timestamp: str
) -> None:
    # 1) fetch data
    block = autopool.chain.client.eth.block_number
    reserve_df = fetch_raw_amounts_by_destination(block, autopool.chain)
    balances = (
        reserve_df[reserve_df["autopool_symbol"] == autopool.symbol]
        .groupby("token_address")["reserve_amount"]
        .sum()
    ).to_dict()

    quote_df, slippage_df = asyncio.run(fetch_quotes(autopool, balances))

    # 2) ensure quotes/ exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3) build timestamped filenames
    quote_path = output_dir / f"{autopool.name}_quotes_{timestamp}.csv"
    slip_path  = output_dir / f"{autopool.name}_slippage_{timestamp}.csv"

    # 4) save CSVs
    quote_df.to_csv(quote_path, index=False)
    slippage_df.to_csv(slip_path, index=False)

    # 5) render charts & download buttons
    st.subheader(f"{autopool.name} — files in `{output_dir.name}`")
    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="percent_sold",
            y="bps_excess_loss_vs_1",
            color="symbol",
            title="Excess slippage bps by % sold",
        )
    )
    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="sell_amount_norm",
            y="bps_excess_loss_vs_1",
            color="symbol",
            title="Excess slippage bps by quantity sold",
        )
    )
    st.plotly_chart(
        px.scatter(
            slippage_df,
            x="sell_amount_norm",
            y="buy_amount_norm",
            color="symbol",
            title="Sell vs. buy amounts",
        )
    )

    st.download_button(
        label="📥 Download Quotes CSV",
        data=quote_path.read_bytes(),
        file_name=quote_path.name,
        mime="text/csv",
        help="Raw quote data",
    )
    st.download_button(
        label="📥 Download Slippage CSV",
        data=slip_path.read_bytes(),
        file_name=slip_path.name,
        mime="text/csv",
        help="Computed slippage",
    )


if __name__ == "__main__":
    # Streamlit setup
    st.set_page_config(page_title="Exit Liquidity Explorer", layout="wide")
    st.title("Exit Liquidity Explorer")

    # single output folder
    quotes_dir = Path("quotes")

    # timestamp for filenames
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    for pool in ALL_AUTOPOOLS:
        st.header(pool.name)
        fetch_and_render_exit_liquidity_from_quotes(pool, quotes_dir, now)
