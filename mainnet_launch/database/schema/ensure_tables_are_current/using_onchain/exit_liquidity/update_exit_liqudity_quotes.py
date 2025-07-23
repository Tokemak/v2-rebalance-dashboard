"""
For each (chain, base asset) pair:

1. Fetch the latest quantity of each asset we hold across each autopool.
2. Using our swapper API, get quotes for selling back to the base asset at various sizes.
"""
import asyncio
import streamlit as st
import plotly.express as px
import pandas as pd


from mainnet_launch.constants import  ChainData, TokemakAddress, AutopoolConstants, ALL_AUTOPOOLS, DOLA, USDC, WETH, ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN
from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.data_fetching.quotes.top_level_check_exit_liquidity import fetch_quotes


USD_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5

STANDARD_USD_QUANTITIES = [USD_REFERENCE_QUANTITY, 35_000, 100_000, 200_000]
STANDARD_ETH_QUANTITIES = [ETH_REFERENCE_QUANTITY, 100, 200]


def _fetch_quote_and_slippage_data(chain: ChainData, base_asset: TokemakAddress):
    valid_autopools = [autopool for autopool in ALL_AUTOPOOLS if autopool.chain == chain and autopool.base_asset == base_asset(autopool.chain)]
    if not valid_autopools:
        # early exit? eg (sonic, ETH) or (base, DOLA) or (sonic, DOLA)
        raise ValueError(f"No valid autopools found for chain {chain.name} and base asset {base_asset}")

    block = chain.client.eth.block_number

    reserve_df = fetch_raw_amounts_by_destination(block, chain)
    valid_autopool_symbols = [pool.symbol for pool in valid_autopools]
    reserve_df = reserve_df[reserve_df["autopool_symbol"].isin(valid_autopool_symbols)].copy()
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)
    balances = reserve_df.groupby("token_address")["reserve_amount"].sum().to_dict()

    quote_df, slippage_df = asyncio.run(
        fetch_quotes(
            chain, base_asset, valid_autopools[0].base_asset_decimals, balances
        )
    )

    return balances, quote_df, slippage_df


# maybe we want a helper table for (chain, base_asset, block, associated_token, quanity (float) we own at this block)
# such that we can get it later?

# I think we do want this


def update_all_exit_liquidity_quotes():
    pass





