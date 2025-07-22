import pandas as pd
import plotly.express as px
import streamlit as st

from mainnet_launch.constants import *

from mainnet_launch.data_fetching.quotes.get_all_underlying_reserves import fetch_raw_amounts_by_destination
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df


def _fetch_autopool_exposure_quantities(autopool: AutopoolConstants) -> dict:
    """Fetches the percent ownership by pool for the given autopool."""

    block = autopool.chain.client.eth.block_number
    reserve_df = fetch_raw_amounts_by_destination(block, autopool.chain)
    reserve_df["reserve_amount"] = reserve_df["reserve_amount"].map(int)
    balances = reserve_df.groupby("token_address")["reserve_amount"].sum().reset_index()
    tokens_table = get_full_table_as_df(Tokens, where_clause=Tokens.token_address.in_(balances["token_address"]))
    balances["token_symbol"] = balances["token_address"].map(tokens_table.set_index("token_address")["symbol"])
    balances["decimals"] = balances["token_address"].map(tokens_table.set_index("token_address")["decimals"])
    balances["norm_reserve_amount"] = balances["reserve_amount"] / (10 ** balances["decimals"])
    return balances


if __name__ == "__main__":
    print(_fetch_autopool_exposure_quantities(AUTO_ETH))
