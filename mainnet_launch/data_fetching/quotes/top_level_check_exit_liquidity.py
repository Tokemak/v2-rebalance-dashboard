import asyncio
import aiohttp

import pandas as pd
from datetime import datetime

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.data_fetching.quotes.tokemak_quote_utils import fetch_swap_quote
import streamlit as st

# PORTIONS_TO_CHECK = [0.1, 1]

PORTIONS_TO_CHECK = [0.025, 0.05, 0.1, 0.25, 1]
ATTEMPTS = 3


async def fetch_quotes(
    chain: ChainData,
    base_asset: str,
    base_asset_decimals: int,
    current_raw_balances: dict[str, int],
) -> pd.DataFrame:
    """
    Note this is not exact, because of latency in the solver

    Even if I ask for a bunch of quotes all at time t,

    the blocks might change between them so the quotes can be slightly different.

    This should be thought of as an approximation not an exact answer.
    """
    tokemak_swap_quote_api_rate_limit = asyncio.Semaphore(5)
    # at most 5 calls / second
    # and wait 12 seconds after each batch of quotes

    tokens_df = get_full_table_as_df(Tokens, where_clause=Tokens.chain_id == chain.chain_id)

    token_to_symbol = tokens_df.set_index("token_address")["symbol"].to_dict()

    token_to_decimals = tokens_df.set_index("token_address")["decimals"].to_dict()

    for sell_token_address, raw_amount in current_raw_balances.items():
        symbol = token_to_symbol[sell_token_address]
        amount = raw_amount / (10 ** token_to_decimals[sell_token_address])
        st.write(symbol, amount)

    total_needed_quotes = len(current_raw_balances.keys()) * (len(PORTIONS_TO_CHECK) + 1) * ATTEMPTS

    # for some unkdnown reason it is not selling the max amount at 100%
    all_quotes = []
    progress_bar = st.progress(0.0, text=f"Fetching {total_needed_quotes} quotes...")
    async with aiohttp.ClientSession() as session:
        for attempt in range(ATTEMPTS):
            tasks = []
            sell_token_to_reference_quantity = {}
            for sell_token_address, raw_amount in current_raw_balances.items():

                amounts_to_check = [int(raw_amount * portion) for portion in PORTIONS_TO_CHECK]
                if base_asset in WETH:
                    sell_token_to_reference_quantity[sell_token_address] = 5
                    # 5 rETH, stETH etc
                    amounts_to_check.append(5e18)
                if (base_asset in USDC) or (base_asset in DOLA):
                    sell_token_decimals = token_to_decimals[sell_token_address]
                    reference_quantity = 10_000 * (10**sell_token_decimals)
                    sell_token_to_reference_quantity[sell_token_address] = 10_000
                    amounts_to_check.append(reference_quantity)

                for scaled_sell_raw_amount in amounts_to_check:

                    def make_rate_limited_fetch(session, chain_id, sell_token, buy_token, sell_amount):
                        # needed to avoid
                        # RuntimeError:
                        # <asyncio.locks.Semaphore object at 0x12d65c250 [locked]> is bound to a different event loop

                        async def _inner():
                            async with tokemak_swap_quote_api_rate_limit:
                                return await fetch_swap_quote(
                                    session=session,
                                    chain_id=chain_id,
                                    sell_token=sell_token,
                                    buy_token=buy_token,
                                    sell_amount=sell_amount,
                                )

                        return _inner()

                    task = make_rate_limited_fetch(
                        session=session,
                        chain_id=chain.chain_id,
                        sell_token=sell_token_address,
                        buy_token=base_asset,
                        sell_amount=scaled_sell_raw_amount,
                    )
                    tasks.append(task)

            for future in asyncio.as_completed(tasks):
                quote = await future
                all_quotes.append(quote)
                portion_done = len(all_quotes) / total_needed_quotes
                portion_done = 1 if portion_done > 1 else portion_done
                progress_bar.progress(portion_done, text=f"Fetched quotes: {len(all_quotes)}/{total_needed_quotes}")

            # ETh block time is 12 seconds, so
            # 12 seconds + solver plan latency makes sure we get a new block
            # time.sleep(12)
            # may add later

    quote_df = pd.DataFrame.from_records(all_quotes)
    quote_df = pd.merge(quote_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
    quote_df["buy_amount_norm"] = quote_df.apply(
        lambda row: int(row["buyAmount"]) / (10**base_asset_decimals) if pd.notna(row["buyAmount"]) else None,
        axis=1,
    )
    quote_df["min_buy_amount_norm"] = quote_df.apply(
        lambda row: (int(row["minBuyAmount"]) / (10**base_asset_decimals) if pd.notna(row["minBuyAmount"]) else None),
        axis=1,
    )
    quote_df["Sold Quantity"] = quote_df.apply(
        lambda row: int(row["sellAmount"]) / (10 ** row["decimals"]) if pd.notna(row["sellAmount"]) else None, axis=1
    )
    quote_df["token_price"] = quote_df["buy_amount_norm"] / quote_df["Sold Quantity"]
    quote_df["min_token_price"] = quote_df["min_buy_amount_norm"] / quote_df["Sold Quantity"]
    quote_df["reference_quantity"] = quote_df["sellToken"].map(sell_token_to_reference_quantity)

    slippage_df = compute_excess_slippage_from_size(quote_df)

    return quote_df, slippage_df


def compute_excess_slippage_from_size(quote_df: pd.DataFrame) -> pd.DataFrame:
    # todo add min_buy_amount_ratio
    slippage_df = (
        quote_df.groupby(["symbol", "Sold Quantity"])[["buy_amount_norm", "token_price", "reference_quantity"]]
        .median()
        .reset_index()
    )

    token_price_at_reference_quantity = (
        slippage_df[slippage_df["reference_quantity"] == slippage_df["Sold Quantity"].astype(int)]
        .set_index("symbol")["token_price"]
        .to_dict()
    )

    slippage_df["token_price_at_reference_quantity"] = slippage_df["symbol"].map(token_price_at_reference_quantity)

    highest_sold_amount = slippage_df.groupby("symbol")["Sold Quantity"].max().to_dict()

    slippage_df["highest_sold_amount"] = slippage_df["symbol"].map(highest_sold_amount)

    slippage_df["percent_sold"] = slippage_df.apply(
        lambda row: round(100 * row["Sold Quantity"] / row["highest_sold_amount"], 2), axis=1
    )
    slippage_df["bps_loss_excess_vs_reference_price"] = slippage_df.apply(
        lambda row: 10_000 * (row["token_price_at_reference_quantity"] - row["token_price"]) / row["token_price"],
        axis=1,
    )
    return slippage_df
