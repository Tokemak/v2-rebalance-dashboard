import asyncio
import aiohttp

import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.data_fetching.quotes.tokemak_quote_utils import fetch_swap_quote
import streamlit as st


import asyncio
import concurrent.futures

ATTEMPTS = 3
STABLE_COINS_REFERENCE_QUANTITY = 10_000
ETH_REFERENCE_QUANTITY = 5
PORTIONS_TO_CHECK = [0.01, 0.05, 0.1, 0.25]


def run_async_safely(coro):
    """
    Sync wrapper around any coroutine. Works whether or not an event loop is already running.
    If there's no running loop: uses asyncio.run.
    If there is one: runs the coroutine in a separate thread's new loop and blocks for the result.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # no running loop
        return asyncio.run(coro)

    # if we get here, there is a running loop; run in separate thread to avoid reentrancy issues
    def _runner(c):
        return asyncio.run(c)  # safe: new loop inside thread

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as exe:
        future = exe.submit(_runner, coro)
        return future.result()


def _post_process_quote_df(
    all_quotes: list[dict],
    tokens_df: pd.DataFrame,
    base_asset_decimals: int,
    sell_token_to_reference_quantity: dict[str, float],
) -> pd.DataFrame:

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

    return quote_df


def _build_sell_token_sell_amount_tuples(
    reference_quantity: float,
    asset_exposure: dict[str, int],
    portion_to_check: list[float],
) -> list[tuple[str, float]]:

    quotes_to_fetch = []
    for sell_token_address, exposure in asset_exposure.items():
        for portion in portion_to_check:
            scaled_down_exposure = exposure * portion
            quotes_to_fetch.append(
                {
                    "sell_token_address": sell_token_address,
                    "sell_amount": scaled_down_exposure,
                }
            )

    # this is not right with decimals,
    for sell_token_address, _ in asset_exposure.items():
        quotes_to_fetch.append({"sell_token_address": sell_token_address, "sell_amount": reference_quantity})


def fetch_quotes(
    chain: ChainData,
    base_asset: str,
    base_asset_decimals: int,
    current_raw_balances: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch quotes for the given balances and chain.
    Returns a DataFrame with quotes and a DataFrame with slippage data.
    """
    progress_bar = st.progress(0, text="Fetching quotes...")

    quote_df, slippage_df = run_async_safely(
        fetch_quotes_OLD(chain, base_asset, base_asset_decimals, current_raw_balances)
    )

    progress_bar.empty()
    return quote_df, slippage_df


async def fetch_quotes_OLD(
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

    progress_bar = st.progress(0, text="Fetching quotes...")
    token_to_decimals = tokens_df.set_index("token_address")["decimals"].to_dict()

    if base_asset in WETH:
        total_needed_quotes = len(current_raw_balances.keys()) * (len(PORTIONS_TO_CHECK) + 1) * ATTEMPTS
    elif (base_asset in USDC) or (base_asset in DOLA):
        # + 3 is for the constant stable coin amounts
        total_needed_quotes = len(current_raw_balances.keys()) * (len(PORTIONS_TO_CHECK) + 1 + 3) * ATTEMPTS

    all_quotes = []
    async with aiohttp.ClientSession() as session:
        for attempt in range(ATTEMPTS):
            tasks = []
            sell_token_to_reference_quantity = {}
            if attempt > 0:
                st.write(f"sleeping for {12 * (attempt)} seconds to avoid rate limits")
                time.sleep(12 * (attempt))
            for sell_token_address, raw_amount in current_raw_balances.items():

                amounts_to_check = [int(raw_amount * portion) for portion in PORTIONS_TO_CHECK]
                if base_asset in WETH:
                    sell_token_to_reference_quantity[sell_token_address] = ETH_REFERENCE_QUANTITY
                    amounts_to_check.append(5e18)
                elif (base_asset in USDC) or (base_asset in DOLA):
                    reference_quantity = STABLE_COINS_REFERENCE_QUANTITY * (10 ** token_to_decimals[sell_token_address])
                    sell_token_to_reference_quantity[sell_token_address] = STABLE_COINS_REFERENCE_QUANTITY
                    amounts_to_check.append(reference_quantity)

                    # for stable coins also add these checks for constants
                    for constant_stable_coin_amounts in [50_000, 100_000, 200_000]:
                        amounts_to_check.append(
                            constant_stable_coin_amounts * (10 ** token_to_decimals[sell_token_address])
                        )
                else:
                    raise ValueError(
                        f"{base_asset=} is not a stable coin or ETH, "
                        f"so we cannot use it to compute the reference quantity for {sell_token_address}"
                    )

                for scaled_sell_raw_amount in amounts_to_check:

                    def make_rate_limited_fetch(session, chain_id, sell_token, buy_token, sell_amount):
                        # this inner function is needed to avoid
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

    quote_df = _post_process_quote_df(all_quotes, tokens_df, base_asset_decimals, sell_token_to_reference_quantity)
    slippage_df = compute_excess_slippage_from_size(quote_df)

    return quote_df, slippage_df


def compute_excess_slippage_from_size(quote_df: pd.DataFrame) -> pd.DataFrame:
    # note, this is in
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
