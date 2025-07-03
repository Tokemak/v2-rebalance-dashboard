import asyncio
import aiohttp

import pandas as pd
from datetime import datetime

from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df
from mainnet_launch.data_fetching.quotes.tokemak_quote_utils import fetch_swap_quote


# PORITONS = [round(0.1 * i, 1) for i in range(1, 11)]
PORITONS = [round(0.2 * i, 1) for i in range(1, 6)]  # 20% chunks


async def fetch_quotes(
    autopool: AutopoolConstants,
    current_raw_balances: dict[str, int],
) -> pd.DataFrame:
    """
    Note this is not exact, because of latency in the solver

    Even if I ask for a bunch of quotes all at time t,

    the blocks might change between them so the quotes can be slightly different.

    This should be thought of as an approximation not an exact answer.
    """
    start = datetime.now()
    tokens_df = get_full_table_as_df(Tokens, where_clause=Tokens.chain_id == autopool.chain.chain_id)

    token_to_decimals = tokens_df.set_index("token_address")["decimals"].to_dict()

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sell_token_address, raw_amount in current_raw_balances.items():
            amounts_to_check = [int(raw_amount * portion) for portion in PORITONS]

            # selling 1 unit of the token is the reference point for slippage
            one_unit_of_sell_token = 10 ** token_to_decimals[sell_token_address]
            amounts_to_check.append(one_unit_of_sell_token)

            for scaled_sell_raw_amount in amounts_to_check:
                task = fetch_swap_quote(
                    session=session,
                    chain_id=autopool.chain.chain_id,
                    sell_token=sell_token_address,
                    buy_token=autopool.base_asset,
                    sell_amount=scaled_sell_raw_amount,
                )

                tasks.append(task)

        quotes = await asyncio.gather(*tasks)

    quote_df = pd.DataFrame.from_records(quotes)
    quote_df = pd.merge(quote_df, tokens_df, how="left", left_on="sellToken", right_on="token_address")
    quote_df["buy_amount_norm"] = quote_df.apply(
        lambda row: int(row["buyAmount"]) / (10**autopool.base_asset_decimals) if pd.notna(row["buyAmount"]) else None,
        axis=1,
    )
    quote_df["min_buy_amount_norm"] = quote_df.apply(
        lambda row: (
            int(row["minBuyAmount"]) / (10**autopool.base_asset_decimals) if pd.notna(row["minBuyAmount"]) else None
        ),
        axis=1,
    )
    quote_df["sell_amount_norm"] = quote_df.apply(
        lambda row: int(row["sellAmount"]) / (10 ** row["decimals"]) if pd.notna(row["sellAmount"]) else None, axis=1
    )
    quote_df["ratio"] = quote_df["buy_amount_norm"] / quote_df["sell_amount_norm"]
    quote_df["min_buy_amount_ratio"] = quote_df["min_buy_amount_norm"] / quote_df["sell_amount_norm"]
    slippage_df = compute_excess_slippage_from_size(quote_df)

    print(datetime.now() - start)
    return quote_df, slippage_df


def compute_excess_slippage_from_size(quote_df: pd.DataFrame) -> pd.DataFrame:

    # todo add min_buy_amount_ratio
    slippage_df = quote_df.groupby(["symbol", "sell_amount_norm"])["ratio"].first().reset_index()
    buy_token_ratio_at_sell_1_token = (
        slippage_df[slippage_df["sell_amount_norm"] == 1][["symbol", "ratio"]].set_index("symbol").to_dict()["ratio"]
    )
    highest_sold_amount = slippage_df.groupby("symbol")["sell_amount_norm"].max().to_dict()

    slippage_df["buy_token_ratio_at_one_token_sold"] = slippage_df["symbol"].map(buy_token_ratio_at_sell_1_token)
    slippage_df["highest_sold_amount"] = slippage_df["symbol"].map(highest_sold_amount)
    slippage_df["percent_sold"] = slippage_df.apply(
        lambda row: round(100 * row["sell_amount_norm"] / row["highest_sold_amount"], 2), axis=1
    )
    slippage_df["bps_excess_loss_vs_1"] = slippage_df.apply(
        lambda row: 10_000 * (row["buy_token_ratio_at_one_token_sold"] - row["ratio"]) / row["ratio"], axis=1
    )
    return slippage_df
