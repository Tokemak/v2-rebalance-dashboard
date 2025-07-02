"""Stub"""

import asyncio
import aiohttp

import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.tokemak_quote_utils import fetch_swap_quote
from mainnet_launch.database.schema.full import Tokens
from mainnet_launch.database.schema.postgres_operations import get_full_table_as_df


PORITONS = [round(0.1 * i, 1) for i in range(1, 11)]

# PORITONS= [.1, 1]
# also aggrerate by all autopools
# eg add all the autopools themselves

def get_current_primary_tokens_amounts(autopool: AutopoolConstants) -> dict[str, int]:
    # stub hit the subgraph to get the info of quantity of primary assets by asset type
    # weth, rETH
    wstETH = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
    rETH = "0xae78736Cd615f374D3085123A210448E74Fc6393"
    # WETH(ETH_CHAIN): 1000e18,
    return {rETH: 500e18, wstETH: 1000e18}

    # return {wstETH: 1000e18}


def fetch_token_details_df(autopool: AutopoolConstants, current_raw_balances: dict[str, int]) -> pd.DataFrame:
    token_addresses = [k for k in current_raw_balances.keys()]

    tokens_df = get_full_table_as_df(
        Tokens, where_clause=((Tokens.chain_id == autopool.chain.chain_id) & Tokens.token_address.in_(token_addresses))
    )

    return tokens_df


async def fetch_quotes(autopool: AutopoolConstants, current_raw_balances: dict[str, int], ) -> pd.DataFrame:
    """
    Note this is not exact, because of latency in the solver
    
    Even if I ask for a bunch of quotes all at time t,

    the blocks might change between them so the quotes can be slightly different.

    This should be thought of as an approximation not an exact answer
    """

    tokens_df = fetch_token_details_df(autopool, current_raw_balances)
    token_to_decimals = tokens_df.set_index('token_address')['decimals'].to_dict()

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

        quotes = asyncio.run(asyncio.gather(*tasks))

    quote_df = pd.DataFrame.from_records(quotes)
    quote_df = pd.merge(quote_df, tokens_df, how='left', left_on='sellToken', right_on='token_address')

    quote_df['buy_amount_norm'] = quote_df.apply(
        lambda row: int(row['buyAmount']) / (10 ** autopool.base_asset_decimals)
                    if pd.notna(row['buyAmount'])
                    else None,
        axis=1
    )

    quote_df['min_buy_amount_norm'] = quote_df.apply(
        lambda row: int(row['minBuyAmount']) / (10 ** autopool.base_asset_decimals)
                    if pd.notna(row['minBuyAmount'])
                    else None,
        axis=1
    )

    quote_df['sell_amount_norm'] = quote_df.apply(
        lambda row: int(row['sellAmount']) / (10 ** row['decimals'])
                    if pd.notna(row['sellAmount'])
                    else None,
        axis=1
    )

    quote_df['ratio'] = quote_df['buy_amount_norm'] / quote_df['sell_amount_norm']
    
    return quote_df


async def main():
    autopool = AUTO_LRT
    current_raw_balances = get_current_primary_tokens_amounts(autopool)
    quote_df = await fetch_quotes(autopool, current_raw_balances)
    return quote_df


if __name__ == '__main__':
    asyncio.run(main())