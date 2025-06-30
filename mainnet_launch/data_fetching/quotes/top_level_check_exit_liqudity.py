"""Stub"""

import asyncio
import aiohttp

import pandas as pd

from mainnet_launch.constants import *
from mainnet_launch.data_fetching.quotes.tokemak_quote_utils import fetch_swap_quote

PORITONS = [round(0.1 * i, 2) for i in range(1, 11)]
PORITONS = [0.1, 1]


# also aggrerate by all autopools
# eg add all the autopools themselves


def get_current_primary_tokens_amounts(autopool: AutopoolConstants) -> dict[str, int]:
    # hit the subgraph to get the info of quantity of primary assets by asset type
    pass
    return {"0xae78736Cd615f374D3085123A210448E74Fc6393": 100e18}
    # weth, rETH
    return {WETH(ETH_CHAIN): 100e18, "0xae78736Cd615f374D3085123A210448E74Fc6393": 100e18}  # mock


async def fetch_quotes(current_raw_balances: dict[str, int], autopool: AutopoolConstants) -> pd.DataFrame:
    session = aiohttp.ClientSession()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for sell_token_address, raw_amount in current_raw_balances.items():
            for portion in PORITONS:
                scaled_sell_raw_amount = int(raw_amount * portion)  # might have scaling problems

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
        return quote_df


async def main():
    autopool = AUTO_LRT
    current_raw_balances = get_current_primary_tokens_amounts(autopool)

    quote_df = await fetch_quotes(current_raw_balances, autopool)
    return quote_df
