import aiohttp
import asyncio
import nest_asyncio
import pandas as pd


from mainnet_launch.constants import *
from mainnet_launch.database.schema.full import *
from mainnet_launch.database.schema.postgres_operations import *
import numpy as np

nest_asyncio.apply()

_rate_limit = asyncio.Semaphore(10)


async def fetch_swap_quote(
    session: aiohttp.ClientSession,
    chain_id: int,
    sell_token: str,
    buy_token: str,
    sell_amount: int,
    system_name: str = "gen3",
    slippage_bps: int = 50,
    include_sources: str = "",
    exclude_sources: str = "Bebop",
    sell_all: bool = True,
    timeout_ms: int = None,
    transfer_to_caller: bool = True,
) -> dict:
    # todo add

    async with _rate_limit:
        url = "https://swaps-pricing.tokemaklabs.com/swap-quote-v2"
        payload = {
            "chainId": chain_id,
            "systemName": system_name,
            "slippageBps": slippage_bps,
            "taker": DEAD_ADDRESS,
            "sellToken": sell_token,
            "buyToken": buy_token,
            "sellAmount": str(int(sell_amount)),
            "includeSources": include_sources,
            "excludeSources": exclude_sources,
            "sellAll": sell_all,
            "timeoutMS": str(timeout_ms) if timeout_ms is not None else "",
            "transferToCaller": transfer_to_caller,
        }
        if sell_token.lower() == buy_token.lower():
            return {"same_token": True, **payload}

        else:
            async with session.post(url, json=payload) as resp:
                resp.raise_for_status()
                data = await resp.json()
                data.update(payload)
                data["same_token"] = False
                return data
