import aiohttp
import asyncio
import nest_asyncio

from mainnet_launch.constants import DEAD_ADDRESS

nest_asyncio.apply()

_rate_limit = asyncio.Semaphore(50)


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
            return {"same_token": True, **payload, "buyAmount": int(sell_amount), "minBuyAmount": int(sell_amount)}

        else:
            async with session.post(url, json=payload) as resp:
                try:
                    resp.raise_for_status()
                    data = await resp.json()
                    data.update(payload)
                    data["same_token"] = False
                    return data
                except aiohttp.client_exceptions.ClientResponseError as e:
                    return {"same_token": False, **payload, "buyAmount": None, "minBuyAmount": None}
