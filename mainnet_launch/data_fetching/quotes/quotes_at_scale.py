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
_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def fetch_swap_quote(
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
            "sellAmount": str(sell_amount),
            "includeSources": include_sources,
            "excludeSources": "",
            "sellAll": sell_all,
            "timeoutMS": str(timeout_ms) if timeout_ms is not None else "",
            "transferToCaller": str(transfer_to_caller),
        }
        session = await get_session()
        async with session.post(url, json=payload) as resp:
            resp.raise_for_status()
            data = await resp.json()
            data.update(payload)
            return data
        # session = await get_session()
        # async with session.post(url, json=payload) as resp:
        #     try:
        #         resp.raise_for_status()
        #         data = await resp.json()
        #         data.update(payload)
        #     except Exception as e:
        #         data = {"error": str(e), **payload}
        #         raise e
        return data


async def fetch_quote_size_df(
    token_orms: list[Tokens], sizes: list[float], autopool: AutopoolConstants
) -> pd.DataFrame:

    tasks = []

    for size in sizes:
        for t in token_orms:

            tasks.append(
                fetch_swap_quote(
                    chain_id=t.chain_id,
                    sell_token=t.token_address,
                    buy_token=autopool.base_asset,
                    sell_amount=int(size * 10**t.decimals),
                )
            )
    quotes = await asyncio.gather(*tasks)
    quote_df = pd.DataFrame.from_records(quotes)
    return quote_df


async def fetch_quote_df(
    token_orms: list[Tokens], base_sell_amount: float, autopool: AutopoolConstants
) -> pd.DataFrame:
    # launch all requests in parallel
    quotes = await asyncio.gather(
        *(
            fetch_swap_quote(
                chain_id=t.chain_id,
                sell_token=t.token_address,
                buy_token=autopool.base_asset,
                sell_amount=int(base_sell_amount * 10**t.decimals),
            )
            for t in token_orms
        )
    )
    return pd.DataFrame.from_records(quotes)


from mainnet_launch.pages.autopool_exposure.allocation_over_time import _fetch_tvl_by_asset_and_destination


async def main():

    autopool = AUTO_DOLA

    safe_value_by_destination, safe_value_by_asset, backing_value_by_destination, quantity_by_asset = (
        _fetch_tvl_by_asset_and_destination(autopool)
    )
    token_orms = get_full_table_as_orm(Tokens, where_clause=Tokens.chain_id == autopool.chain.chain_id)

    # note this introduces some latency, but not a big deal imo
    quantity_by_asset

    latest_quantity_by_assets = quantity_by_asset.iloc[-1]
    tokens_to_get_quotes_for = [t for t in token_orms if t.symbol in latest_quantity_by_assets.index]
    tokens_to_get_quotes_for

    token_symbol_to_token_orm = {t.symbol: t for t in token_orms}
    all_quotes = []
    for token_symbol, normalized_quantity in latest_quantity_by_assets.items():
        this_token_orm: Tokens = token_symbol_to_token_orm[token_symbol]
        unscaled_quantity = int(normalized_quantity * (10 ** (this_token_orm.decimals)))
        print(token_symbol_to_token_orm[token_symbol], normalized_quantity, unscaled_quantity)

        for scale in range(1, 10):
            percent_of_assets_to_liquidate = scale / 10

            quote = fetch_swap_quote(
                chain_id=autopool.chain.chain_id,
                sell_token=this_token_orm.token_address,
                buy_token=autopool.base_asset,
                sell_amount=int(unscaled_quantity * percent_of_assets_to_liquidate),
            )
            all_quotes.append(quote)
            break

    a_quote = await all_quotes[0]
    return a_quote


if __name__ == "__main__":
    asyncio.run(main())
