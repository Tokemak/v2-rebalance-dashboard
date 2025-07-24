import asyncio
import aiohttp
from datetime import datetime, timezone

import pandas as pd
from aiolimiter import AsyncLimiter

RATE_LIMITER = AsyncLimiter(max_rate=250, time_period=60)

async def fetch_dex_pair(session, chain: str, pool_address: str):
    """This gets the USD liqudity on each side"""
    async with RATE_LIMITER:
        datetime_requested = datetime.now(timezone.utc)

        async with session.get(
            f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{pool_address.lower()}", timeout=30
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            pair = data.get("pair", {})
            datetime_received = datetime.now(timezone.utc)
            return {
                **pair,
                "pairAddress": pool_address,
                "datetime_requested": datetime_requested,
                "datetime_received": datetime_received,
            }


async def get_dex_sided_liquidity(chain: str, pool_addresses: list[str]):
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*(fetch_dex_pair(session, chain, addr) for addr in pool_addresses))

    dex_df = pd.DataFrame.from_records(results)
    liq = pd.json_normalize(dex_df["liquidity"]).add_prefix("liquidity_")
    base_token = pd.json_normalize(dex_df["baseToken"]).add_prefix("base_token_")
    quote_token = pd.json_normalize(dex_df["quoteToken"]).add_prefix("quote_token_")
    dex_df = pd.concat([dex_df.drop(columns=['baseToken', 'quoteToken','liquidity']), base_token, quote_token, liq], axis=1)
    return dex_df


async def fetch_token_pairs(session, chain: str, token_address: str):
    """Get the pools found that cointain token_address on chain"""
    url = f"https://api.dexscreener.com/token-pairs/v1/{chain}/{token_address.lower()}"
    async with RATE_LIMITER:
        datetime_requested = datetime.now(timezone.utc)
        async with session.get(url, timeout=30) as resp:
            resp.raise_for_status()
            data = await resp.json()
            datetime_received = datetime.now(timezone.utc)

            for pool in data:
                pool["datetime_requested"] = datetime_requested
                pool["datetime_received"] = datetime_received
            return data



async def get_token_pair_pools(
    chain: str,
    token_addresses: list[str],
) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_token_pairs(session, chain, addr) for addr in token_addresses]
        results = await asyncio.gather(*tasks)
        # flatten the list of lists
        results = [item for sublist in results for item in sublist]

    pairs_df = pd.DataFrame(results)
    liq = pd.json_normalize(pairs_df["liquidity"]).add_prefix("liquidity_")
    base_token = pd.json_normalize(pairs_df["baseToken"]).add_prefix("base_token_")
    quote_token = pd.json_normalize(pairs_df["quoteToken"]).add_prefix("quote_token_")
    flat_pair_df = pd.concat([pairs_df[["pairAddress", "dexId"]], base_token, quote_token, liq], axis=1)
    return flat_pair_df


if __name__ == "__main__":
    # Example usage
    chain = "ethereum"

    token_addresses = ["0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"]
    df = asyncio.run(get_token_pair_pools(chain, token_addresses))

    few_pools = ['0x6951bDC4734b9f7F3E1B74afeBC670c736A0EDB6','0x88794C65550DeB6b4087B7552eCf295113794410']

    dex_df = asyncio.run(get_dex_sided_liquidity(chain, few_pools))
