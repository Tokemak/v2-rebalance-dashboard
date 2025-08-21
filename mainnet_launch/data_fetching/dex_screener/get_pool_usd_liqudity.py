import asyncio
import aiohttp
from datetime import datetime, timezone

import pandas as pd
from aiolimiter import AsyncLimiter
import nest_asyncio
import concurrent.futures

from mainnet_launch.constants.constants import ChainData, ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN

# nest_asyncio.apply()


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


RATE_LIMITER = AsyncLimiter(max_rate=250, time_period=60)


async def _get_json_with_retry(session: aiohttp.ClientSession, url: str):
    """Fetch JSON from `url`, retrying after 60 s if we hit 429."""
    while True:
        async with RATE_LIMITER:
            async with session.get(url, timeout=30) as resp:
                if resp.status == 429:
                    # Too many requests: wait and retry
                    await asyncio.sleep(60)
                    continue
                resp.raise_for_status()
                return await resp.json()


async def fetch_dex_pair(session: aiohttp.ClientSession, chain: ChainData, pool_address: str):
    """This gets the USD liquidity on each side, with 429‐retry logic."""
    datetime_requested = datetime.now(timezone.utc)
    chain_slug = _chain_to_dex_screener_slug(chain)
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain_slug}/{pool_address.lower()}"
    data = await _get_json_with_retry(session, url)

    pair = data.get("pair") or {}
    datetime_received = datetime.now(timezone.utc)
    return {
        **pair,
        "pairAddress": pool_address,
        "datetime_requested": datetime_requested,
        "datetime_received": datetime_received,
    }


async def fetch_token_pairs(session: aiohttp.ClientSession, chain: ChainData, token_address: str):
    """Get the pools found that contain token_address on chain, with 429‐retry logic."""
    datetime_requested = datetime.now(timezone.utc)
    chain_slug = _chain_to_dex_screener_slug(chain)
    url = f"https://api.dexscreener.com/token-pairs/v1/{chain_slug}/{token_address.lower()}"
    data = await _get_json_with_retry(session, url)

    datetime_received = datetime.now(timezone.utc)
    for pool in data:
        pool["datetime_requested"] = datetime_requested
        pool["datetime_received"] = datetime_received
    return data


async def _get_dex_sided_liquidity(chain: ChainData, pool_addresses: list[str]):
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*(fetch_dex_pair(session, chain, addr) for addr in pool_addresses))

    dex_df = pd.DataFrame.from_records(results)
    liq = pd.json_normalize(dex_df["liquidity"]).add_prefix("liquidity_")
    base_token = pd.json_normalize(dex_df["baseToken"]).add_prefix("base_token_")
    quote_token = pd.json_normalize(dex_df["quoteToken"]).add_prefix("quote_token_")
    dex_df = pd.concat(
        [dex_df.drop(columns=["baseToken", "quoteToken", "liquidity"]), base_token, quote_token, liq],
        axis=1,
    )
    return dex_df


async def _get_token_pair_pools(chain: ChainData, token_addresses: list[str]) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_token_pairs(session, chain, addr) for addr in token_addresses]
        results = await asyncio.gather(*tasks)
    # flatten the list of lists
    results = [item for sublist in results for item in sublist]

    pairs_df = pd.DataFrame(results)
    liq = pd.json_normalize(pairs_df["liquidity"]).add_prefix("liquidity_")
    base_token = pd.json_normalize(pairs_df["baseToken"]).add_prefix("base_token_")
    quote_token = pd.json_normalize(pairs_df["quoteToken"]).add_prefix("quote_token_")
    flat_pair_df = pd.concat(
        [pairs_df[["pairAddress", "dexId"]], base_token, quote_token, liq],
        axis=1,
    )
    return flat_pair_df


def get_liquidity_quantities_of_many_pools(chain: ChainData, pool_addresses: list[str]) -> pd.DataFrame:
    """
    Fetches the USD liquidity on each side of the pool.
    :param chain: The blockchain network.
    :param pool_addresses: List of pool addresses to fetch liquidity for.
    :return: DataFrame containing the liquidity information for each pool.
    """
    return run_async_safely(_get_dex_sided_liquidity(chain, pool_addresses))


def get_many_pairs_from_dex_screener(chain: ChainData, token_addresses: list[str]) -> pd.DataFrame:
    """
    Fetches pairs from DexScreener for a list of token addresses.
    :param chain: The blockchain network.
    :param token_addresses: List of token addresses to fetch pairs for.
    :return: DataFrame containing the pairs information.
    """
    return run_async_safely(_get_token_pair_pools(chain, token_addresses))


def _chain_to_dex_screener_slug(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "ethereum"
    elif chain == BASE_CHAIN:
        return "base"  # not tested
    elif chain == SONIC_CHAIN:
        return "sonic"  # not tested
    raise ValueError(f"Unsupported chain: {chain}")


if __name__ == "__main__":
    # Example usage
    chain = ETH_CHAIN
    token_addresses = ["0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"]
    df = get_many_pairs_from_dex_screener(chain, token_addresses)
    print(df.head())
