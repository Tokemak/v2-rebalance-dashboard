import asyncio
import aiohttp

import pandas as pd


async def fetch_pair(session, chain: str, pool_address: str):
    def _extract_sided_liqudity_from_dex_screener(data: dict) -> dict:
        """
        Given a DexScreener response JSON (with 'pairs' or 'pair'),
        return a dict mapping each token address to its token liquidity,
        plus the total USD TVL under the key 'usd_tvl'.
        """
        # pick whichever key exists
        pair = data["pair"]
        if pair is None:
            return {"pairAddress": pool_address}

        data["pairAddress"] = pool_address
        return data

    # some of the dex screener endpoints have their liqudity in ETH terms, not USD terms
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{pool_address.lower()}"
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return _extract_sided_liqudity_from_dex_screener(data)


async def get_dex_sided_liquidity(chain: str, pool_addresses: list[str]):
    results = []
    BATCH_SIZE = 200
    # dex screener does not have api keys, but rate limited at 300 requests per minute
    # using 200 to avoid hitting the limit
    for i in range(0, len(pool_addresses), BATCH_SIZE):
        batch = pool_addresses[i : i + BATCH_SIZE]
        async with aiohttp.ClientSession() as session:
            # fire off up to 200 in parallel
            batch_results = await asyncio.gather(*(fetch_pair(session, chain, addr) for addr in batch))
            results.extend(batch_results)

        # if there’s more work to do, wait a minute
        if i + BATCH_SIZE < len(pool_addresses):
            await asyncio.sleep(60)

    return pd.DataFrame(results)
