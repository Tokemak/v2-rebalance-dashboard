import asyncio
import aiohttp
import nest_asyncio
from datetime import datetime, timezone

import pandas as pd
from aiolimiter import AsyncLimiter
from web3 import Web3

from mainnet_launch.constants import (
    ChainData,
    COINGECKO_API_KEY,
    ETH_CHAIN,
    BASE_CHAIN,
    SONIC_CHAIN,
)

nest_asyncio.apply()

# no idea here on limit
RATE_LIMITER = AsyncLimiter(max_rate=100, time_period=60)


async def _get_json_with_retry(session: aiohttp.ClientSession, url: str, params=None, headers=None):
    """Rate-limit + on‑429 sleep 60 s then retry."""
    while True:
        async with RATE_LIMITER:
            async with session.get(url, params=params, headers=headers, timeout=30) as resp:
                if resp.status == 429:
                    await asyncio.sleep(60)
                    continue
                resp.raise_for_status()
                return await resp.json()


def _chain_to_coingecko_slug_token_prices(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "ethereum"
    elif chain == BASE_CHAIN:
        return "base"
    elif chain == SONIC_CHAIN:
        return "sonic"
    raise ValueError(f"Unsupported chain: {chain}")


def _chain_to_coingecko_slug_network_id(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "eth"
    elif chain == BASE_CHAIN:
        return "base"
    elif chain == SONIC_CHAIN:
        return "sonic"
    raise ValueError(f"Unsupported chain: {chain}")


async def _fetch_token_prices_from_coingecko_async(
    chain: ChainData,
    token_addresses: list[str],
    vs_currencies: str = "usd",
) -> pd.DataFrame:
    slug = _chain_to_coingecko_slug_token_prices(chain)
    url = f"https://pro-api.coingecko.com/api/v3/simple/token_price/{slug}"
    params = {
        "contract_addresses": ",".join(addr.lower() for addr in token_addresses),
        "vs_currencies": vs_currencies,
        "include_market_cap": "false",
        "include_24hr_vol": "false",
        "include_24hr_change": "false",
        "include_last_updated_at": "true",
        "precision": "full",
    }
    headers = {"x-cg-pro-api-key": COINGECKO_API_KEY}

    async with aiohttp.ClientSession() as session:
        data = await _get_json_with_retry(session, url, params=params, headers=headers)

    # build DataFrame
    df = (
        pd.DataFrame.from_dict(data, orient="index")
        .reset_index()
        .rename(columns={"index": "token_address", vs_currencies: f"{vs_currencies}_price"})
    )
    df = df[["token_address", f"{vs_currencies}_price", "last_updated_at"]]
    df["token_address"] = df["token_address"].apply(Web3.toChecksumAddress)
    return df


def fetch_token_prices_from_coingecko(
    chain: ChainData,
    token_addresses: list[str],
    vs_currencies: str = "usd",
) -> pd.DataFrame:
    """
    Sync wrapper around the async fetch.
    """
    return asyncio.run(_fetch_token_prices_from_coingecko_async(chain, token_addresses, vs_currencies))


async def _fetch_pool_by_token_from_coingecko_async(
    session: aiohttp.ClientSession,
    start_token: str,
    chain: ChainData,
) -> pd.DataFrame:
    slug = _chain_to_coingecko_slug_network_id(chain)
    url = f"https://pro-api.coingecko.com/api/v3/onchain/networks/" f"{slug}/tokens/{start_token}/pools"
    headers = {"x-cg-pro-api-key": COINGECKO_API_KEY}
    params = {"sort": "h24_volume_usd_liquidity_desc", "include": ["base_token", "quote_token"]}

    data = await _get_json_with_retry(session, url, params=params, headers=headers)
    pools = data.get("data", [])
    return pd.DataFrame(pools)


# async def _fetch_n_hops_from_coingecko_async(
#     tokens_to_check: set[str],
#     chain: ChainData,
#     min_USD_reserves: int = 1,
#     n_hops: int = 1,
# ) -> pd.DataFrame:
#     tokens_to_check = {t.lower() for t in tokens_to_check}
#     checked: set[str] = set()
#     all_pools = pd.DataFrame()

#     async with aiohttp.ClientSession() as session:
#         for hop in range(n_hops):
#             to_do = [t for t in tokens_to_check if t not in checked]
#             if not to_do:
#                 break

#             # fetch all this hop in parallel
#             tasks = [
#                 _fetch_pool_by_token_from_coingecko_async(session, tok, chain)
#                 for tok in to_do
#             ]
#             results = await asyncio.gather(*tasks)
#             hop_df = pd.concat(results, ignore_index=True)

#             # optional filter by min_USD_reserves if that field exists
#             if "liquidity" in hop_df.columns:
#                 hop_df = hop_df[hop_df["liquidity"].apply(lambda L: L.get("usd", 0) >= min_USD_reserves)]

#             all_pools = pd.concat([all_pools, hop_df], ignore_index=True)
#             checked.update(to_do)

#             # prepare next hop
#             next_tokens = set(hop_df["quote_token_id"].tolist() + hop_df["base_token_id"].tolist())
#             tokens_to_check = {t.lower() for t in next_tokens}

#     return all_pools.drop_duplicates()


# def fetch_n_hops_from_tokens_with_coingecko(
#     tokens_to_check: set[str],
#     chain: ChainData,
#     min_USD_reserves: int = 1,
#     n_hops: int = 1,
# ) -> pd.DataFrame:
#     return asyncio.run(
#         _fetch_n_hops_from_coingecko_async(tokens_to_check, chain, min_USD_reserves, n_hops)
#     )


async def _fetch_many_pairs_from_coingecko_async(
    tokens_to_check: list[str],
    chain: ChainData,
) -> pd.DataFrame:
    unique = {t.lower() for t in tokens_to_check}
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_pool_by_token_from_coingecko_async(session, tok, chain) for tok in unique]
        dfs = await asyncio.gather(*tasks)

    # flatten attributes & relationships
    out = []
    for df in dfs:
        if df.empty:
            continue
        attrs = pd.json_normalize(df["attributes"]).add_prefix("attr_")
        rels = pd.json_normalize(df["relationships"]).add_prefix("rel_")
        flat = pd.concat(
            [df.drop(columns=["attributes", "relationships"]), attrs, rels],
            axis=1,
        )
        out.append(flat)

    return pd.concat(out, ignore_index=True)


def fetch_many_pairs_from_coingecko(tokens_to_check: list[str], chain: ChainData) -> pd.DataFrame:
    """
    Sync wrapper around the async “many at once” pool fetch.
    """
    return asyncio.run(_fetch_many_pairs_from_coingecko_async(tokens_to_check, chain))


if __name__ == "__main__":
    # example usage
    chain = ETH_CHAIN
    tokens = [
        "0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367",
        # ...
    ]

    price_df = fetch_token_prices_from_coingecko(chain, tokens[:3])
    print(price_df.head())

    pools_df = fetch_many_pairs_from_coingecko(tokens, chain)
    print(pools_df.head())

    # hops_df = fetch_n_hops_from_tokens_with_coingecko(set(tokens), chain, min_USD_reserves=10, n_hops=2)
    # print(hops_df.shape)
