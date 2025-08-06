import pandas as pd
from web3 import Web3

from mainnet_launch.constants import (
    ChainData,
    COINGECKO_API_KEY,
    ETH_CHAIN,
    BASE_CHAIN,
    SONIC_CHAIN,
)


from mainnet_launch.data_fetching.fetch_data_from_3rd_party_api import (
    make_many_get_requests_to_3rd_party,
    make_single_get_request_to_3rd_party,
)


_CHAIN_TO_COINGECKO_SLUGS = {
    ETH_CHAIN: {"token_prices": "ethereum", "network_id": "eth"},
    BASE_CHAIN: {"token_prices": "base", "network_id": "base"},
    SONIC_CHAIN: {"token_prices": "sonic", "network_id": "sonic"},
}


def fetch_token_prices_from_coingecko(
    chain: ChainData,
    token_addresses: list[str],
    vs_currencies: str = "usd",
) -> pd.DataFrame:
    slug = _CHAIN_TO_COINGECKO_SLUGS[chain]["token_prices"]
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

    data = make_single_get_request_to_3rd_party(
        url,
        params=params,
        headers=headers,
    )
    if not data["failed"]:
        data.pop("failed")
    else:
        raise ValueError(f"Failed to fetch data from Coingecko: {data}")

    df = (
        pd.DataFrame.from_dict(data, orient="index")
        .reset_index()
        .rename(columns={"index": "token_address", vs_currencies: f"{vs_currencies}_price"})
    )
    df = df[["token_address", f"{vs_currencies}_price", "last_updated_at"]]
    df["token_address"] = df["token_address"].apply(Web3.toChecksumAddress)
    return df


def fetch_many_pairs_from_coingecko(
    chain: ChainData,
    tokens_to_check: list[str],
) -> pd.DataFrame:
    """Note this includes coingecko's price of each token in the 'quote' and 'base' section"""

    slug = _CHAIN_TO_COINGECKO_SLUGS[chain]["network_id"]

    urls, params_list, headers_list = [], [], []
    for token_address in tokens_to_check:
        # 10 pages is the max for the coingecko API
        for page in range(1, 11):
            urls.append(f"https://pro-api.coingecko.com/api/v3/onchain/networks/{slug}/tokens/{token_address}/pools")
            params_list.append(
                {"sort": "h24_volume_usd_liquidity_desc", "include": ["base_token", "quote_token", "dex"], "page": page}
            )
            headers_list.append({"x-cg-pro-api-key": COINGECKO_API_KEY})

    datas = make_many_get_requests_to_3rd_party(
        rate_limit_max_rate=100,
        rate_limit_time_period=60,
        urls=urls,
        params_list=params_list,
        headers_list=headers_list,
    )

    # included_pools = []
    found_pools = []
    for data in datas:
        if not data["failed"]:
            pools = data["data"]
            found_pools.extend(pools)

    df = pd.DataFrame(found_pools)
    attrs = pd.json_normalize(df["attributes"]).add_prefix("attr_")
    rels = pd.json_normalize(df["relationships"]).add_prefix("rel_")
    flat_df = pd.concat(
        [df.drop(columns=["attributes", "relationships"]), attrs, rels],
        axis=1,
    )

    return flat_df


if __name__ == "__main__":

    from mainnet_launch.constants import ALL_CHAINS, DOLA, USDC, WETH

    print("Prices")
    for chain in ALL_CHAINS:
        token_addresses = [USDC(chain), WETH(chain), DOLA(chain)]
        price_df = fetch_token_prices_from_coingecko(chain, token_addresses)
        print(chain.name)
        print(price_df.shape)

    print("Found Pools")
    for chain in ALL_CHAINS:
        token_addresses = [USDC(chain), WETH(chain), DOLA(chain)]
        pool_df = fetch_many_pairs_from_coingecko(chain, token_addresses)
        print(chain.name)
        print(pool_df.shape)
        pass

        #  eth
        # (433, 58)
        # base
        # (462, 58)
        # sonic
        # (392, 58)
