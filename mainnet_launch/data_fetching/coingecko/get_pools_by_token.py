from mainnet_launch.constants import ChainData, COINGECKO_API_KEY, ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN

import requests
from web3 import Web3
import pandas as pd

def _chain_to_coingecko_slug(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "ethereum"
    elif chain == BASE_CHAIN:
        return  "base"
    elif chain == SONIC_CHAIN:
        return "sonic"


def fetch_token_prices_from_coingecko(
    chain: ChainData,
    token_addresses: list[str],
    vs_currencies: str = 'usd',
) -> pd.DataFrame:
    coingecko_coin_slug = _chain_to_coingecko_slug(chain)
    url = f"https://pro-api.coingecko.com/api/v3/simple/token_price/{coingecko_coin_slug}"

    params = {
        "contract_addresses": ",".join([addr.lower() for addr in token_addresses]),
        "vs_currencies": vs_currencies,
        "include_market_cap": "false",
        "include_24hr_vol": "false",
        "include_24hr_change": "false",
        "include_last_updated_at": "true",
        "precision": "full",
    }

    headers = {"x-cg-pro-api-key": COINGECKO_API_KEY}

    response = requests.get(url, headers=headers, params=params, timeout=15)
    response.raise_for_status()

    prices =  response.json()
    df = pd.DataFrame.from_dict(prices, orient='index').reset_index()
    df.columns = ['token_address', 'usd_price', 'last_updated_at']
    df['token_address'] = df['token_address'].apply(lambda x: Web3.toChecksumAddress(x))
    return df

def _fetch_pool_by_token_from_coingecko(
    start_token: str, chain: ChainData, min_USD_reserves: int = 100_000
) -> pd.DataFrame:
    
    coingecko_coin_slug = _chain_to_coingecko_slug(chain)

    base_url = "https://pro-api.coingecko.com/api/v3"
    url = f"{base_url}/onchain/networks/{coingecko_coin_slug}/tokens/{start_token}/pools"
    headers = {"x-cg-pro-api-key": COINGECKO_API_KEY}
    params = {"sort": "h24_volume_usd_liquidity_desc", "include": ["base_token", "quote_token"]}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    print(resp.text)
    try:

        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
        print(start_token)
        return pd.DataFrame()
    pools = resp.json()["data"]

    def _extract_pool_fields_from_coingecko(pools):
        rows = []
        for p in pools:
            row = {
                "address": Web3.toChecksumAddress(p["attributes"]["address"]),
                "name": p["attributes"]["name"],
                "reserve_in_usd": round(float(p["attributes"]["reserve_in_usd"])),
                "base_token_id": p["relationships"]["base_token"]["data"]["id"].split("_")[1],
                "quote_token_id": p["relationships"]["quote_token"]["data"]["id"].split("_")[1],
                "dex_id": p["relationships"]["dex"]["data"]["id"],
            }

            rows.append(row)

        return pd.DataFrame.from_records(rows)

    df = _extract_pool_fields_from_coingecko(pools)

    return df[df["reserve_in_usd"] >= min_USD_reserves]


def fetch_n_hops_from_tokens_with_coingecko(
    tokens_to_check: set[str], chain: str, min_USD_reserves: int = 1, n_hops: int = 1
) -> pd.DataFrame:
    tokens_to_check = set([Web3.toChecksumAddress(t).lower() for t in tokens_to_check])
    tokens_already_checked = set()

    dex_liqudity_df = None

    for hop_num in range(n_hops):
        tokens_to_check_this_hop = [t for t in tokens_to_check if t not in tokens_already_checked]
        print(f"Checking {len(tokens_to_check_this_hop)} tokens in this hop. {hop_num}")
        for token in tokens_to_check_this_hop:
            if dex_liqudity_df is None:
                dex_liqudity_df = _fetch_pool_by_token_from_coingecko(token, chain, min_USD_reserves=min_USD_reserves)
            else:
                this_token_hops_df = _fetch_pool_by_token_from_coingecko(
                    token, chain, min_USD_reserves=min_USD_reserves
                )
                dex_liqudity_df = pd.concat([dex_liqudity_df, this_token_hops_df], ignore_index=True)

            tokens_already_checked.add(Web3.toChecksumAddress(token))

        new_tokens_to_check = set(
            dex_liqudity_df["quote_token_id"].tolist() + dex_liqudity_df["base_token_id"].tolist()
        )
        new_tokens_to_check = set([Web3.toChecksumAddress(t).lower() for t in new_tokens_to_check])
        tokens_to_check = new_tokens_to_check

    return dex_liqudity_df.drop_duplicates()



if __name__ == "__main__":
    tokens =  ['0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6', '0x0100546F2cD4C9D97f798fFC9755E47865FF7Ee6']


    dex_df = fetch_n_hops_from_tokens_with_coingecko(tokens, ETH_CHAIN, min_USD_reserves=1, n_hops=1)
    pass


    # price_df = fetch_token_prices_from_coingecko(ETH_CHAIN, tokens[:2])
    # print(price_df.head())
