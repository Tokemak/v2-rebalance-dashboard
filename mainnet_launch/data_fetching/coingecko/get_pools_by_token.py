from mainnet_launch.constants import ChainData, COINGECKO_API_KEY, ETH_CHAIN, BASE_CHAIN, SONIC_CHAIN

import requests
from web3 import Web3
import pandas as pd


def _chain_to_coingecko_slug_token_prices(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "ethereum"
    elif chain == BASE_CHAIN:
        return "base"
    elif chain == SONIC_CHAIN:
        return "sonic"


def _chain_to_coingecko_slug_network_id(chain: ChainData) -> str:
    if chain == ETH_CHAIN:
        return "eth"
    elif chain == BASE_CHAIN:
        return "base"  # not tested
    elif chain == SONIC_CHAIN:
        return "sonic"  # not tested


def fetch_token_prices_from_coingecko(
    chain: ChainData,
    token_addresses: list[str],
    vs_currencies: str = "usd",
) -> pd.DataFrame:
    coingecko_coin_slug = _chain_to_coingecko_slug_token_prices(chain)
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

    prices = response.json()
    df = pd.DataFrame.from_dict(prices, orient="index").reset_index()
    df.columns = ["token_address", "usd_price", "last_updated_at"]
    df["token_address"] = df["token_address"].apply(lambda x: Web3.toChecksumAddress(x))
    return df


def _fetch_pool_by_token_from_coingecko(start_token: str, chain: ChainData) -> pd.DataFrame:

    coingecko_network_id_slug = _chain_to_coingecko_slug_network_id(chain)

    url = (
        f"https://pro-api.coingecko.com/api/v3/onchain/networks/{coingecko_network_id_slug}/tokens/{start_token}/pools"
    )
    headers = {"x-cg-pro-api-key": COINGECKO_API_KEY}
    params = {"sort": "h24_volume_usd_liquidity_desc", "include": ["base_token", "quote_token"]}

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        return pd.DataFrame(columns=["quote_token_id", "base_token_id"])

    pools = resp.json()["data"]

    df = pd.DataFrame(pools)
    # def _extract_pool_fields_from_coingecko(pools):
    #     rows = []
    #     for p in pools:
    #         row = {
    #             "address": Web3.toChecksumAddress(p["attributes"]["address"]),
    #             "name": p["attributes"]["name"],
    #             "reserve_in_usd": round(float(p["attributes"]["reserve_in_usd"])),
    #             "base_token_id": p["relationships"]["base_token"]["data"]["id"].split("_")[1],
    #             "quote_token_id": p["relationships"]["quote_token"]["data"]["id"].split("_")[1],
    #             "dex_id": p["relationships"]["dex"]["data"]["id"],
    #         }

    #         rows.append(row)

    #     return pd.DataFrame.from_records(rows)

    # df = _extract_pool_fields_from_coingecko(pools)

    return df


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


def fetch_many_pairs_from_coingecko(tokens_to_check: list[str], chain: ChainData) -> pd.DataFrame:
    """
    Fetch pools for each token on Coingecko and flatten the 'attributes' and
    'relationships' JSON columns into top‑-evel DataFrame columns.
    """
    dfs = []
    failed = []
    for token in set(tokens_to_check):
        df = _fetch_pool_by_token_from_coingecko(token, chain)

        # 1) Normalize 'attributes' dict into its own DataFrame
        attrs = pd.json_normalize(df["attributes"]).add_prefix("attr_")

        # 2) Normalize 'relationships' dict into its own DataFrame
        rels = pd.json_normalize(df["relationships"]).add_prefix("rel_")

        # 3) Drop the original nested‑JSON columns and stitch everything back together
        df = pd.concat([df.drop(columns=["attributes", "relationships"]), attrs, rels], axis=1)

        dfs.append(df)

    # 4) Finally, concat all token‑level DataFrames into one
    return pd.concat(dfs, ignore_index=True), failed


if __name__ == "__main__":
    tokens_to_check = {
        "0x04C154b66CB340F3Ae24111CC767e0184Ed00Cc6",
        "0x0655977FEb2f289A4aB78af67BAB0d17aAb84367",
        "0x15700B564Ca08D9439C58cA5053166E8317aa138",
        "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f",
        "0x57aB1E0003F623289CD798B1824Be09a793e4Bec",
        "0x865377367054516e17014CcdED1e7d814EDC9ce4",
        "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497",
        "0xA0D3707c569ff8C87FA923d3823eC5D81c98Be78",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        "0xA35b1B31Ce002FBF2058D22F30f95D405200A15b",
        "0xBC6DA0FE9aD5f3b0d58160288917AA56653660E9",
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",
        "0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD",
        "0xae78736Cd615f374D3085123A210448E74Fc6393",
        "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",
        "0xb45ad160634c528Cc3D2926d9807104FA3157305",
        "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",
        "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38",
        "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E",
    }

    dex_df = fetch_many_pairs_from_coingecko(tokens_to_check, ETH_CHAIN)
    pass

    # price_df = fetch_token_prices_from_coingecko(ETH_CHAIN, tokens[:2])
    # print(price_df.head())
