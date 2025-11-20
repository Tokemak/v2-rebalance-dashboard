from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.constants import *

from multicall import Call
import pandas as pd
from mainnet_launch.data_fetching.get_state_by_block import get_state_by_one_block, identity_with_bool_success
from web3 import Web3

import requests
import pandas as pd

DEFILLAMA_YIELDS_URL = "https://yields.llama.fi/pools"


def fetch_all_defillama_yields() -> pd.DataFrame:
    resp = requests.get(DEFILLAMA_YIELDS_URL, timeout=40, headers={"User-Agent": "defillama-yields-script"})
    resp.raise_for_status()
    payload = resp.json()
    df = pd.DataFrame.from_records(payload["data"])
    df["millions_usd"] = df["tvlUsd"] / 1_000_000
    df["url"] = "https://www.defillama.com/yields/pool/" + df["pool"]
    return df


def fetch_an_autopools_valid_tokens(autopool: AutopoolConstants) -> list[str]:
    call = Call(
        autopool.autopool_eth_addr,
        ["getDestinations()(address[])"],
        [("destinations", identity_with_bool_success)],
    )

    destinations = list(
        get_state_by_one_block([call], autopool.chain.get_block_near_top(), autopool.chain)["destinations"]
    )
    destinations = [Web3.toChecksumAddress(addr) for addr in destinations]
    get_destination_tokens_query = f"""
        SELECT DISTINCT
            dt.token_address,
            t.symbol,
            t.name,
            t.decimals
        FROM destination_tokens AS dt
        JOIN tokens AS t
        ON t.token_address = dt.token_address
        AND t.chain_id     = dt.chain_id
        WHERE dt.destination_vault_address = ANY(ARRAY{destinations}::text[])
    """
    destination_tokens = _exec_sql_and_cache(get_destination_tokens_query)
    return destination_tokens["token_address"].tolist()


def get_valid_rows_for_autopool(
    autopool: AutopoolConstants,
    df: pd.DataFrame,
    tvl_threshold: float = 1_000_000,
    apy_threshold: float = 6.0,
) -> pd.DataFrame:
    autopool_tokens_lower = {str(t).lower() for t in fetch_an_autopools_valid_tokens(autopool)}

    def check_all_tokens_in_underlying(tokens, autopool_tokens_lower, project: str | None = None) -> bool:
        if not isinstance(tokens, list) or len(tokens) == 0:
            return False

        in_set_count = sum(1 for token in tokens if str(token).lower() in autopool_tokens_lower)
        n = len(tokens)

        project_str = (project or "").lower()
        if "aura" in project_str:
            # allow 1 token to be missing: require at least n-1 in the set
            # accounts for composable stable pool tokens (lp token is in the underlying)
            return in_set_count >= n - 1

        # default: require all tokens in the set
        return in_set_count == n

    underlying_tokens_in_autopool = df.apply(
        lambda row: check_all_tokens_in_underlying(row["underlyingTokens"], autopool_tokens_lower, row["project"]),
        axis=1,
    )

    chain_to_defi_llama_yield_chain_name = {
        ETH_CHAIN: "Ethereum",
        BASE_CHAIN: "Base",
        SONIC_CHAIN: "Sonic",
        ARBITRUM_CHAIN: "Arbitrum",
        PLASMA_CHAIN: "Plasma",
        LINEA_CHAIN: "Linea",
    }

    right_chain = df["chain"] == chain_to_defi_llama_yield_chain_name[autopool.chain]

    valid_protocols = [
        "morpho-v1",
        "morpho-v2",
        "morpho-v3",
        "convex-finance",
        "curve-dex",
        "aave-v3",
        "aave-v2",
        "fluid-lending",
        "silo-v2",
        "balancer-v3",
        "balancer-v2",
        "aura",
        "fluid-dex",
    ]

    valid_protocols_mask = df["project"].str.lower().isin(valid_protocols)

    valid_rows = df[
        underlying_tokens_in_autopool
        & right_chain
        & valid_protocols_mask
        & (df["tvlUsd"] >= tvl_threshold)
        & (df["apyMean30d"] >= apy_threshold)
    ].copy()

    valid_rows["autopool_name"] = autopool.name
    return valid_rows


def fetch_possible_new_autopool_destinations(
    tvl_threshold: float = 1_000_000, apy_threshold: float = 1.0
) -> pd.DataFrame:
    df = fetch_all_defillama_yields()

    valid_rows_list = []
    for autopool in ALL_AUTOPOOLS:
        valid_rows = get_valid_rows_for_autopool(autopool, df, tvl_threshold, apy_threshold)
        valid_rows_list.append(valid_rows)

    all_valid_rows_df = pd.concat(valid_rows_list, axis=0).reset_index(drop=True)
    
    return all_valid_rows_df, df


if __name__ == "__main__":
    df = fetch_possible_new_autopool_destinations()
    df
