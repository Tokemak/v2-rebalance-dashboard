"""Returns information about transactions sent from an address"""

import os
import requests
import pandas as pd
from mainnet_launch.constants import ChainData
from mainnet_launch.database.schema.full import Transactions

# For Etherscan you need to use the v2 endpoint
# use
# ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

# not
# ETHERSCAN_API_URL = "https://api.etherscan.io/api"
# the old API silently misbehaves

ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")


def _fetch_pages(chain: ChainData, address: str, start: int, end: int, offset: int = 1000):
    """
    Fetch pages 1–10 for [start…end]. Returns (tx_list, hit_limit).
    hit_limit==True if page 10 returned a full batch (i.e. you may have more).
    """
    txs = []
    for page in range(1, 11):
        params = {
            "module": "account",
            "action": "txlist",
            "chainid": chain.chain_id,
            "address": address,
            "startblock": start,
            "endblock": end,
            "page": page,
            "offset": offset,
            "sort": "asc",
            "apikey": ETHERSCAN_API_KEY,
        }
        resp = requests.get(ETHERSCAN_API_URL, params=params)
        resp.raise_for_status()
        batch = resp.json().get("result", [])
        if not batch:
            return txs, False
        txs.extend(batch)
        # if we got fewer than `offset`, we know there's no more in this window
        if len(batch) < offset:
            return txs, False
    # if we made it through 10 full pages, we hit the 10 000 record cap
    return txs, True


def _get_normal_transactions_from_etherscan_recursive(
    chain: ChainData, address: str, start: int, end: int
) -> list[dict]:
    """
    Recursively page through [start…end]. If you hit the 10 000‐-ecord cap,
    advance start to the highest block seen +1 and recurse.
    """
    all_txs, hit_limit = _fetch_pages(chain, address, start, end)
    if not hit_limit:
        return all_txs

    # We fetched 10 full pages => there are more transactions in [start…end]
    max_block = max(int(tx["blockNumber"]) for tx in all_txs)
    # Recurse from just past the highest block
    return all_txs + _get_normal_transactions_from_etherscan_recursive(chain, address, max_block + 1, end)


def get_all_transactions_sent_by_eoa_address(
    chain: ChainData,
    EOA_address: str,
    from_block: int,
    to_block: int,
) -> pd.DataFrame:
    """Use pagination to get *all* internal txns sent by `EOA_address` from Etherscan,
    note, no concurrency, or rate limiting. Make sure to add it later"""

    all_txs = _get_normal_transactions_from_etherscan_recursive(chain, EOA_address, from_block, to_block)
    df = pd.DataFrame.from_records(all_txs)
    if df.empty:
        return df
    # we only care about transactions sent by the EOA address
    # the etherscan endpoint returns all normal transactions where the EOA is in the `to` or `from` field
    df["from"] = df["from"].apply(lambda x: chain.client.toChecksumAddress(x))
    df = df[df["from"] == chain.client.toChecksumAddress(EOA_address)].copy()
    return df


if __name__ == "__main__":

    # — example usage —
    mainnet_deployer = "0x123cC4AFA59160C6328C0152cf333343F510e5A3"

    from mainnet_launch.constants import ETH_CHAIN

    tx_df = get_all_transactions_sent_by_eoa_address(
        ETH_CHAIN, mainnet_deployer, from_block=20638356 - 100, to_block=22884434
    )
    print(tx_df.columns)

    print(tx_df.head())
