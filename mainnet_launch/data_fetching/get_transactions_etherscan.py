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


def _get_outgoing_transactions(
    chain: ChainData,
    address: str,
    from_block: int,
    to_block: int,
    page: int,
    offset: int = 1000,
) -> tuple[list[dict], bool]:
    """
    # I think this is to or from, not just from
    Fetches up to `offset` internal txns sent *from* `address` between
    `from_block` and `to_block` on chain via Etherscan.
    Returns (tx_list, has_more) where `has_more` is True if we got a full page.
    """
    params = {
        "module": "account",
        "action": "txlist",
        "chainid": chain.chain_id,
        "address": address,
        "startblock": from_block,
        "endblock": to_block,
        "page": page,
        "offset": offset,
        "sort": "asc",
        "apikey": os.getenv("ETHERSCAN_API_KEY"),
    }

    resp = requests.get(ETHERSCAN_API_URL, params=params)
    resp.raise_for_status()
    payload = resp.json()

    txs = payload.get("result", [])
    # if we received exactly `offset` entries, there *may* be more on next page
    has_more = len(txs) == offset
    return txs, has_more


def get_all_transactions_sent_by_eoa_address(
    chain: ChainData,
    EOA_address: str,
    from_block: int,
    to_block: int,
) -> pd.DataFrame:
    """Use pagination to get *all* internal txns sent by `EOA_address`"""
    all_txs: list[dict] = []
    page = 1

    while True:
        txs, has_more = _get_outgoing_transactions(
            chain=chain,
            address=EOA_address,
            from_block=from_block,
            to_block=to_block,
            page=page,
            offset=1000,
        )
        if not txs:
            break

        all_txs.extend(txs)
        if not has_more:
            break

        page += 1

    # convert list of dicts into DataFrame
    return pd.DataFrame(all_txs)


if __name__ == "__main__":

    # — example usage —
    mainnet_deployer = "0x123cC4AFA59160C6328C0152cf333343F510e5A3"

    from mainnet_launch.constants import ETH_CHAIN

    tx_df = get_all_transactions_sent_by_eoa_address(
        ETH_CHAIN, mainnet_deployer, from_block=20638356 - 100, to_block=22884434
    )
    print(tx_df.columns)

    print(tx_df.head())
