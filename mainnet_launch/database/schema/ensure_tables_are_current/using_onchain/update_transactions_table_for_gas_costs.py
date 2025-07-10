import requests
import pandas as pd
import requests
from mainnet_launch.constants import ChainData, ETH_CHAIN

TOKEMAK_ADDRESSES_CONFIG_API_URL = "https://v2-config.tokemaklabs.com/api/systems"


def build_deployers_df(systems: list[dict]) -> pd.DataFrame:
    """One row per deployer (chainId, deployer)."""
    rows = []
    for sys in systems:
        for deployer in sys["deployers"]:
            rows.append({"chain_id": int(sys["chainId"]), "deployer": deployer})
    return pd.DataFrame(rows)


def build_keepers_df(systems: list[dict]) -> pd.DataFrame:
    """One row per Chainlink keeper (chainId + keeper fields)."""
    rows = []
    for sys in systems:
        for keeper in sys["chainlinkKeepers"]:
            keeper_row = {
                "chain_id": int(sys["chainId"]),
                "name": keeper["name"],
                "id": keeper["id"],
                "url": keeper["url"],
                "deprecated": keeper["deprecated"],
            }
            rows.append(keeper_row)
    return pd.DataFrame(rows)


def build_service_accounts_df(systems: list[dict]) -> pd.DataFrame:
    """One row per service account (chainId + account fields)."""
    rows = []
    for sys in systems:
        for acct in sys["serviceAccounts"]:
            acct_row = {
                "chain_id": int(sys["chainId"]),
                "name": acct["name"],
                "address": acct["address"],
                "type": acct["type"],
            }
            rows.append(acct_row)
    return pd.DataFrame(rows)


def fetch_systems_df():
    resp = requests.get(TOKEMAK_ADDRESSES_CONFIG_API_URL)
    resp.raise_for_status()
    systems = resp.json()
    deployers_df = build_deployers_df(systems)
    chainlink_keepers_df = build_keepers_df(systems)
    service_accounts_df = build_service_accounts_df(systems)
    return deployers_df, chainlink_keepers_df, service_accounts_df


# method,
def stub(addresses: str):

    # select from_address, max(block) from transactions, groupby from_address
    # where chain_id == 1
    # and from_address in list_of_my_addresses_to_check
    #

    eoa_to_last_block_with_transaction: dict[str, int] = {"0x1234": 1234}


# deployers_df, chainlink_keepers_df, service_accounts_df = fetch_systems_df()
