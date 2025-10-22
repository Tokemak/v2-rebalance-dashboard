import requests
import pandas as pd
from time import sleep


from mainnet_launch.constants import ChainData, ETH_CHAIN
from mainnet_launch.database.postgres_operations import simple_agg_by_one_table
from mainnet_launch.database.schema.full import Transactions
from mainnet_launch.data_fetching.etherscan.get_transactions_etherscan import get_all_transactions_sent_by_eoa_address
from web3 import Web3
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)

TOKEMAK_ADDRESSES_CONFIG_API_URL = "https://v2-config.tokemaklabs.com/api/systems"


def _extract_deployers_df(systems: list[dict]) -> pd.DataFrame:
    """One row per deployer (chainId, deployer)."""
    rows = []
    for sys in systems:
        for deployer in sys["deployers"]:
            rows.append({"chain_id": int(sys["chainId"]), "deployer": Web3.toChecksumAddress(deployer)})
    return pd.DataFrame(rows)


def _extract_keepers_df(systems: list[dict]) -> pd.DataFrame:
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


def _extract_service_accounts_df(systems: list[dict]) -> pd.DataFrame:
    """One row per service account (chainId + account fields)."""
    rows = []
    for sys in systems:
        for acct in sys["serviceAccounts"]:
            acct_row = {
                "chain_id": int(sys["chainId"]),
                "name": acct["name"],
                "address": Web3.toChecksumAddress(acct["address"]),
                "type": str(acct["type"]) + " " + str(sys["chainId"]),
            }
            rows.append(acct_row)
    return pd.DataFrame(rows)


def fetch_tokemak_address_constants_dfs():
    resp = requests.get(TOKEMAK_ADDRESSES_CONFIG_API_URL)
    resp.raise_for_status()
    systems = resp.json()
    deployers_df = _extract_deployers_df(systems)
    chainlink_keepers_df = _extract_keepers_df(systems)
    service_accounts_df = _extract_service_accounts_df(systems)
    return deployers_df, chainlink_keepers_df, service_accounts_df


def _get_all_tx_hashes_by_from_address():
    from_address_to_hashes_already_saved = (
        simple_agg_by_one_table(
            table=Transactions,
            target_column=Transactions.tx_hash,
            target_column_alias="unique_tx_hashes",
            group_by_column=Transactions.from_address,
            aggregation_function="array_agg",
            where_clause=Transactions.chain_id == ETH_CHAIN.chain_id,
        )
        .set_index("from_address")["unique_tx_hashes"]
        .to_dict()
    )

    return from_address_to_hashes_already_saved


def _from_address_to_highest_block_already_stored_in_db(chain: ChainData) -> dict:
    highest_block_already_seen = (
        simple_agg_by_one_table(
            table=Transactions,
            target_column=Transactions.block,
            target_column_alias="max_block",
            group_by_column=Transactions.from_address,
            aggregation_function="MAX",
            where_clause=Transactions.chain_id == chain.chain_id,
        )
        .set_index("from_address")["max_block"]
        .to_dict()
    )
    return highest_block_already_seen


def ensure_tokemak_EOA_gas_costs_are_current():
    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()

    for chain in [ETH_CHAIN]:
        highest_block_already_seen = _from_address_to_highest_block_already_stored_in_db(chain)
        EOAs_we_want_to_track = set(
            deployers_df[deployers_df["chain_id"] == chain.chain_id]["deployer"].tolist()
            + service_accounts_df[service_accounts_df["chain_id"] == chain.chain_id]["address"].tolist()
        )
        transaction_hashes_required = []

        for i, EOA_address in enumerate(EOAs_we_want_to_track):
            from_block = highest_block_already_seen.get(EOA_address, 0) + 1
            etherscan_tx_df = get_all_transactions_sent_by_eoa_address(
                chain, EOA_address, from_block=from_block, to_block=chain.get_block_near_top()
            )
            if not etherscan_tx_df.empty:
                transaction_hashes_required.extend(etherscan_tx_df["hash"].tolist())
                # print(f"{len(transaction_hashes_required)=} {i=} {len(EOAs_we_want_to_track)=} {EOA_address=}")
        ensure_all_transactions_are_saved_in_db(transaction_hashes_required, chain)


def update_tokemak_EOA_gas_costs_from_0():
    """Be certain to get all transactions from 0 to the current block for all deployers and service accounts"""

    deployers_df, chainlink_keepers_df, service_accounts_df = fetch_tokemak_address_constants_dfs()

    for chain in [ETH_CHAIN]:
        EOAs_we_want_to_track = set(
            deployers_df[deployers_df["chain_id"] == chain.chain_id]["deployer"].tolist()
            + service_accounts_df[service_accounts_df["chain_id"] == chain.chain_id]["address"].tolist()
        )

        transaction_hashes_required = []

        for i, EOA_address in enumerate(EOAs_we_want_to_track):
            # this should have a rate limiter of no more than 4/ second
            etherscan_tx_df = get_all_transactions_sent_by_eoa_address(
                chain, EOA_address, from_block=0, to_block=chain.get_block_near_top()
            )
            transaction_hashes_required.extend(etherscan_tx_df["hash"].tolist())

        ensure_all_transactions_are_saved_in_db(transaction_hashes_required, chain)


if __name__ == "__main__":

    ensure_tokemak_EOA_gas_costs_are_current()

    from mainnet_launch.constants import profile_function

    # profile_function(ensure_tokemak_EOA_gas_costs_are_current)
    # not sure why this sometimes fails with this error

    #     etherscan_tx_df = get_all_transactions_sent_by_eoa_address(
    #   File "/home/runner/work/v2-rebalance-dashboard/v2-rebalance-dashboard/mainnet_launch/data_fetching/etherscan/get_transactions_etherscan.py", line 78, in get_all_transactions_sent_by_eoa_address
    #     df["from"] = df["from"].apply(lambda x: chain.client.toChecksumAddress(x))
    #   File "/home/runner/work/v2-rebalance-dashboard/v2-rebalance-dashboard/.venv/lib/python3.10/site-packages/pandas/core/frame.py", line 4107, in __getitem__
    #     indexer = self.columns.get_loc(key)
    #   File "/home/runner/work/v2-rebalance-dashboard/v2-rebalance-dashboard/.venv/lib/python3.10/site-packages/pandas/core/indexes/range.py", line 417, in get_loc
    #     raise KeyError(key)
    # KeyError: 'from'
