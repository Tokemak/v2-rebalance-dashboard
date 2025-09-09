import pandas as pd
from web3 import Web3

from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function, AutopoolConstants
from mainnet_launch.database.schema.full import AutopoolTransfer
from mainnet_launch.database.schema.postgres_operations import _exec_sql_and_cache
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)

from mainnet_launch.abis import AUTOPOOL_VAULT_ABI
from mainnet_launch.constants import time_decorator


def get_highest_already_fetched_autopool_transfer_block() -> dict[str, int]:
    query = """
            SELECT at.autopool_vault_address,
                MAX(tx.block) AS max_block
            FROM autopool_transfers AS at
            JOIN transactions AS tx
            ON tx.tx_hash  = at.tx_hash
            GROUP BY at.autopool_vault_address;
    """
    df = _exec_sql_and_cache(query)
    highest_block_by_pool = df.set_index("autopool_vault_address")["max_block"].to_dict() if not df.empty else {}

    for autopool in ALL_AUTOPOOLS:
        if autopool.autopool_eth_addr not in highest_block_by_pool:
            # Default to the deploy block if no rows exist yet
            highest_block_by_pool[autopool.autopool_eth_addr] = autopool.block_deployed
        else:
            # Start from the next block after the highest already fetched
            highest_block_by_pool[autopool.autopool_eth_addr] += 1
    return highest_block_by_pool


def _fetch_all_autopool_transfer_events() -> pd.DataFrame:
    highest_block_by_pool = get_highest_already_fetched_autopool_transfer_block()

    transfer_dfs: list[pd.DataFrame] = []

    for autopool in ALL_AUTOPOOLS:
        contract = autopool.chain.client.eth.contract(
            address=autopool.autopool_eth_addr,
            abi=AUTOPOOL_VAULT_ABI,
        )

        transfer_df = fetch_events(
            contract.events.Transfer,
            chain=autopool.chain,
            start_block=highest_block_by_pool[autopool.autopool_eth_addr],
        )

        if transfer_df.empty:
            continue

        transfer_df["value"] = transfer_df["value"].apply(lambda x: int(x) / 1e18)  # always 1e18
        transfer_df["autopool_vault_address"] = autopool.autopool_eth_addr
        transfer_df["chain_id"] = autopool.chain.chain_id
        transfer_df["from_address"] = transfer_df["from"].apply(lambda x: Web3.toChecksumAddress(x))
        transfer_df["to_address"] = transfer_df["to"].apply(lambda x: Web3.toChecksumAddress(x))

        print(f"Fetched {len(transfer_df)} new Autopool transfers for {autopool.name} on {autopool.chain.name}")
        transfer_dfs.append(transfer_df)

    if len(transfer_dfs) == 0:
        print("no new transfer events, early exit")
        return pd.DataFrame()  # early exit, nothing to do

    all_transfers_df = pd.concat(transfer_dfs, ignore_index=True)
    return all_transfers_df


def ensure_autopool_transfers_are_current():
    all_transfers_df = _fetch_all_autopool_transfer_events()
    if all_transfers_df.empty:
        return

    for chain in ALL_CHAINS:
        txs = list(all_transfers_df.loc[all_transfers_df["chain_id"] == chain.chain_id, "hash"].drop_duplicates())
        if txs:
            ensure_all_transactions_are_saved_in_db(txs, chain)

    new_rows = all_transfers_df.apply(
        lambda r: AutopoolTransfer(
            tx_hash=r["hash"],
            log_index=r["log_index"],
            chain_id=r["chain_id"],
            autopool_vault_address=r["autopool_vault_address"],
            from_address=r["from_address"],
            to_address=r["to_address"],
            value=r["value"],
        ),
        axis=1,
    ).to_list()

    insert_avoid_conflicts(new_rows, AutopoolTransfer)


if __name__ == "__main__":
    # 8 seconds on after current
    profile_function(ensure_autopool_transfers_are_current)
