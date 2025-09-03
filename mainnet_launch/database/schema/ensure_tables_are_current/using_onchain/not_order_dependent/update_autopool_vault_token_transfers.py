import pandas as pd
from web3 import Web3

from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function, AutopoolConstants
from mainnet_launch.database.schema.full import AutopoolTransfer
from mainnet_launch.database.schema.postgres_operations import _exec_sql_and_cache
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)

from mainnet_launch.abis import AUTOPOOL_VAULT_ABI


def get_highest_already_fetched_autopool_transfer_block() -> dict[str, int]:
    query = """
        WITH autopool_transfers_block AS (
            SELECT
                autopool_transfers.autopool_vault_address,
                transactions.block
            FROM autopool_transfers
            JOIN transactions
              ON autopool_transfers.tx_hash = autopool_transfers.tx_hash
        )
        SELECT
            autopool_vault_address,
            MAX(block) AS max_block
        FROM autopool_transfers_block
        GROUP BY autopool_vault_address;
    """
    df = _exec_sql_and_cache(query)
    highest = df.set_index("autopool_vault_address")["max_block"].to_dict() if not df.empty else {}

    for autopool in ALL_AUTOPOOLS:
        if autopool.autopool_eth_addr not in highest:
            # Default to the deploy block if no rows exist yet
            highest[autopool.autopool_eth_addr] = autopool.block_deployed
    return highest


def ensure_autopool_transfers_are_current():
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

        transfer_df["value"] = transfer_df["value"].apply(lambda x: int(x) / 1e18)  # always 1e18
        transfer_df["autopool_vault_address"] = autopool.autopool_eth_addr
        transfer_df["chain_id"] = autopool.chain.chain_id
        transfer_df["from_address"] = transfer_df["from"].apply(lambda x: Web3.toChecksumAddress(x))
        transfer_df["to_address"] = transfer_df["to"].apply(lambda x: Web3.toChecksumAddress(x))

        if transfer_df.empty:
            continue
        print(f"Fetched {len(transfer_df)} new Autopool transfers for {autopool.name} on {autopool.chain.name}")
        transfer_dfs.append(transfer_df)

    if len(transfer_dfs) == 0:
        return  # early exit, nothing to do

    all_df = pd.concat(transfer_dfs, ignore_index=True)

    for chain in ALL_CHAINS:
        txs = list(all_df.loc[all_df["chain_id"] == chain.chain_id, "hash"].drop_duplicates())
        if txs:
            ensure_all_transactions_are_saved_in_db(txs, chain)

    new_rows = all_df.apply(
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
    profile_function(ensure_autopool_transfers_are_current)
