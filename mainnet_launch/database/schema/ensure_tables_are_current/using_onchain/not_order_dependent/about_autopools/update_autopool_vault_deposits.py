import pandas as pd
from web3 import Web3

from mainnet_launch.abis import AUTOPOOL_VAULT_ABI
from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function
from mainnet_launch.database.schema.full import AutopoolDeposit
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)


def get_highest_already_fetched_autopool_deposit_block() -> dict[str, int]:
    """
    For each autopool vault, find the max transactions.block for rows already in autopool_deposits.
    Default to the deploy block for pools with no rows yet.
    """
    query = """
        SELECT
            ad.autopool_vault_address,
            MAX(tx.block) AS max_block
        FROM autopool_deposits AS ad
        JOIN transactions AS tx
          ON tx.tx_hash = ad.tx_hash
        GROUP BY ad.autopool_vault_address;
    """
    df = _exec_sql_and_cache(query)
    highest_block_by_pool = df.set_index("autopool_vault_address")["max_block"].to_dict() if not df.empty else {}

    # Fill missing pools with their deploy block
    for autopool in ALL_AUTOPOOLS:
        if autopool.autopool_eth_addr not in highest_block_by_pool:
            highest_block_by_pool[autopool.autopool_eth_addr] = autopool.block_deployed
        else:
            # Start from the next block after the last seen
            highest_block_by_pool[autopool.autopool_eth_addr] += 1

    return highest_block_by_pool


def _fetch_all_autopool_deposit_events() -> pd.DataFrame:
    """
    Fetch new Deposit events for every autopool, starting from the last seen block (or deploy).
    Returns a concatenated DataFrame across pools (or empty df if nothing new).
    """
    highest_block_by_pool = get_highest_already_fetched_autopool_deposit_block()

    deposit_dfs: list[pd.DataFrame] = []

    for autopool in ALL_AUTOPOOLS:
        contract = autopool.chain.client.eth.contract(
            address=autopool.autopool_eth_addr,
            abi=AUTOPOOL_VAULT_ABI,
        )

        # Important: use the Deposit event, not Transfer
        deposit_df = fetch_events(
            contract.events.Deposit,
            chain=autopool.chain,
            start_block=highest_block_by_pool[autopool.autopool_eth_addr],
        )

        if deposit_df.empty:
            continue

        deposit_df["assets"] = deposit_df["assets"].apply(lambda x: int(x) / (10**autopool.base_asset_decimals))
        deposit_df["shares"] = deposit_df["shares"].apply(lambda x: int(x) / 1e18)

        deposit_df["autopool_vault_address"] = autopool.autopool_eth_addr
        deposit_df["chain_id"] = autopool.chain.chain_id

        deposit_df["sender"] = deposit_df["sender"].apply(lambda x: Web3.toChecksumAddress(x))
        deposit_df["owner"] = deposit_df["owner"].apply(lambda x: Web3.toChecksumAddress(x))

        print(f"Fetched {len(deposit_df)} new Autopool deposits for {autopool.name} on {autopool.chain.name}")
        deposit_dfs.append(deposit_df)

    if not deposit_dfs:
        print("no new deposit events, early exit")
        return pd.DataFrame()

    all_deposits_df = pd.concat(deposit_dfs, ignore_index=True)
    return all_deposits_df


def ensure_autopool_deposits_are_current():
    """
    Orchestrates:
    - fetch Deposit events
    - ensure their transactions exist
    - bulk upsert rows into autopool_deposits (conflicts avoided)
    """
    all_deposits_df = _fetch_all_autopool_deposit_events()
    if all_deposits_df.empty:
        return

    for chain in ALL_CHAINS:
        txs = list(all_deposits_df.loc[all_deposits_df["chain_id"] == chain.chain_id, "hash"].drop_duplicates())
        if txs:
            ensure_all_transactions_are_saved_in_db(txs, chain)

    # Build ORM rows (fail fast on any missing col)
    new_rows = all_deposits_df.apply(
        lambda r: AutopoolDeposit(
            autopool_vault_address=r["autopool_vault_address"],
            tx_hash=r["hash"],
            log_index=r["log_index"],
            chain_id=r["chain_id"],
            shares=r["shares"],
            assets=r["assets"],
            sender=r["sender"],
            owner=r["owner"],
        ),
        axis=1,
    ).to_list()

    insert_avoid_conflicts(new_rows, AutopoolDeposit)


if __name__ == "__main__":
    profile_function(ensure_autopool_deposits_are_current)
