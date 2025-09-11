import pandas as pd
from web3 import Web3

from mainnet_launch.abis import AUTOPOOL_VAULT_ABI
from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function
from mainnet_launch.database.schema.full import AutopoolWithdrawal
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache
from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)

# TODO you need to add the navPerShare() at each withdraw event. TO get the user's slippage


def get_highest_already_fetched_autopool_withdrawal_block() -> dict[str, int]:
    """
    For each autopool vault, find the max transactions.block for rows already in autopool_withdrawals.
    Default to the deploy block for pools with no rows yet. Start from next block for pools with rows.
    """
    query = """
        SELECT
            aw.autopool_vault_address,
            MAX(tx.block) AS max_block
        FROM autopool_withdrawals AS aw
        JOIN transactions AS tx
          ON tx.tx_hash = aw.tx_hash
        GROUP BY aw.autopool_vault_address;
    """
    df = _exec_sql_and_cache(query)
    highest = df.set_index("autopool_vault_address")["max_block"].to_dict() if not df.empty else {}

    for ap in ALL_AUTOPOOLS:
        if ap.autopool_eth_addr not in highest:
            highest[ap.autopool_eth_addr] = ap.block_deployed
        else:
            highest[ap.autopool_eth_addr] += 1  # resume from next block

    return highest


def _fetch_all_autopool_withdrawal_events() -> pd.DataFrame:
    """
    Fetch new Withdraw events for every autopool, starting from the last seen block (or deploy).
    Returns a concatenated DataFrame across pools (or empty df if nothing new).
    """
    highest_by_pool = get_highest_already_fetched_autopool_withdrawal_block()
    out: list[pd.DataFrame] = []

    for ap in ALL_AUTOPOOLS:
        contract = ap.chain.client.eth.contract(
            address=ap.autopool_eth_addr,
            abi=AUTOPOOL_VAULT_ABI,
        )

        # Use the Withdraw event
        df = fetch_events(
            contract.events.Withdraw,
            chain=ap.chain,
            start_block=highest_by_pool[ap.autopool_eth_addr],
        )

        if df.empty:
            continue

        df["assets"] = df["assets"].apply(lambda x: int(x) / (10**ap.base_asset_decimals))
        df["shares"] = df["shares"].apply(lambda x: int(x) / 1e18)

        # Stamp autopool / chain metadata
        df["autopool_vault_address"] = ap.autopool_eth_addr
        df["chain_id"] = ap.chain.chain_id

        df["sender"] = df["sender"].apply(lambda x: Web3.toChecksumAddress(x))
        df["receiver"] = df["receiver"].apply(lambda x: Web3.toChecksumAddress(x))
        df["owner"] = df["owner"].apply(lambda x: Web3.toChecksumAddress(x))

        print(f"Fetched {len(df)} new Autopool withdrawals for {ap.name} on {ap.chain.name}")
        out.append(df)

    if not out:
        print("no new withdrawal events, early exit")
        return pd.DataFrame()

    return pd.concat(out, ignore_index=True)


def ensure_autopool_withdraws_are_current():
    all_wd_df = _fetch_all_autopool_withdrawal_events()
    if all_wd_df.empty:
        return

    for chain in ALL_CHAINS:
        txs = list(all_wd_df.loc[all_wd_df["chain_id"] == chain.chain_id, "hash"].drop_duplicates())
        if txs:
            ensure_all_transactions_are_saved_in_db(txs, chain)

    new_rows = all_wd_df.apply(
        lambda r: AutopoolWithdrawal(
            autopool_vault_address=r["autopool_vault_address"],
            tx_hash=r["hash"],
            log_index=r["log_index"],
            chain_id=r["chain_id"],
            shares=r["shares"],
            assets=r["assets"],
            sender=r["sender"],
            receiver=r["receiver"],
            owner=r["owner"],
        ),
        axis=1,
    ).to_list()

    insert_avoid_conflicts(new_rows, AutopoolWithdrawal)


if __name__ == "__main__":
    profile_function(ensure_autopool_withdraws_are_current)
