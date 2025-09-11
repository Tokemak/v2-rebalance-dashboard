# does not depend on any other tables being current
# this can be ran in parellel to other update functions
# 20 seconds from 0,
# 12 seconds with no updates
from __future__ import annotations



import pandas as pd

from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function
from mainnet_launch.abis import AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI
from mainnet_launch.database.schema.full import AutopoolFees
from mainnet_launch.database.postgres_operations import _exec_sql_and_cache

from mainnet_launch.data_fetching.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)


def get_highest_already_fetched_autopool_fees_block() -> dict[str, int]:
    query = """
        SELECT
            af.autopool_vault_address,
            MAX(tx.block) AS max_block
        FROM autopool_fees AS af
        JOIN transactions AS tx
          ON tx.tx_hash = af.tx_hash
        GROUP BY af.autopool_vault_address;
    """

    df = _exec_sql_and_cache(query)
    highest_block_already_fetched = df.set_index("autopool_vault_address")["max_block"].to_dict()
    for autopool in ALL_AUTOPOOLS:
        if autopool.autopool_eth_addr not in highest_block_already_fetched:
            # set default block to when it was deployed
            highest_block_already_fetched[autopool.autopool_eth_addr] = autopool.block_deployed
        else:
            highest_block_already_fetched[autopool.autopool_eth_addr] += 1

    return highest_block_already_fetched


def ensure_autopool_fees_are_current():
    highest_block_already_fetched = get_highest_already_fetched_autopool_fees_block()

    new_fee_events = []
    for autopool in ALL_AUTOPOOLS:
        contract = autopool.chain.client.eth.contract(
            address=autopool.autopool_eth_addr, abi=AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI
        )
        fee_collected_events_df = fetch_events(
            contract.events.FeeCollected,
            chain=autopool.chain,
            start_block=highest_block_already_fetched[autopool.autopool_eth_addr] + 1,
        )

        periodic_fee_collected_events_df = fetch_events(
            contract.events.PeriodicFeeCollected,
            chain=autopool.chain,
            start_block=highest_block_already_fetched[autopool.autopool_eth_addr] + 1,
        )
        # cols = ["hash", "log_index", "event", "feeSink", "mintedShares"]

        # df = pd.concat([fee_collected_events_df[cols], periodic_fee_collected_events_df[cols]], ignore_index=True)

        df = pd.concat([fee_collected_events_df, periodic_fee_collected_events_df], ignore_index=True)
        if df.empty:
            continue

        df["mintedShares"] = df["mintedShares"].apply(lambda x: int(x) / 1e18)
        df["chain_id"] = autopool.chain.chain_id
        df["autopool_vault_address"] = autopool.autopool_eth_addr
        new_fee_events.append(df)

    if len(new_fee_events) == 0:
        # early exit if no new fee events
        return

    fee_df = pd.concat(new_fee_events, ignore_index=True)

    for chain in ALL_CHAINS:
        new_transactions = list(fee_df.loc[fee_df["chain_id"] == chain.chain_id, "hash"].drop_duplicates())
        if len(new_transactions) > 0:
            ensure_all_transactions_are_saved_in_db(
                new_transactions,
                chain,
            )

    new_fee_rows = fee_df.apply(
        lambda row: AutopoolFees(
            tx_hash=row["hash"],
            log_index=row["log_index"],
            autopool_vault_address=row["autopool_vault_address"],
            fee_name=row["event"],
            fee_sink=row["feeSink"],
            minted_shares=row["mintedShares"],
        ),
        axis=1,
    ).to_list()

    insert_avoid_conflicts(new_fee_rows, AutopoolFees)


if __name__ == "__main__":

    # ensure_autopool_fees_are_current()
    profile_function(ensure_autopool_fees_are_current)
