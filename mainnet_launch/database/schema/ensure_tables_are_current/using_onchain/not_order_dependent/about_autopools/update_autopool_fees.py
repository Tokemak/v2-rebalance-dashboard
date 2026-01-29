# does not depend on any other tables being current
# this can be ran in parellel to other update functions
# 20 seconds from 0,
# 12 seconds with no updates


import pandas as pd

from mainnet_launch.constants import ALL_AUTOPOOLS, ALL_CHAINS, profile_function
from mainnet_launch.abis import AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI
from mainnet_launch.database.schema.full import AutopoolFees

from mainnet_launch.data_fetching.alchemy.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
    insert_avoid_conflicts,
)

from mainnet_launch.database.schema.track_last_processed_block_helper import (
    get_last_processed_block_for_table,
    write_last_processed_block,
)


def ensure_autopool_fees_are_current():
    chain_to_start_block = get_last_processed_block_for_table(AutopoolFees)

    for chain in ALL_CHAINS:
        addresess = [a.autopool_eth_addr for a in ALL_AUTOPOOLS if a.chain == chain]
        contract = chain.client.eth.contract(address=addresess[0], abi=AUTOPOOL_VAULT_WITH_FEE_COLLECTED_EVENT_ABI)
        top_block = chain.get_block_near_top()
        fee_collected_events_df = fetch_events(
            contract.events.FeeCollected,
            chain=chain,
            start_block=chain_to_start_block[chain] + 1 ,
            end_block=top_block,
            addresses=addresess,
        )

        periodic_fee_collected_events_df = fetch_events(
            contract.events.PeriodicFeeCollected,
            chain=chain,
            start_block=chain_to_start_block[chain] + 1,
            end_block=top_block,
            addresses=addresess,
        )

        fee_df = pd.concat([fee_collected_events_df, periodic_fee_collected_events_df], ignore_index=True)
        if fee_df.empty:
            print(
                f"No new autopool fee events for autopools on {chain.name} after block {chain_to_start_block[chain]:,}"
            )
            continue

        fee_df["mintedShares"] = fee_df["mintedShares"].apply(lambda x: int(x) / 1e18)
        fee_df["chain_id"] = chain.chain_id
        fee_df["autopool_vault_address"] = fee_df["address"]

        ensure_all_transactions_are_saved_in_db(
            fee_df["hash"].tolist(),
            chain,
        )

        new_fee_rows = fee_df.apply(
            lambda row: AutopoolFees(
                tx_hash=row["hash"],
                log_index=row["log_index"],
                chain_id=row["chain_id"],
                autopool_vault_address=row["autopool_vault_address"],
                fee_name=row["event"],
                fee_sink=row["feeSink"],
                minted_shares=row["mintedShares"],
            ),
            axis=1,
        ).to_list()

        insert_avoid_conflicts(new_fee_rows, AutopoolFees)
        print(f"Inserted {len(new_fee_rows):,} new autopool fee events for autopools on {chain.name}")

        write_last_processed_block(
            chain,
            top_block,
            AutopoolFees,
        )


if __name__ == "__main__":

    # ensure_autopool_fees_are_current()
    profile_function(ensure_autopool_fees_are_current)
    # ensure_autopool_fees_are_current()
