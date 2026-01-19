"""Assumes that all the destinations are in Destinations Table"""

import pandas as pd

from mainnet_launch.abis import BALANCER_AURA_DESTINATION_VAULT_ABI
from mainnet_launch.constants import ChainData, ALL_CHAINS, PLASMA_CHAIN

from mainnet_launch.database.schema.full import Destinations, DestinationUnderlyingDeposited, Transactions
from mainnet_launch.database.postgres_operations import (
    get_full_table_as_df,
    merge_tables_as_df,
    TableSelector,
    insert_avoid_conflicts,
)

from mainnet_launch.data_fetching.alchemy.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)


def _get_highest_block_already_fetched_for_destination_underlying_deposited(chain: ChainData) -> dict[str, int]:
    prior_destination_underlying_deposited_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationUnderlyingDeposited,
                select_fields=[
                    DestinationUnderlyingDeposited.destination_vault_address,
                    DestinationUnderlyingDeposited.tx_hash,
                ],
            ),
            TableSelector(
                Transactions,
                select_fields=[Transactions.block],
                join_on=(DestinationUnderlyingDeposited.tx_hash == Transactions.tx_hash),
            ),
        ],
        where_clause=Transactions.chain_id == chain.chain_id,
    )

    destination_to_highest_block = (
        prior_destination_underlying_deposited_df.groupby("destination_vault_address")["block"]
        .max()
        .astype(int)
        .to_dict()
    )
    return destination_to_highest_block



def fetch_new_destination_underlying_deposited_events(
    chain: ChainData,
    destination_to_highest_block: dict[str, int],
) -> pd.DataFrame:
    start_block_to_addresses = {}
    chain_top = chain.get_block_near_top()

    for destination_vault_address, highest_block_already_fetched in destination_to_highest_block.items():
        start_block = int(highest_block_already_fetched) + 1
        if start_block > chain_top:
            continue

        if start_block not in start_block_to_addresses:
            start_block_to_addresses[start_block] = []
        start_block_to_addresses[start_block].append(destination_vault_address)

    if not start_block_to_addresses:
        return pd.DataFrame()

    sample_address = next(iter(destination_to_highest_block))
    sample_contract = chain.client.eth.contract(
        address=sample_address,
        abi=BALANCER_AURA_DESTINATION_VAULT_ABI,
    )
    event = sample_contract.events.UnderlyingDeposited

    dfs = []
    for start_block, addresses in start_block_to_addresses.items():
        df = fetch_events(
            event=event,
            chain=chain,
            start_block=start_block,
            end_block=chain_top,
            addresses=addresses,
        )
        if df is None or df.empty:
            continue

        df = df.copy()
        df["destination_vault_address"] = df["address"]
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def _insert_new_rows_into_destination_underlying_deposited(
    chain: ChainData, all_underlying_deposited_events_df: pd.DataFrame
) -> None:
    if all_underlying_deposited_events_df.empty:
        return

    new_transaction_hashes = all_underlying_deposited_events_df["hash"].unique().tolist()
    ensure_all_transactions_are_saved_in_db(new_transaction_hashes, chain)

    def _underlying_deposited_event_row_to_record_for_database(row: pd.Series) -> DestinationUnderlyingDeposited:
        return DestinationUnderlyingDeposited(
            tx_hash=row["hash"],
            destination_vault_address=row["destination_vault_address"],
            amount=str(row["amount"]),
            sender=row["sender"],
        )

    new_destination_underlying_deposited_rows = all_underlying_deposited_events_df.apply(
        _underlying_deposited_event_row_to_record_for_database, axis=1
    ).tolist()

    insert_avoid_conflicts(
        new_destination_underlying_deposited_rows,
        DestinationUnderlyingDeposited,
    )


def ensure_destination_underlying_deposits_are_current() -> None:
    for chain in ALL_CHAINS:
        destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == chain.chain_id)
        destination_to_highest_block = _get_highest_block_already_fetched_for_destination_underlying_deposited(chain)
        for destination_vault_address in destinations_df["destination_vault_address"].unique():
            if destination_vault_address not in destination_to_highest_block:
                destination_to_highest_block[destination_vault_address] = chain.block_autopool_first_deployed

        all_underlying_deposited_events_df = fetch_new_destination_underlying_deposited_events(
            chain,
            destination_to_highest_block,
        )

        _insert_new_rows_into_destination_underlying_deposited(chain, all_underlying_deposited_events_df)


# at least in theory, if we have an event on chain for a withdrawal 
# we should have everything before it too
# that maybe be an optimization needed later

if __name__ == "__main__":
    from mainnet_launch.constants import profile_function
    ensure_destination_underlying_deposits_are_current()