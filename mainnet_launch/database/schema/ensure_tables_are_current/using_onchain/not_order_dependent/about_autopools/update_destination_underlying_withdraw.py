"""Assumes that all the destinations are in Destinations Table"""

# duplicates *much* code from mainnet_launch/database/schema/ensure_tables_are_current/using_onchain/update_destination_underlying_deposited.py


import pandas as pd

from mainnet_launch.abis import BALANCER_AURA_DESTINATION_VAULT_ABI
from mainnet_launch.constants import ChainData, ALL_CHAINS

from mainnet_launch.database.schema.full import Destinations, DestinationUnderlyingWithdraw, Transactions, ENGINE
from mainnet_launch.database.schema.postgres_operations import (
    get_full_table_as_df,
    merge_tables_as_df,
    TableSelector,
    insert_avoid_conflicts,
)

from mainnet_launch.data_fetching.get_events import fetch_many_events, FetchEventParams

from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)


def _get_highest_block_already_fetched_for_destination_underlying_withdraw(chain: ChainData) -> dict[str, int]:
    prior_destination_underlying_withdraw_df = merge_tables_as_df(
        [
            TableSelector(
                DestinationUnderlyingWithdraw,
                select_fields=[
                    DestinationUnderlyingWithdraw.destination_vault_address,
                    DestinationUnderlyingWithdraw.tx_hash,
                ],
            ),
            TableSelector(
                Transactions,
                select_fields=[Transactions.block],
                join_on=(DestinationUnderlyingWithdraw.tx_hash == Transactions.tx_hash),
            ),
        ],
        where_clause=Transactions.chain_id == chain.chain_id,
    )

    destination_to_highest_block = (
        prior_destination_underlying_withdraw_df.groupby("destination_vault_address")["block"]
        .max()
        .astype(int)
        .to_dict()
    )
    return destination_to_highest_block


def _insert_new_rows_into_destination_underlying_withdraw(
    chain: ChainData, all_underlying_withdraw_events_df: pd.DataFrame
) -> None:

    if all_underlying_withdraw_events_df.empty:
        return

    new_transaction_hashes = all_underlying_withdraw_events_df["hash"].unique().tolist()
    ensure_all_transactions_are_saved_in_db(new_transaction_hashes, chain)

    def _underlying_withdraw_event_row_to_record_for_database(row: pd.Series) -> DestinationUnderlyingWithdraw:
        return DestinationUnderlyingWithdraw(
            tx_hash=row["hash"],
            destination_vault_address=row["destination_vault_address"],
            amount=str(row["amount"]),
            to_address=row["to"],
            owner=row["owner"],
        )

    new_destination_underlying_withdraw_rows = all_underlying_withdraw_events_df.apply(
        _underlying_withdraw_event_row_to_record_for_database, axis=1
    ).tolist()

    insert_avoid_conflicts(
        new_destination_underlying_withdraw_rows,
        DestinationUnderlyingWithdraw,
    )


def fetch_new_destination_underlying_withdraw_events(
    chain: ChainData,
    destination_to_highest_block: dict[str, int],
    num_threads: int = 16,  # not certain how this number, 16 seems fine
) -> pd.DataFrame:
    """
    Fetch UnderlyingWithdraw events for many destination vaults concurrently.
    Returns a single DataFrame with destination_vault_address annotated.
    """
    # small safety margin behind the tip

    # Build the event fetch plan
    plans: list[FetchEventParams] = []
    for destination_vault_address, highest_block_already_fetched in destination_to_highest_block.items():
        contract = chain.client.eth.contract(
            address=destination_vault_address,
            abi=BALANCER_AURA_DESTINATION_VAULT_ABI,
        )

        plans.append(
            FetchEventParams(
                event=contract.events.UnderlyingWithdraw,
                chain=chain,
                id=destination_vault_address,
                start_block=highest_block_already_fetched + 1,
                end_block=chain.get_block_near_top(),
            )
        )

    if not plans:
        return pd.DataFrame()

    # Fetch concurrently
    results: dict[str, pd.DataFrame] = fetch_many_events(plans, num_threads=num_threads)

    # Tag each result with its destination address and combine
    dfs: list[pd.DataFrame] = []
    for destination_vault_address, df in results.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        df["destination_vault_address"] = destination_vault_address
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# 8.3 seconds when empty
def ensure_destination_underlying_withdraw_are_current() -> None:
    for chain in ALL_CHAINS:
        destinations_df = get_full_table_as_df(Destinations, where_clause=Destinations.chain_id == chain.chain_id)
        destination_to_highest_block = _get_highest_block_already_fetched_for_destination_underlying_withdraw(chain)
        for destination_vault_address in destinations_df["destination_vault_address"].unique():
            if destination_vault_address not in destination_to_highest_block:
                destination_to_highest_block[destination_vault_address] = chain.block_autopool_first_deployed

        all_underlying_withdraw_events_df = fetch_new_destination_underlying_withdraw_events(
            chain,
            destination_to_highest_block,
            num_threads=16,
        )

        _insert_new_rows_into_destination_underlying_withdraw(chain, all_underlying_withdraw_events_df)


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(ensure_destination_underlying_withdraw_are_current)
