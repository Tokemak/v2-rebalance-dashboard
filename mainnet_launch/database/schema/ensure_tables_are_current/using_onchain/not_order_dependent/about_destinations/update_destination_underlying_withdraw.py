import pandas as pd

from mainnet_launch.abis import BALANCER_AURA_DESTINATION_VAULT_ABI
from mainnet_launch.constants import ChainData, ALL_CHAINS

from mainnet_launch.database.schema.full import Destinations, DestinationUnderlyingWithdraw
from mainnet_launch.database.postgres_operations import (
    _exec_sql_and_cache,
    insert_avoid_conflicts,
    get_full_table_as_df,
)

from mainnet_launch.data_fetching.alchemy.get_events import fetch_events
from mainnet_launch.database.schema.ensure_tables_are_current.using_onchain.helpers.update_transactions import (
    ensure_all_transactions_are_saved_in_db,
)


def _get_highest_block_already_fetched_by_chain_id() -> dict[int, int]:
    query = """
        SELECT
            duw.chain_id,
            MAX(t.block) + 1 AS max_block
        FROM destination_underlying_withdraw duw
        JOIN transactions t
            ON t.tx_hash = duw.tx_hash
        AND t.chain_id = duw.chain_id
        GROUP BY
            duw.chain_id
    """
    df = _exec_sql_and_cache(query)
    highest = {} if df is None or df.empty else df.set_index("chain_id")["max_block"].to_dict()

    for chain in ALL_CHAINS:
        if chain.chain_id not in highest or highest[chain.chain_id] is None:
            highest[chain.chain_id] = chain.block_autopool_first_deployed

    return highest


def fetch_new_underlying_withdraw_events(
    start_block: int,
    chain: ChainData,
    destination_addresses: list[str],
) -> pd.DataFrame:
    if not destination_addresses:
        return pd.DataFrame()

    contract = chain.client.eth.contract(
        address=chain.client.toChecksumAddress(destination_addresses[0]),
        abi=BALANCER_AURA_DESTINATION_VAULT_ABI,
    )

    df = fetch_events(
        event=contract.events.UnderlyingWithdraw,
        chain=chain,
        start_block=start_block,
        addresses=destination_addresses,
        end_block=chain.get_block_near_top(),
    )

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["chain_id"] = chain.chain_id
    df["destination_vault_address"] = df["address"]
    return df


def _insert_new_rows(chain: ChainData, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return

    ensure_all_transactions_are_saved_in_db(
        tx_hashes=df["hash"].unique().tolist(),
        chain=chain,
    )

    rows = df.apply(
        lambda r: DestinationUnderlyingWithdraw(
            tx_hash=r["hash"],
            chain_id=int(r["chain_id"]),
            log_index=int(r["log_index"]),
            destination_vault_address=r["destination_vault_address"],
            amount=str(r["amount"]),
            owner=r["owner"],
            to_address=r["to"],
        ),
        axis=1,
    ).tolist()

    insert_avoid_conflicts(rows, DestinationUnderlyingWithdraw)


def ensure_destination_underlying_withdraw_are_current() -> None:
    highest_block = _get_highest_block_already_fetched_by_chain_id()
    destinations_df = get_full_table_as_df(Destinations)

    for chain in ALL_CHAINS:
        destination_addresses = (
            destinations_df[destinations_df["chain_id"] == chain.chain_id]["destination_vault_address"]
            .unique()
            .tolist()
        )
        if not destination_addresses:
            continue

        new_df = fetch_new_underlying_withdraw_events(
            start_block=highest_block[chain.chain_id],
            chain=chain,
            destination_addresses=destination_addresses,
        )

        if new_df.empty:
            print(f"No new DestinationUnderlyingWithdraw events for chain {chain.name}")
            continue

        _insert_new_rows(chain, new_df)

        print(
            f"Fetched {len(new_df):,} new DestinationUnderlyingWithdraw events for chain {chain.name} "
            f"starting from block {highest_block[chain.chain_id]:,}"
        )


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(ensure_destination_underlying_withdraw_are_current)
