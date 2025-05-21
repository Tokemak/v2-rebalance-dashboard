from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import select, func
import pandas as pd


from mainnet_launch.database.schema.full import Transactions, Blocks, Session
from mainnet_launch.database.schema.postgres_operations import insert_avoid_conflicts

from mainnet_launch.constants import ChainData, ETH_CHAIN, time_decorator
from mainnet_launch.data_fetching.block_timestamp import add_blocks_from_dataframe_to_database
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks


def _fetch_new_transaction_row(tx_hash: str, chain: ChainData) -> Transactions:
    tx_hash = tx_hash.lower()
    tx_receipt = chain.client.eth.get_transaction_receipt(tx_hash)
    # effectiveGasPrice replaces gasPrice after 2021 because of the London Fork
    return Transactions(
        tx_hash=tx_hash,
        block=tx_receipt["blockNumber"],
        chain_id=chain.chain_id,
        from_address=tx_receipt["from"],
        to_address=tx_receipt["to"],
        effective_gas_price=tx_receipt["effectiveGasPrice"],
        gas_used=tx_receipt["gasUsed"],
        gas_cost_in_eth=(tx_receipt["gasUsed"] * tx_receipt["effectiveGasPrice"]) / 1e18,
    )


def _fetch_all_new_transaction_records(tx_hashes: list[str], chain: ChainData) -> list[Transactions]:
    new_transactions_records: list[Transactions] = []
    hashes_to_fetch = _extract_subset_of_hashes_not_already_in_transactions_table(tx_hashes)
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = {executor.submit(_fetch_new_transaction_row, h, chain): h for h in hashes_to_fetch}
        for future in as_completed(futures):
            new_transactions_records.append(future.result())

    return new_transactions_records


def _extract_subset_of_hashes_not_already_in_transactions_table(possible_hashes: list[str]) -> list[str]:
    with Session() as session:
        # unnest is similar to *[item1, item2 ...] unpacking
        sel_unnest = select(func.unnest(possible_hashes).label("tx_hash"))
        sel_existing = select(Transactions.tx_hash)
        stmt = sel_unnest.except_(sel_existing)
        return session.scalars(stmt).all()


def _ensure_all_blocks_are_in_block_table(new_transactions: list[Transactions], chain: ChainData) -> None:
    blocks = list(set([t.block for t in new_transactions]))
    blocks_to_fetch = _extract_subset_of_blocks_not_already_in_blocks_table(blocks, chain)
    if blocks_to_fetch:
        block_timestamp_df = get_raw_state_by_blocks([], blocks_to_fetch, chain, include_block_number=True)
        block_timestamp_df["chain_id"] = chain.chain_id
        add_blocks_from_dataframe_to_database(block_timestamp_df)


def _extract_subset_of_blocks_not_already_in_blocks_table(blocks: list[int], chain: ChainData) -> list[int]:
    # make sure all the big blocks of sql are in one file
    if not blocks:
        return []
    with Session() as session:
        sel_unnest = select(func.unnest(blocks).label("block"))
        sel_existing = select(Blocks.block).filter(Blocks.chain_id == chain.chain_id)
        stmt = sel_unnest.except_(sel_existing)
        return session.scalars(stmt).all()


@time_decorator
def insert_many_transactions_into_table(tx_hashes: pd.Series | list[str], chain: ChainData) -> None:
    if isinstance(tx_hashes, pd.Series):
        tx_hashes = list(tx_hashes)
    new_transactions = _fetch_all_new_transaction_records(tx_hashes, chain)

    if not new_transactions:
        return
    _ensure_all_blocks_are_in_block_table(new_transactions, chain)
    insert_avoid_conflicts(new_transactions, Transactions, index_elements=[Transactions.tx_hash])


from mainnet_launch.pages.rebalance_events.rebalance_events import *
from mainnet_launch.constants import AUTO_ETH


def main():

    autopool = AUTO_ETH

    weth_contract = autopool.chain.client.eth.contract(WETH(autopool.chain), abi=ERC_20_ABI)
    to_autopool = fetch_events(weth_contract.events.Transfer, ETH_CHAIN, 19_000_000, 19_000_000 + 10)

    print(to_autopool)
    insert_many_transactions_into_table(to_autopool["hash"], ETH_CHAIN)


if __name__ == "__main__":
    main()
