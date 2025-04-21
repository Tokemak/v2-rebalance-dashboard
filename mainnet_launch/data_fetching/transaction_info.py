from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

from concurrent.futures import ThreadPoolExecutor, as_completed

from mainnet_launch.database.schema.full import Transactions, Blocks, Session
from mainnet_launch.constants import ChainData, ETH_CHAIN

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
        # 1) a SELECT over the unnest()
        sel_unnest = select(func.unnest(possible_hashes).label("tx_hash"))
        # 2) a SELECT of the existing tx_hashes
        sel_existing = select(Transactions.tx_hash)
        # 3) subtract them
        stmt = sel_unnest.except_(sel_existing)
        return session.scalars(stmt).all()


def _ensure_all_blocks_are_in_block_table(new_transactions_records: list[Transactions], chain) -> None:
    # make sure that the block timestamps are already saved
    blocks = [t.block for t in new_transactions_records]
    blocks_to_fetch = _extract_subset_of_blocks_not_already_in_blocks_table(blocks, chain)
    if blocks_to_fetch:
        block_timestamp_df = get_raw_state_by_blocks([], blocks_to_fetch, chain, include_block_number=True)
        block_timestamp_df["chain_id"] = chain.chain_id
        add_blocks_from_dataframe_to_database(block_timestamp_df)


def _extract_subset_of_blocks_not_already_in_blocks_table(blocks: list[int], chain: ChainData) -> list[int]:
    with Session() as session:
        sel_unnest = select(func.unnest(blocks).label("block"))  # unnest is similar to *[item1, item2 ...] unpacking
        sel_existing = select(Blocks.block).filter(Blocks.chain_id == chain.chain_id)
        stmt = sel_unnest.except_(sel_existing)
        return session.scalars(stmt).all()


def insert_many_transactions_into_table(tx_hashes: list[str], chain: ChainData) -> None:

    new_transactions_records = _fetch_all_new_transaction_records(tx_hashes, chain)
    _ensure_all_blocks_are_in_block_table(new_transactions_records, chain)

    stmt = (
        pg_insert(Transactions)
        .values(new_transactions_records[0].to_record())  # shape placeholder; executemany will handle the rest
        .on_conflict_do_nothing(index_elements=[Transactions.tx_hash])
    )
    CHUNK_SIZE = 1000

    # 6) transaction‐scoped session: commits on success, rolls back on error
    with Session.begin() as session:
        # 7) chunk large batches to avoid blowing out query size
        for i in range(0, len(new_transactions_records), CHUNK_SIZE):
            batch = new_transactions_records[i : i + CHUNK_SIZE]
            mappings = [tx.to_record() for tx in batch]
            session.execute(stmt, mappings)


def main():

    


    insert_many_transactions_into_table([tx_hash], ETH_CHAIN)


if __name__ == "__main__":
    main()
