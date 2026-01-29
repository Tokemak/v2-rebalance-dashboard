from sqlalchemy import text
from mainnet_launch.database.schema.full import TrackLastProcessedBlock, Session, Base
from mainnet_launch.database.postgres_operations import get_full_table_as_df
from mainnet_launch.constants import ChainData, ALL_CHAINS


def write_last_processed_block(chain: ChainData, block: int, table: type):
    """After we update a (chain, table) combination to a given block, store is so that we don't do redundent queries later"""

    query = """
            BEGIN;

            DELETE FROM track_last_processed_block
            WHERE chain_id = :chain_id
            AND table_name = :table_name;

            INSERT INTO track_last_processed_block (chain_id, table_name, last_processed_block)
            VALUES (:chain_id, :table_name, :block);

            COMMIT;
        """
    with Session() as session:
        session.execute(text(query), {"chain_id": chain.chain_id, "table_name": table.__tablename__, "block": block})
        session.commit()

    print(f"Wrote last processed block {block:,} for table {table.__tablename__} on chain {chain.name}")


def get_last_processed_block_for_table(table: type[Base]) -> dict[ChainData, int]:
    """Get the last processed block for each chain for the given table. If no entry exists for a chain, use the chain's block_autopool_first_deployed."""

    df = get_full_table_as_df(
        TrackLastProcessedBlock, where_clause=TrackLastProcessedBlock.table_name == table.__tablename__
    )

    chain_id_to_last_processed_block = df.set_index("chain_id")["last_processed_block"].to_dict()

    for chain in ALL_CHAINS:
        if chain.chain_id not in chain_id_to_last_processed_block:
            chain_id_to_last_processed_block[chain.chain_id] = chain.block_autopool_first_deployed

    chain_data_to_last_processed_block = {
        chain: chain_id_to_last_processed_block[chain.chain_id] for chain in ALL_CHAINS
    }

    return chain_data_to_last_processed_block
