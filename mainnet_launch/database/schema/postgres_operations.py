"""helper functions for postgres"""

from sqlalchemy.dialects.postgresql import insert as pg_insert

from mainnet_launch.database.schema.full import Session, Base, LastAutopoolUpdated, LastChainUpdated
from sqlalchemy.orm import InstrumentedAttribute

CHUNK_SIZE = 1000


def insert_avoid_conflicts(
    new_rows: list[Base], table: Base, index_elements: list[InstrumentedAttribute], expecting_rows: bool = False
) -> None:
    if not new_rows:
        if expecting_rows:
            raise ValueError("expecterd new rows here but found None")
        else:
            return

    rows_as_records = [r.to_record() for r in new_rows]
    stmt = pg_insert(table).on_conflict_do_nothing(index_elements=index_elements)
    with Session.begin() as session:
        for i in range(0, len(rows_as_records), CHUNK_SIZE):
            batch = rows_as_records[i : i + CHUNK_SIZE]
            session.execute(stmt, batch)


def update_last_autopool_updated(table_name: str, block: int, autopool: str) -> None:
    """
    Inserts or updates a row in the 'last_autopool_updated' table for the given
    table_name, setting the latest processed block and autopool identifier.

    :param table_name: Name of the destination table being tracked.
    :param block: Latest block number processed.
    :param autopool: Name of the autopool associated with this update.
    """
    stmt = insert(LastAutopoolUpdated).values(table_name=table_name, block=block, autopool=autopool)
    stmt = stmt.on_conflict_do_update(
        index_elements=[LastAutopoolUpdated.table_name], set_={"block": block, "autopool": autopool}
    )

    with Session.begin() as session:
        session.execute(stmt)
        session.commit()


def update_last_chain_updated(table_name: str, block: int, chain_id: str) -> None:
    """
    Inserts or updates a row in the 'last_chain_updated' table for the given
    table_name, setting the latest processed block and chain identifier.

    :param table_name: Name of the destination table being tracked.
    :param block: Latest block number processed.
    :param chain_id: Name of the chain being tracked.
    """
    stmt = insert(LastChainUpdated).values(table_name=table_name, block=block, chain_id=chain_id)
    stmt = stmt.on_conflict_do_update(
        index_elements=[LastChainUpdated.table_name], set_={"block": block, "chain_id": chain_id}
    )
    with Session.begin() as session:
        session.execute(stmt)
        session.commit()
