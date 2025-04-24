"""helper functions for postgres"""

from mainnet_launch.database.schema.full import Session, Base
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import OperatorExpression
from sqlalchemy import text

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

    cols = list(rows_as_records[0].keys())
    col_list = ", ".join(cols)
    placeholder = ", ".join(f":{c}" for c in cols)
    conflict_cols = ", ".join(col.key for col in index_elements)

    sql = text(
        f"""
        INSERT INTO {table.__tablename__} ({col_list})
        VALUES ({placeholder})
        ON CONFLICT ({conflict_cols}) DO NOTHING
        """
    )

    with Session.begin() as session:
        for i in range(0, len(rows_as_records), CHUNK_SIZE):
            batch = rows_as_records[i : i + CHUNK_SIZE]
            session.execute(sql, batch)


def get_highest_value_in_field_where(table: Base, column: InstrumentedAttribute, where_clause: OperatorExpression):
    # eg `where_clause = Blocks.chain_id == 1`
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT MAX({column.key})
              FROM {table.__tablename__}
             WHERE {where_sql}
        """
        )
        return session.execute(sql).scalar_one_or_none()


def get_subset_not_already_in_column(
    table: Base, column: InstrumentedAttribute, values: list[any], where_clause: OperatorExpression | None
):
    # returns[ a for a in all_values if a not in table[column] given where_clause]
    with Session.begin() as session:
        if where_clause is not None:
            where_sql = _where_clause_to_string(where_clause, session)
            where_sql = f"WHERE {where_sql}"
        else:
            where_sql = ""

        sql = text(
            f"""
            SELECT v
              FROM unnest(:values) AS v
            EXCEPT
            SELECT {column.key}
              FROM {table.__tablename__}
            {where_sql}
        """
        )

        return session.execute(sql, {"values": values}).scalars().all()


def _where_clause_to_string(where_clause: OperatorExpression, session) -> str:
    """
    where_clause like `Blocks.chain_id == ETH_CHAIN.chain_id`
    """
    dialect = session.get_bind().dialect
    compiled_where = where_clause.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    where_clause_as_sql = str(compiled_where)
    return where_clause_as_sql


# def insert_avoid_conflicts2(
#     new_rows: list[Base], table: Base, index_elements: list[InstrumentedAttribute], expecting_rows: bool = False
# ) -> None:
#     if not new_rows:
#         if expecting_rows:
#             raise ValueError("expecterd new rows here but found None")
#         else:
#             return

# rows_as_records = [r.to_record() for r in new_rows]
# stmt = pg_insert(table).on_conflict_do_nothing(index_elements=index_elements)
# with Session.begin() as session:
#     for i in range(0, len(rows_as_records), CHUNK_SIZE):
#         batch = rows_as_records[i : i + CHUNK_SIZE]
#         session.execute(stmt, batch)


# def update_last_autopool_updated(table_name: str, block: int, autopool: str) -> None:
#     """
#     Inserts or updates a row in the 'last_autopool_updated' table for the given
#     table_name, setting the latest processed block and autopool identifier.

#     :param table_name: Name of the destination table being tracked.
#     :param block: Latest block number processed.
#     :param autopool: Name of the autopool associated with this update.
#     """
#     stmt = insert(LastAutopoolUpdated).values(table_name=table_name, block=block, autopool=autopool)
#     stmt = stmt.on_conflict_do_update(
#         index_elements=[LastAutopoolUpdated.table_name], set_={"block": block, "autopool": autopool}
#     )

#     with Session.begin() as session:
#         session.execute(stmt)
#         session.commit()


# def update_last_chain_updated(table_name: str, block: int, chain_id: str) -> None:
#     """
#     Inserts or updates a row in the 'last_chain_updated' table for the given
#     table_name, setting the latest processed block and chain identifier.

#     :param table_name: Name of the destination table being tracked.
#     :param block: Latest block number processed.
#     :param chain_id: Name of the chain being tracked.
#     """
#     stmt = insert(LastChainUpdated).values(table_name=table_name, block=block, chain_id=chain_id)
#     stmt = stmt.on_conflict_do_update(
#         index_elements=[LastChainUpdated.table_name], set_={"block": block, "chain_id": chain_id}
#     )
#     with Session.begin() as session:
#         session.execute(stmt)
#         session.commit()
