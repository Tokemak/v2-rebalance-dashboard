"""data access layer functions for postgres"""

from dataclasses import dataclass

from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import OperatorExpression, BooleanClauseList
from sqlalchemy import text
from psycopg2.extras import execute_values
import pandas as pd

from mainnet_launch.database.schema.full import Session, Base


@dataclass
class TableSelector:
    table: Base
    select_fields: InstrumentedAttribute | list[InstrumentedAttribute] | None = None
    join_on: BooleanClauseList | None = None
    row_filter: BooleanClauseList = None


def merge_tables_as_df(
    selectors: list[TableSelector],
    where_clause: BooleanClauseList | None = None,
) -> pd.DataFrame:
    """
    Perform a multi-table JOIN based on the provided selectors.

    :param selectors: List of TableSelector, where the first entry becomes the FROM table,
                      and subsequent entries are JOINs.
    :param global_filter: An optional SQLA boolean expression applied as a global WHERE.
    :returns: A pandas DataFrame containing the joined result.
    """
    if not selectors:
        raise ValueError("At least one TableSelector is required")

    with Session.begin() as session:
        dialect = session.get_bind().dialect

        select_parts: list[str] = []
        for spec in selectors:
            tbl_name = spec.table.__tablename__
            if spec.select_fields is None:
                select_parts.append(f"{tbl_name}.*")
            else:
                cols = spec.select_fields if isinstance(spec.select_fields, (list, tuple)) else [spec.select_fields]
                for col in cols:
                    select_parts.append(f"{tbl_name}.{col.key}")

        # Start SQL with FROM
        first = selectors[0]
        sql = f"SELECT {', '.join(select_parts)}\nFROM {first.table.__tablename__}"

        # Add JOIN clauses
        for spec in selectors[1:]:
            on_sql = spec.join_on.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
            sql += f"\nJOIN {spec.table.__tablename__} ON {on_sql}"

        # Collect WHERE filters
        filters = []
        if where_clause is not None:
            filters.append(where_clause)
        for spec in selectors:
            if spec.row_filter is not None:
                filters.append(spec.row_filter)

        if filters:
            # Combine filters with AND
            where_sql = " AND ".join(
                f"({flt.compile(dialect=dialect, compile_kwargs={'literal_binds': True})})" for flt in filters
            )
            sql += f"\nWHERE {where_sql}"

        # Execute and return DataFrame
        return pd.read_sql(text(sql), con=session.get_bind())


CHUNK_SIZE = 1000


def insert_avoid_conflicts(
    new_rows: list[Base], table: Base, index_elements: list[InstrumentedAttribute], expecting_rows: bool = False
) -> None:
    if not new_rows:
        if expecting_rows:
            raise ValueError("expecterd new rows here but found None")
        else:
            return

    rows_as_tuples = [r.to_tuple() for r in new_rows]

    cols = list(new_rows[0].to_record().keys())
    col_list = ", ".join(cols)
    conflict_cols = ", ".join(col.key for col in index_elements)

    sql = f"""
        INSERT INTO {table.__tablename__} ({col_list})
        VALUES %s
        ON CONFLICT ({conflict_cols}) DO NOTHING
        """

    with Session.begin() as sess:
        conn = sess.connection().connection
        # bulk insert chunk size rows at a time
        with conn.cursor() as cur:
            for i in range(0, len(rows_as_tuples), CHUNK_SIZE):
                batch = rows_as_tuples[i : i + CHUNK_SIZE]
                execute_values(cur, sql, batch)


def get_highest_value_in_field_where(table: Base, column: InstrumentedAttribute, where_clause: OperatorExpression):
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT MAX({column.key})
              FROM {table.__tablename__}
             {where_sql}
        """
        )
        return session.execute(sql).scalar_one_or_none()


def get_subset_not_already_in_column(
    table: Base, column: InstrumentedAttribute, values: list[any], where_clause: OperatorExpression | None
):
    orms = get_full_table_as_orm(table, where_clause=where_clause)
    existing = [getattr(obj, column.key) for obj in orms]
    return [v for v in values if v not in existing]

    # this is running into issues with unnest. doing in python here, slight optimization to do it in sql

    # with Session.begin() as session:
    #     col = column.property.columns[0]
    #     base_type = str(col.type).split("(")[0]
    #     where_sql = _where_clause_to_string(where_clause, session)

    #     sql = text(f"""
    #         SELECT v
    #           FROM unnest(:values:: {base_type}[]) AS t(v)
    #         EXCEPT
    #         SELECT {column.key}
    #           FROM {table.__tablename__}
    #         {where_sql}
    #     """)
    #     # Now :values is literally a PG array
    #     return session.execute(sql, {"values": values}).scalars().all()


def get_full_table_as_df(table: Base, where_clause: OperatorExpression | None = None) -> pd.DataFrame:
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT *
            FROM {table.__tablename__}
            {where_sql}
        """
        )

        df = pd.read_sql(sql, con=session.get_bind())
        return df


def get_full_table_as_orm(table: Base, where_clause: OperatorExpression | None = None) -> list[Base]:
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT *
            FROM {table.__tablename__}
            {where_sql}
        """
        )

        return [table.from_tuple(tup) for tup in session.execute(sql).all()]


# TODO use the merge many tables instead (if needed)
def natural_left_right_using_where(
    left: Base,
    right: Base,
    using: list[InstrumentedAttribute],
    where_clause: OperatorExpression | None = None,
) -> pd.DataFrame:

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)
        cols = ", ".join(col.key for col in using)
        using_sql = f"({cols})"
        sql = text(
            f"""
            SELECT *
            FROM {left.__tablename__}
            JOIN {right.__tablename__}
            USING {using_sql}
            {where_sql}
        """
        )

        return pd.read_sql(sql, con=session.get_bind())


# not used, use the natural join when possible, it is clearer
def generic_merge_tables_as_df(
    left: type[Base],
    right: type[Base],
    left_join_columns: list[InstrumentedAttribute],
    right_join_columns: list[InstrumentedAttribute],
    where_clause: OperatorExpression | None = None,
) -> pd.DataFrame:
    """
    Fully merges two tables on arbitrary column pairs:
      SELECT l.*, r.*
      FROM <left>  AS l
      JOIN <right> AS r
        ON (l.left_join_columns[i] = r.right_join_columns[i] AND ...)
      [WHERE <where_clause>]

    Arguments:
      left                -- ORM class for the “left” table
      right               -- ORM class for the “right” table
      left_join_columns   -- list of InstrumentedAttribute on left side
      right_join_columns  -- list of InstrumentedAttribute on right side (same length)
      where_clause        -- optional SQLAlchemy boolean expression for filtering
    """
    if len(left_join_columns) != len(right_join_columns):
        raise ValueError("left_join_columns and right_join_columns must be the same length")

    left_table = left.__tablename__
    right_table = right.__tablename__

    # build the ON clause
    on_clauses = []
    for lcol, rcol in zip(left_join_columns, right_join_columns):
        on_clauses.append(f"l.{lcol.key} = r.{rcol.key}")
    on_sql = " AND ".join(on_clauses)

    # build WHERE fragment
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT
              l.*,    -- all columns from left
              r.*     -- all columns from right
            FROM {left_table}  AS l
            JOIN {right_table} AS r
              ON {on_sql}
            {where_sql}
        """
        )

        return pd.read_sql(sql, con=session.get_bind())


def _where_clause_to_string(where_clause: OperatorExpression | None, session) -> str:
    """
    where_clause like `Blocks.chain_id == ETH_CHAIN.chain_id`
    """
    if where_clause is not None:
        dialect = session.get_bind().dialect
        compiled_where = where_clause.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
        return f"WHERE {str(compiled_where)}"
    else:
        return ""


if __name__ == "__main__":
    from mainnet_launch.database.schema.full import Blocks

    all_blocks = get_full_table_as_orm(Blocks, Blocks.chain_id == 1)
    print(all_blocks[0])
