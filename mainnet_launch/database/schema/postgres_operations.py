"""data access layer functions for postgres"""

from dataclasses import dataclass
import io
import csv
from psycopg2 import sql

from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import OperatorExpression, BooleanClauseList
from sqlalchemy import text
from psycopg2.extras import execute_values
from sqlalchemy.dialects import postgresql
import numpy as np

import pandas as pd


from mainnet_launch.database.schema.full import Session, Base, ENGINE
from mainnet_launch.constants import time_decorator


@dataclass
class TableSelector:
    table: Base
    select_fields: InstrumentedAttribute | list[InstrumentedAttribute] | None = None
    join_on: BooleanClauseList | None = None
    row_filter: BooleanClauseList = None


def merge_tables_as_df(
    selectors: list[TableSelector],
    where_clause: BooleanClauseList | None = None,
    order_by: InstrumentedAttribute | None = None,
    order: str = "asc",
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
    #
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

        sql = "SELECT\n" "    " + ",\n    ".join(select_parts) + "\n" f"FROM {selectors[0].table.__tablename__}\n"

        # Add JOIN clauses
        for spec in selectors[1:]:
            on_sql = spec.join_on.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
            sql += f"JOIN {spec.table.__tablename__}\n" f"  ON {on_sql}\n"
        # Collect WHERE filters
        filters = []
        if where_clause is not None:
            filters.append(where_clause)
        for spec in selectors:
            if spec.row_filter is not None:
                filters.append(spec.row_filter)

        if filters:
            # Combine filters with AND
            where_sql = " AND\n     ".join(
                f"({flt.compile(dialect=dialect, compile_kwargs={'literal_binds': True})})" for flt in filters
            )
            sql += "WHERE\n"
            sql += "    " + where_sql + "\n"

        if order_by is not None:
            compiled_order = order_by.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
            dir_upper = order.lower().upper()
            if dir_upper not in ("ASC", "DESC"):
                raise ValueError("order must be 'asc' or 'desc'")

            sql += "ORDER BY\n"
            sql += f"    {compiled_order} {dir_upper}\n"

        return _exec_sql_and_cache(sql)


# @st.cache_data(ttl=60 * 60)
def _exec_sql_and_cache(sql_plain_text: str) -> pd.DataFrame:
    """cached on just the SQL text"""
    if not isinstance(sql_plain_text, str):
        raise TypeError("sql_plain_text must be a string")

    with Session.begin() as session:
        return pd.read_sql(text(sql_plain_text), con=session.get_bind())


def insert_avoid_conflicts(
    new_rows: list[Base], table: Base, index_elements: list[InstrumentedAttribute] = None, expecting_rows: bool = False
) -> None:
    if not (isinstance(table, type) and issubclass(table, Base)):
        raise TypeError("must be in order insert_avoid_conflicts(new_rows, table, *args), might have wrong order")

    if not new_rows:
        if expecting_rows:
            raise ValueError("expecterd new rows here but found None")
        else:
            return

    rows_as_tuples = list(set([r.to_tuple() for r in new_rows]))
    bulk_copy_skip_duplicates(rows_as_tuples, table)


def bulk_copy_skip_duplicates(rows: list[tuple], table: type[Base]) -> None:
    """
    Bulk‑load `rows` into `table`, skipping any duplicates
    (based on the table's primary key or unique constraints).
    """
    tn = table.__tablename__
    cols = [col.name for col in table.__table__.columns]
    id_cols = [col.name for col in table.__table__.primary_key.columns]

    # 1) Serialize rows to in‑memory CSV
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerows(rows)
    buf.seek(0)

    # 2) Prepare COPY & INSERT statements
    copy_into_staging = sql.SQL("COPY {stg} ({fields}) FROM STDIN WITH CSV").format(
        stg=sql.Identifier(f"{tn}_staging"),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
    )
    insert_main = sql.SQL(
        """
        INSERT INTO {main} ({fields})
        SELECT {fields} FROM {stg}
        ON CONFLICT ({pkey}) DO NOTHING
    """
    ).format(
        main=sql.Identifier(tn),
        stg=sql.Identifier(f"{tn}_staging"),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
        pkey=sql.SQL(", ").join(map(sql.Identifier, id_cols)),
    )

    with ENGINE.connect() as conn:
        # this begin() opens a transaction and will commit on exit
        with conn.begin():
            # grab the raw psycopg2 connection
            raw_conn = conn.connection
            with raw_conn.cursor() as cur:
                # a) Drop old staging
                cur.execute(sql.SQL("DROP TABLE IF EXISTS {stg}").format(stg=sql.Identifier(f"{tn}_staging")))
                # b) Create new temp staging
                cur.execute(
                    sql.SQL(
                        "CREATE TEMP TABLE {stg} (LIKE {main} "
                        "INCLUDING ALL EXCLUDING CONSTRAINTS EXCLUDING INDEXES) "
                        "ON COMMIT DROP"
                    ).format(
                        stg=sql.Identifier(f"{tn}_staging"),
                        main=sql.Identifier(tn),
                    )
                )
                # c) Bulk‐COPY into staging
                cur.copy_expert(copy_into_staging, buf)
                # d) Move into main table
                cur.execute(insert_main)


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


def _to_python_list(values) -> list:
    """Flatten to 1-D and convert numpy scalars -> native Python scalars."""
    try:
        arr = np.asarray(values)
        if arr.ndim != 1:
            raise ValueError(f"Expected 1-D values, got shape {arr.shape}")
        return arr.ravel().tolist()  # returns native Python scalars
    except Exception:
        # No numpy or coercion failed; fall back to simple list
        return list(values)


# TODO this is too slow, need a better pattern
# crazy non linear increases
# Inserted 6598 new transactions for base
# ensure_all_transactions_are_saved_in_db took 188.5424 seconds.
# Fetching 833 new transactions for sonic
# Inserted 833 new transactions for sonic
# ensure_all_transactions_are_saved_in_db took 8.7739 seconds.


def get_subset_not_already_in_column(
    table: Base,
    column: InstrumentedAttribute,
    values,
    where_clause: OperatorExpression | None = None,
) -> list:
    """
    Return the items in `values` that are NOT already present in `table.column`,
    respecting an optional SQLAlchemy `where_clause`.

    Uses a single round-trip UNNEST + anti-join. Preserves input order and duplicates.
    NULL-safe equality via IS NOT DISTINCT FROM.
    """

    col = column.property.columns[0]
    sql_column_type = col.type.compile(dialect=postgresql.dialect()).upper()

    if sql_column_type.upper() not in ["TEXT", "VARCHAR", "INTEGER", "BIGINT", "NUMERIC", "FLOAT"]:
        raise ValueError(f"Unsupported sql_column_type: {sql_column_type}")

    values = list(set(_to_python_list(values)))
    if not values:
        return []

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql_txt = f"""
        WITH input(v) AS (
            SELECT * FROM UNNEST(CAST(:vals AS {sql_column_type}[]))
        ),
        existing AS (
            SELECT {column.key}
            FROM {table.__tablename__}
            {where_sql}
        )
        SELECT i.v
        FROM input i
        LEFT JOIN existing e
          ON e.{column.key} IS NOT DISTINCT FROM i.v
        WHERE e.{column.key} IS NULL
        """
        rows = session.execute(text(sql_txt), {"vals": values}).scalars().all()
        return rows


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


def get_subset_of_table_as_df(
    table: Base, columns: list[InstrumentedAttribute] | None = None, where_clause: OperatorExpression | None = None
) -> pd.DataFrame:
    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        if columns:
            tbl = table.__tablename__
            cols_sql = ", ".join(f"{tbl}.{col.key}" for col in columns)
        else:
            cols_sql = "*"

        sql = text(
            f"""
            SELECT
              {cols_sql}
            FROM
              {table.__tablename__}
            {where_sql}
        """
        )

        return pd.read_sql(sql, con=session.get_bind())


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


def bulk_overwrite(rows: list[tuple], table: type[Base]) -> None:
    """
    Atomically delete any rows whose primary keys appear in `rows`,
    then insert all of `rows` (i.e. “upsert by delete+insert”),
    using context managers throughout.
    """
    tn = table.__tablename__
    cols = [c.name for c in table.__table__.columns]
    id_cols = [c.name for c in table.__table__.primary_key.columns]

    # build list of just the PK‐tuples for deletion
    pk_positions = [cols.index(pk) for pk in id_cols]
    pk_tuples = [tuple(r[pos] for pos in pk_positions) for r in rows]

    delete_sql = sql.SQL("DELETE FROM {table} WHERE ({pkey}) IN %s").format(
        table=sql.Identifier(tn), pkey=sql.SQL(", ").join(map(sql.Identifier, id_cols))
    )
    insert_sql = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s").format(
        table=sql.Identifier(tn), fields=sql.SQL(", ").join(map(sql.Identifier, cols))
    )

    # Use `with` so conn.commit()/rollback() and close() are automatic
    with ENGINE.raw_connection() as conn:
        with conn.cursor() as cur:
            # 1) delete any conflicting rows
            cur.execute(delete_sql, (tuple(pk_tuples),))
            # 2) bulk‐insert all new rows
            execute_values(cur, insert_sql.as_string(cur), rows)


def set_some_cells_to_null(
    table: Base,
    rows: list[Base],
    cols_to_null: list[InstrumentedAttribute],
) -> None:
    """
    UPDATE <table>
       SET col1 = NULL, col2 = NULL, …
     WHERE (pk1, pk2, …) IN ( (v11,v12,…), (v21,v22,…), … )
    """
    tn = table.__tablename__

    # 1) extract PK column names and build tuple-of-tuples from rows
    pk_cols = [c.name for c in table.__table__.primary_key.columns]
    pk_tuples = [tuple(getattr(row, col) for col in pk_cols) for row in rows]

    # 2) build "col1 = NULL, col2 = NULL, …"
    set_clause = ", ".join(f"{col.key} = NULL" for col in cols_to_null)
    #    and "(pk1, pk2, …)"
    pkey_list = ", ".join(pk_cols)

    # 3) parameterized SQL; SQLAlchemy will pass %s parameters through to psycopg2
    sql_stmt = f"""
        UPDATE {table.__tablename__}
           SET {set_clause}
         WHERE ({pkey_list}) IN %s
    """

    # use raw_connection so psycopg2 can handle the tuple‐of‐tuples param
    with ENGINE.connect() as conn:
        # this begin() opens a transaction and will commit on exit
        with conn.begin():
            conn.exec_driver_sql(sql_stmt, (tuple(pk_tuples),))


def simple_agg_by_one_table(
    table: Base,
    target_column: InstrumentedAttribute,
    target_column_alias: str,
    group_by_column: InstrumentedAttribute,
    aggregation_function: str,
    where_clause: BooleanClauseList | None = None,
) -> pd.DataFrame:
    """
    Run the “max block per from_address” query with an arbitrary WHERE clause.

    :param where_clause: a SQL fragment, e.g. "chain_id = 1"
    :returns: DataFrame(columns=['from_address', 'max_block'])
    """

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)
        sql = f"""
            SELECT
            {group_by_column.key},
            {aggregation_function}({target_column.key}) as {target_column_alias}
            FROM {table.__tablename__}
            {where_sql}
            GROUP BY {group_by_column.key}
        """
        return _exec_sql_and_cache(sql)


if __name__ == "__main__":

    from mainnet_launch.database.schema.full import Blocks

    all_blocks = get_full_table_as_orm(Blocks, Blocks.chain_id == 1)
    print(len(all_blocks))

    pass
