"""data access layer functions for postgres"""

from __future__ import annotations

from dataclasses import dataclass
import io
import csv
from psycopg2 import sql

from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute
from sqlalchemy.sql.elements import OperatorExpression, BooleanClauseList
from sqlalchemy import text, inspect
from psycopg2.extras import execute_values
from sqlalchemy.dialects import postgresql
import numpy as np
import pandas as pd


from mainnet_launch.database.schema.full import Session, Base, ENGINE

# cchecksum faster if needed
# https://github.com/BobTheBuidler/cchecksum


class CustomPostgresOperationException(Exception):
    """An error generated on the query side not by postgres"""

    pass


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
        raise CustomPostgresOperationException("At least one TableSelector is required")
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
                raise CustomPostgresOperationException("order must be 'asc' or 'desc'")

            sql += "ORDER BY\n"
            sql += f"    {compiled_order} {dir_upper}\n"

        return _exec_sql_and_cache(sql)


# @st.cache_data(ttl=60 * 60)
def _exec_sql_and_cache(sql_plain_text: str) -> pd.DataFrame:
    """cached on just the SQL text"""
    if not isinstance(sql_plain_text, str):
        raise TypeError("sql_plain_text must be a string")

    with Session.begin() as session:
        df = pd.read_sql(text(sql_plain_text), con=session.get_bind())
        return df


def assert_table_schema_matches_model(engine, table: type[DeclarativeBase]) -> None:
    sa_table = table.__table__
    tn = sa_table.name
    schema = sa_table.schema  # None -> search_path/default

    model_cols = [c.name for c in sa_table.columns]

    insp = inspect(engine)
    db_cols = [c["name"] for c in insp.get_columns(tn, schema=schema)]

    model_only = [c for c in model_cols if c not in db_cols]
    db_only = [c for c in db_cols if c not in model_cols]

    if model_only or db_only:
        qual = f"{schema}.{tn}" if schema else tn
        raise RuntimeError(
            f"Schema drift detected for {qual}.\n"
            f"  Columns only in model: {model_only}\n"
            f"  Columns only in DB:    {db_only}\n"
            f"  Model columns:         {model_cols}\n"
            f"  DB columns:            {db_cols}\n"
            f"Resolve via migrations / rebuild."
        )


def insert_avoid_conflicts(
    new_rows: list[Base], table: Base, index_elements: list[InstrumentedAttribute] = None, expecting_rows: bool = False
) -> None:
    assert_table_schema_matches_model(ENGINE, table)
    if not (isinstance(table, type) and issubclass(table, Base)):
        raise TypeError("must be in order insert_avoid_conflicts(new_rows, table, *args), might have wrong order")

    if not new_rows:
        if expecting_rows:
            raise CustomPostgresOperationException("expecterd new rows here but found None")
        else:
            return

    rows_as_tuples = list(set([r.to_tuple() for r in new_rows]))
    bulk_copy_skip_duplicates(rows_as_tuples, table)


def bulk_copy_skip_duplicates(rows: list[tuple], table: type[Base]) -> None:
    """
    Bulk-load `rows` into `table`, skipping any duplicates
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


def get_subset_not_already_in_column(
    table: Base,
    column: InstrumentedAttribute,
    values,
    where_clause: OperatorExpression | None = None,
) -> list:
    # set differece in python is still faster, than unnest generally,
    # return get_subset_not_already_in_column_unnest(table, column, values, where_clause)
    return get_subset_not_already_in_column_in_python(table, column, values, where_clause)


def get_subset_not_already_in_column_in_python(
    table: Base,
    column: InstrumentedAttribute,
    values,
    where_clause: OperatorExpression | None = None,
) -> list:

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        query = f"""SELECT {column.key}
                FROM {table.__tablename__}
                {where_sql}"""

        df = pd.read_sql(text(query), con=session.get_bind())

        rows = df[column.key].tolist()

    existing_values = set(rows)
    input_values = set(_to_python_list(values))
    missing_values = input_values - existing_values
    return list(missing_values)


def get_subset_not_already_in_column_unnest(
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
        raise CustomPostgresOperationException(f"Unsupported sql_column_type: {sql_column_type}")

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


def get_full_table_as_df_with_block(table: Base, where_clause: OperatorExpression | None = None) -> pd.DataFrame:
    """Fetch the full contents of a table (that has block and chain_id) along with the associated block datetime.

    Optionally filter with a SQLAlchemy where_clause.
    """
    column_names = [a.name for a in table.__table__.columns]
    if not {"block", "chain_id"}.issubset(column_names):
        raise CustomPostgresOperationException(
            f"Table {table.__tablename__} must have both 'block' and 'chain_id' columns"
        )

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT
                {table.__tablename__}.*,
                blocks.datetime
            FROM {table.__tablename__}
            JOIN blocks
              ON {table.__tablename__}.block = blocks.block
             AND {table.__tablename__}.chain_id = blocks.chain_id
            {where_sql}
            ORDER BY blocks.datetime DESC
            """
        )

        df = pd.read_sql(sql, con=session.get_bind())

        df.set_index("datetime", inplace=True)
        return df


def get_full_table_as_df_with_tx_hash(table: Base, where_clause: OperatorExpression | None = None) -> pd.DataFrame:
    """Fetch the full contents of a table (that has transaction hashes) along with the associated transaction datetime.

    Optionally filter with a SQLAlchemy where_clause.
    """
    if "tx_hash" not in [a.name for a in table.__table__.columns]:
        raise CustomPostgresOperationException(f"Table {table.__tablename__} must have a 'tx_hash' column")

    with Session.begin() as session:
        where_sql = _where_clause_to_string(where_clause, session)

        sql = text(
            f"""
            SELECT
                {table.__tablename__}.*,
                blocks.datetime,
                blocks.block
            FROM {table.__tablename__}
            JOIN transactions
              ON {table.__tablename__}.tx_hash = transactions.tx_hash
            JOIN blocks
              ON transactions.block = blocks.block
             AND transactions.chain_id = blocks.chain_id
            {where_sql}
            ORDER BY blocks.datetime DESC
            """
        )

        df = pd.read_sql(sql, con=session.get_bind())

        df.set_index("datetime", inplace=True)
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

        df = pd.read_sql(sql, con=session.get_bind())

        return df


def get_full_table_as_orm(table: Base, where_clause: OperatorExpression | None = None) -> list[Base]:
    print(
        "This function might not properly convert from BYTEA to 0x strings, might want to use get_full_table_as_df instead"
    )
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

        df = pd.read_sql(sql, con=session.get_bind())

        return df


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


# broken
def bulk_overwrite(new_rows: list[Base], table: type[Base]) -> None:
    """
    Atomically delete any rows whose primary keys appear in `new_rows`,
    then insert all of `new_rows` (delete+insert “overwrite”).

    Requires ORM rows that implement `.to_tuple()`.
    """
    if not new_rows:
        return

    tn = table.__tablename__
    cols = [c.name for c in table.__table__.columns]
    id_cols = [c.name for c in table.__table__.primary_key.columns]

    # Minimal conversion: require ORM rows with .to_tuple()
    try:
        rows = [r.to_tuple() for r in new_rows]
    except Exception as e:
        raise TypeError(
            "bulk_overwrite expects `new_rows` to be ORM objects that implement `.to_tuple()` "
            "(e.g., subclasses of Base)."
        ) from e

    pk_positions = [cols.index(pk) for pk in id_cols]
    pk_tuples = [tuple(r[pos] for pos in pk_positions) for r in rows]

    delete_sql = sql.SQL("DELETE FROM {table} WHERE ({pkey}) IN %s").format(
        table=sql.Identifier(tn),
        pkey=sql.SQL(", ").join(map(sql.Identifier, id_cols)),
    )
    insert_sql = sql.SQL("INSERT INTO {table} ({fields}) VALUES %s").format(
        table=sql.Identifier(tn),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
    )

    with ENGINE.raw_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(delete_sql, (tuple(pk_tuples),))
            execute_values(cur, insert_sql.as_string(cur), rows)
        conn.commit()


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


# import string
# from typing import Any

# _HEXDIGITS = set(string.hexdigits)


# class BytesConversionError(Exception):
#     pass


# # I think this should just be string | None to bytes
# def _coerce_to_bytes(v: Any) -> bytes | None:
#     if v is None:
#         return None
#     if isinstance(v, memoryview):
#         return v.tobytes()
#     if isinstance(v, (bytes, bytearray)):
#         return bytes(v)
#     try:
#         if isinstance(v, HexBytes):
#             return bytes(v)
#     except Exception as e:
#         raise BytesConversionError("Failed to convert HexBytes", v) from e

#     raise BytesConversionError(f"Failed to convert {v} into bytes Btes")


# def _bytea_to_0x(v: Any) -> Any:
#     b = _coerce_to_bytes(v)
#     if b is None:
#         return v

#     # Case 1: BYTEA contains ASCII like b"0xabc123..." or b"abc123..."
#     try:
#         s = b.decode("ascii").strip()
#     except UnicodeDecodeError:
#         s = None

#     if s:
#         if s.startswith(("0x", "0X")):
#             hex_part = s[2:]
#             if hex_part and all(c in _HEXDIGITS for c in hex_part):
#                 hex_str = "0x" + hex_part.lower()
#             else:
#                 hex_str = None
#         else:
#             if s and all(c in _HEXDIGITS for c in s):
#                 hex_str = "0x" + s.lower()
#             else:
#                 hex_str = None

#         if hex_str is not None:
#             # If it's an address-length hex string, checksum it
#             if len(hex_str) == 42:  # 0x + 40 hex chars = 20 bytes

#                 return Web3.toChecksumAddress(hex_str)
#             return hex_str

#     # Case 2: BYTEA contains raw bytes; represent as Ethereum-style 0x + hex
#     hex_str = "0x" + b.hex()

#     # Only checksum raw-byte values that look like addresses (20 bytes)
#     if len(b) == 20:

#         return Web3.toChecksumAddress(hex_str)

#     return hex_str


# def normalize_bytea_in_df(df):
#     """
#     In-place-ish normalization for DataFrames coming from SQL reads.
#     Only touches object columns and only if they contain byte-like values.
#     """
#     if df is None or df.empty:
#         return df

#     obj_cols = [c for c in df.columns if df[c].dtype == "object"]
#     for c in obj_cols:
#         # Find a representative non-null
#         sample = next((x for x in df[c].values if x is not None), None)
#         if isinstance(sample, (memoryview, bytes, bytearray, HexBytes)):
#             df[c] = df[c].map(_bytea_to_0x)

#     return df


# def normalize_bytea_in_record(record: dict[str, Any]) -> dict[str, Any]:
#     """
#     Same idea for dict rows (if you ever use .mappings() or manual dict assembly).
#     """
#     return {k: _bytea_to_0x(v) for k, v in record.items()}


if __name__ == "__main__":

    from mainnet_launch.database.schema.full import Blocks

    all_blocks = get_full_table_as_orm(Blocks, Blocks.chain_id == 1)
    print(len(all_blocks))

    pass
