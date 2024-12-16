import sqlite3
from datetime import datetime, timezone

import pandas as pd

from mainnet_launch.constants import DB_DIR


db_file = DB_DIR / "autopool_dashboard.db"
TABLE_NAME_TO_LAST_UPDATED = "TABLE_NAME_TO_LAST_UPDATED"


def ensure_table_to_last_updated_exists():
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_TO_LAST_UPDATED} (
        table_name TEXT PRIMARY KEY,
        last_updated TEXT
    )
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)


# not sure the best place to put this, this works for now. It runs on import
ensure_table_to_last_updated_exists()


def get_last_updated(table_name: str) -> pd.Timestamp | None:
    """
    Returns the timestamp
    """
    select_query = f"""
        SELECT last_updated FROM {TABLE_NAME_TO_LAST_UPDATED}
        WHERE table_name = {table_name};
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(select_query, (table_name,))
        result = cursor.fetchone()
        if result is not None:
            return pd.to_datetime(result[0], utc=True)
        return None


def _add_new_rows_to_already_existing_table(df: pd.DataFrame, table_name: str, conn, cursor) -> None:
    df.to_sql("temp_table", conn, index=False, if_exists="fail")
    cursor.execute(
        f"""
    INSERT OR IGNORE INTO {table_name}
    SELECT * FROM temp_table
    """
    )
    cursor.execute(f"DROP TABLE temp_table")


def _verify_all_rows_in_df_are_properly_saved(df: pd.DataFrame, table_name: str, conn):
    # TODO: consider rewriting this in pure sql
    full_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    if "timestamp" in full_df.columns:
        full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], utc=True)

    df_is_a_subset_of_full_df = df.apply(tuple, axis=1).isin(full_df.apply(tuple, axis=1))
    if not df_is_a_subset_of_full_df.all():
        raise ValueError(
            f"Data inconsistency detected between the DataFrame and the table '{table_name}'. Rolling back."
        )


def _update_table_to_last_updated_table(table_name: str, conn, cursor):
    current_time = datetime.now(timezone.utc)
    upsert_query = f"""
    INSERT INTO {TABLE_NAME_TO_LAST_UPDATED} table_name, last_updated
    VALUES (?, ?)
    ON CONFLICT(table_name) DO UPDATE SET
        last_updated=excluded.last_updated
    """

    cursor.execute(upsert_query, (table_name, current_time))


def write_df_to_table(df: pd.DataFrame, table_name: str) -> None:
    # note this should be the only external method that writes new data to any table
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            df.to_sql(table_name, conn, index=False, if_exists="fail")

        else:
            _add_new_rows_to_already_existing_table(df, table_name, conn, cursor)

        _verify_all_rows_in_df_are_properly_saved(df, table_name, conn)
        _update_table_to_last_updated_table(table_name, conn, cursor)

        # TODO consider adding a check that there are no duplicate rows in the table, and raising an error if so
        # I don't think it should be needed, but it can't hurt to check


def load_table_if_exists(table_name: str, where_clause: str | None = None) -> pd.DataFrame | None:
    """
    Loads data from the specified table if it exists and applies the given WHERE clause.

    Parameters:
        table_name (str): Name of the table to load.
        where_clause (str): SQL WHERE clause to filter the data. example: f"autopool_eth_addr = '{auotpool.autopool_eth_addr}'"

    Returns:
        pd.DataFrame | None: DataFrame containing the queried data or None if the table doesn't exist.
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone() is not None
        if not table_exists:
            return None

        if where_clause is None:
            query = f"SELECT * FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"

        df = pd.read_sql_query(query, conn)

        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
