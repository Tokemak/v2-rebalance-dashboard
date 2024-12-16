import sqlite3
import pickle
from datetime import datetime, timezone

import pandas as pd
from multicall import Multicall

from mainnet_launch.constants import DB_DIR


TX_HASH_TO_GAS_INFO_DB = DB_DIR / "tx_hash_to_gas_info.db"
MULTICALL_LOGS_DB = DB_DIR / "multicall_logs.db"


def batch_insert_multicall_logs(db_hash_to_multicall_and_response, highest_finalized_block: int):

    # all responses should not be None

    hashes_to_insert = []
    responses_to_insert = []
    for db_hash, multicall_and_response in db_hash_to_multicall_and_response.items():

        if multicall_and_response["multicall"].block_id < highest_finalized_block:
            hashes_to_insert.append(db_hash)
            responses_to_insert.append(multicall_and_response["response"])

    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT OR REPLACE INTO multicall_logs (multicall_hash, response)
            VALUES (?, ?)
            """,
            zip(hashes_to_insert, (pickle.dumps(response) for response in responses_to_insert)),
        )
        conn.commit()
        print(f"successfully wrote { len(hashes_to_insert)= }")


def batch_load_multicall_logs_if_exists(
    db_hash_to_multicall_and_response: dict[str, list[Multicall, None]]
) -> dict[str, list[Multicall, None]]:

    db_hashes_to_fetch = list(db_hash_to_multicall_and_response.keys())

    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        placeholders = ",".join(["?"] * len(db_hashes_to_fetch))
        query = f"SELECT multicall_hash, response FROM multicall_logs WHERE multicall_hash IN ({placeholders})"
        cursor.execute(query, db_hashes_to_fetch)
        rows = cursor.fetchall()
        print(f"successfully read{ len(rows)= } of {len(db_hashes_to_fetch)=}")

    # only valid jsons should be here so we should fail on trying to load one that does not work
    cached_hash_to_response = {row[0]: pickle.loads((row[1])) for row in rows}

    for db_hash_found_cached, cached_response in cached_hash_to_response.items():
        db_hash_to_multicall_and_response[db_hash_found_cached]["response"] = cached_response

    # returns a dict of (dbHash:[Multicall, response | None])
    return db_hash_to_multicall_and_response


def _initialize_multicall_hash_response_db():
    """Initialize the multicall_logs table in the SQLite database."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS multicall_logs (
                multicall_hash TEXT PRIMARY KEY, 
                response TEXT                      
            )
            """
        )
        conn.commit()


# TODO move gas caching logic here?


def _initalize_tx_hash_to_gas_info_db():
    with sqlite3.connect(TX_HASH_TO_GAS_INFO_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gas_info (
                hash TEXT PRIMARY KEY,
                gas_price INTEGER,
                gas_used INTEGER
            )
            """
        )
        conn.commit()


db_file = DB_DIR / "autopool_dashboard.db"
TABLE_NAME_TO_LAST_UPDATED = "TABLE_NAME_TO_LAST_UPDATED"


def _create_new_table_if_it_does_not_exist(df: pd.DataFrame, table_name: str, conn):
    df.to_sql(table_name, conn, index=False, if_exists="fail")
    print(f"Table '{table_name}' Does not exist so it is created and data inserted.")


def _add_new_rows_to_already_existing_table(df: pd.DataFrame, table_name: str, conn, cursor):
    temp_table_name = "temp_table"
    df.to_sql(temp_table_name, conn, index=False, if_exists="append")
    cursor.execute(
        f"""
    INSERT OR IGNORE INTO {table_name}
    SELECT * FROM {temp_table_name}
    """
    )
    cursor.execute(f"DROP TABLE {temp_table_name}")


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


def write_df_to_table(df: pd.DataFrame, table_name: str) -> None:
    # note this should be the only external method that writes new data to any table
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            _create_new_table_if_it_does_not_exist(df, table_name, conn)

        else:
            _add_new_rows_to_already_existing_table(df, table_name, conn, cursor)

        _verify_all_rows_in_df_are_properly_saved(df, table_name, conn)
        update_last_updated(table_name)
        # TODO consider adding a check that there are no duplicate rows in the table, and raising an error if so


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


def run_query(query: str):
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()


# Table and column names
TABLE_NAME_TO_LAST_UPDATED = "TABLE_NAME_TO_LAST_UPDATED"
TABLE_NAME_COLUMN = "table_name"
LAST_UPDATED_COLUMN = "last_updated"


def create_last_updated_table():
    """Creates the TABLE_NAME_TO_LAST_UPDATED table if it doesn't exist."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_TO_LAST_UPDATED} (
        {TABLE_NAME_COLUMN} TEXT PRIMARY KEY,
        {LAST_UPDATED_COLUMN} TEXT
    );
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()


def update_last_updated(table_name: str):
    """
    Updates the last_updated timestamp for the given table name.
    If the table name does not exist in the table, it inserts a new record.

    Args:
        table_name (str): The name of the table to update.
    """
    current_time = datetime.now(timezone.utc)
    upsert_query = f"""
    INSERT INTO {TABLE_NAME_TO_LAST_UPDATED} ({TABLE_NAME_COLUMN}, {LAST_UPDATED_COLUMN})
    VALUES (?, ?)
    ON CONFLICT({TABLE_NAME_COLUMN}) DO UPDATE SET
        {LAST_UPDATED_COLUMN}=excluded.{LAST_UPDATED_COLUMN};
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(upsert_query, (table_name, current_time))
        conn.commit()


def get_last_updated(table_name: str):
    """
    Retrieves the last_updated UTC timestamp for the given table name.

    Args:
        table_name (str): The name of the table to query.

    Returns:
        Optional[str]: The last updated timestamp in ISO 8601 format, or None if not found.
    """
    select_query = f"""
    SELECT {LAST_UPDATED_COLUMN} FROM {TABLE_NAME_TO_LAST_UPDATED}
    WHERE {TABLE_NAME_COLUMN} = ?;
    """
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()
        cursor.execute(select_query, (table_name,))
        result = cursor.fetchone()
        if result:
            return pd.to_datetime(result[0], utc=True)  # Return the last_updated timestamp
        return None


def _initalize_all_databases():
    _initalize_tx_hash_to_gas_info_db()
    _initialize_multicall_hash_response_db()
    create_last_updated_table()


_initalize_all_databases()
# on import ensure the tables exist

if __name__ == "__main__":
    _initalize_all_databases()
