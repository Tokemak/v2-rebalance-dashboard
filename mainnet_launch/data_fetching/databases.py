import sqlite3
import pickle
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


def write_df_to_table(df: pd.DataFrame, table_name: str) -> None:
    with sqlite3.connect(db_file) as conn:
        cursor = conn.cursor()

        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            # Create table if it doesn't exist
            df.to_sql(table_name, conn, index=False, if_exists="replace")
            print(f"Table '{table_name}' created and data inserted.")
        else:
            # Load existing table into a DataFrame
            existing_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)

            # Find rows in df not in existing_df
            new_rows = df[~df.apply(tuple, axis=1).isin(existing_df.apply(tuple, axis=1))]

            if not new_rows.empty:
                # Append new rows to the table
                new_rows.to_sql(table_name, conn, index=False, if_exists="append")
                print(f"Added {len(new_rows)} new rows to '{table_name}'.")
            else:
                print(f"No new rows to add to '{table_name}'.")

        full_df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        if "timestamp" in full_df.columns:
            full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], utc=True)

        df_is_a_subset_of_full_df = df.apply(tuple, axis=1).isin(full_df.apply(tuple, axis=1))

        if not df_is_a_subset_of_full_df.all():
            raise ValueError(
                f"Data inconsistency detected between the DataFrame and the table '{table_name}'. Rolling back."
            )
        else:
            print(f"Data consistency verified for table '{table_name}'.")


def load_table_if_exists(table_name: str, where_clause: str | None) -> pd.DataFrame | None:
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


def _initalize_all_databases():
    _initalize_tx_hash_to_gas_info_db()
    _initialize_multicall_hash_response_db()


if __name__ == "__main__":
    _initalize_all_databases()
