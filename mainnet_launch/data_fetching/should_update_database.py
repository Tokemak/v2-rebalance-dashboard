import sqlite3
from datetime import datetime, timezone

import pandas as pd

from mainnet_launch.constants import DB_FILE


TABLE_NAME_TO_LAST_UPDATED = "TABLE_NAME_TO_LAST_UPDATED"


def ensure_table_to_last_updated_exists() -> None:
    """
    Ensures that the TABLE_NAME_TO_LAST_UPDATED table exists in the database.
    If it does not exist, the table is created with the appropriate schema.
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME_TO_LAST_UPDATED} (
        table_name TEXT PRIMARY KEY,
        last_updated_unix_timestamp INTEGER
    )
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_query)
            conn.commit()
            print(f"Table '{TABLE_NAME_TO_LAST_UPDATED}' is ready.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating the table: {e}")
        raise e


def get_timestamp_table_was_last_updated(table_name: str) -> None | pd.Timestamp:
    """
    Retrieves the last updated timestamp for a given table from the database.

    Parameters:
        table_name (str): The name of the table to query.

    Returns:
        Optional[pd.Timestamp]: The last updated timestamp as a pandas Timestamp in UTC,
                                or None if the table is not found.
    """
    select_query = f"""
        SELECT last_updated_unix_timestamp FROM {TABLE_NAME_TO_LAST_UPDATED}
        WHERE table_name = ?
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(select_query, (table_name,))
            result = cursor.fetchone()
            if result is not None:
                return pd.to_datetime(result[0], utc=True, unit="s")
            return None
    except sqlite3.Error as e:
        print(f"An error occurred while fetching the timestamp for '{table_name}': {e}")
        raise e


def write_timestamp_table_was_last_updated(table_name: str, cursor) -> None:
    """
    Inserts or updates the last updated Unix timestamp for a given table in the database.

    Parameters:
        table_name (str): The name of the table to update.
    """
    current_unix_time = int(datetime.now(timezone.utc).timestamp())
    upsert_query = f"""
    INSERT INTO {TABLE_NAME_TO_LAST_UPDATED} (table_name, last_updated_unix_timestamp)
    VALUES (?, ?)
    ON CONFLICT(table_name) DO UPDATE SET
        last_updated_unix_timestamp = excluded.last_updated_unix_timestamp
    """
    try:
        cursor.execute(upsert_query, (table_name, current_unix_time))
        print(f"Successfully updated '{table_name}' with timestamp {current_unix_time}.")
    except sqlite3.Error as e:
        print(f"An error occurred while updating '{table_name}': {e}")
        raise e


def should_update_table(table_name:str, max_latency: str = "6 hours") -> bool:
    current_time = datetime.now(timezone.utc)
    last_updated = get_timestamp_table_was_last_updated(table_name)

    if last_updated is None:
        return True

    return (current_time - last_updated) > pd.Timedelta(max_latency)    

def setup_database():
    ensure_table_to_last_updated_exists()


setup_database()