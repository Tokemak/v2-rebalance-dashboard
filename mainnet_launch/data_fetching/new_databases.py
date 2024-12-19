import sqlite3
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from mainnet_launch.constants import DB_FILE, time_decorator
from mainnet_launch.data_fetching.should_update_database import (
    write_timestamp_table_was_last_updated,
    ensure_table_to_last_updated_exists,
)


def convert_timestamps_to_iso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts the 'timestamp' column in the DataFrame to ISO8601 string format.

    Parameters:
        df (pd.DataFrame): DataFrame containing a 'timestamp' column.

    Returns:
        pd.DataFrame: DataFrame with 'timestamp' column converted to strings.
    """
    if "timestamp" in df.columns:
        df = df.copy()  # To avoid SettingWithCopyWarning
        df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


def add_new_rows(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection) -> None:
    """
    Adds new rows from the DataFrame to an existing table using INSERT OR IGNORE.

    Parameters:
        df (pd.DataFrame): DataFrame containing new data to be inserted.
        table_name (str): Name of the target table.
        conn (sqlite3.Connection): SQLite database connection.
    """
    try:
        # Convert 'timestamp' to ISO8601 format before writing
        df_to_insert = convert_timestamps_to_iso(df)

        df_to_insert.to_sql("temp_table", conn, index=False, if_exists="replace", method="multi", chunksize=10_000)
        insert_query = f"""
        INSERT OR IGNORE INTO {table_name}
        SELECT * FROM temp_table
        """
        conn.execute(insert_query)
        conn.commit()
        print(f"Inserted new rows into '{table_name}'.")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"Error inserting new rows into '{table_name}': {e}")
        raise


def verify_rows_saved(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection) -> None:
    """
    Verifies that all rows in the DataFrame have been successfully saved to the table.

    Parameters:
        df (pd.DataFrame): Original DataFrame containing data to be verified.
        table_name (str): Name of the target table.
        conn (sqlite3.Connection): SQLite database connection.

    Raises:
        ValueError: If any row in the DataFrame is not found in the table.
    """
    try:
        query = f"SELECT * FROM {table_name}"
        full_df = pd.read_sql_query(query, conn)

        if "timestamp" in full_df.columns:
            # Parse 'timestamp' back to datetime
            full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], format="%Y-%m-%d %H:%M:%S", utc=True)

        # Ensure the original DataFrame has 'timestamp' as datetime for accurate comparison
        if "timestamp" in df.columns:
            df = df.copy()
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Convert both DataFrames to sets of tuples for efficient comparison
        df_tuples = set([tuple(row) for row in df.itertuples(index=False, name=None)])
        full_df_tuples = set([tuple(row) for row in full_df.itertuples(index=False, name=None)])

        missing_rows = df_tuples - full_df_tuples
        if missing_rows:
            raise ValueError(f"Data inconsistency detected in table '{table_name}'. Missing rows: {missing_rows}")
        print(f"All rows from DataFrame are successfully saved to '{table_name}'.")
    except sqlite3.Error as e:
        print(f"Error verifying rows in table '{table_name}': {e}")
        raise


@time_decorator
def write_dataframe_to_table(df: pd.DataFrame, table_name: str, verify_data_stored_properly: bool = True) -> None:
    """
    Writes a DataFrame to the specified table in the database.
    If the table does not exist, it is created. Otherwise, new rows are added.

    Parameters:
        df (pd.DataFrame): DataFrame containing data to be written.
        table_name (str): Name of the target table.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            # Check if the target table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # Convert 'timestamp' to ISO8601 format before writing
                df_to_write = convert_timestamps_to_iso(df)

                # Create the table and insert data
                df_to_write.to_sql(table_name, conn, index=False, if_exists="fail", method="multi", chunksize=10_000)
                print(f"Table '{table_name}' created and data inserted.")
            else:
                # Insert new rows into the existing table
                add_new_rows(df, table_name, conn)

            if verify_data_stored_properly:
                # Verify that all rows are saved
                verify_rows_saved(df, table_name, conn)

            # Update the last updated timestamp
            write_timestamp_table_was_last_updated(table_name, cursor)

    except sqlite3.Error as e:
        print(f"Database error during write operation on table '{table_name}': {e}")
        raise
    except ValueError as ve:
        print(ve)
        raise


@time_decorator
def load_table(table_name: str, where_clause: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Loads data from the specified table if it exists and applies the given WHERE clause.

    Parameters:
        table_name (str): Name of the table to load.
        where_clause (Optional[str]): SQL WHERE clause to filter the data.
                                      Example: "autopool_eth_addr = '0x123...'"

    Returns:
        Optional[pd.DataFrame]: DataFrame containing the queried data or None if the table doesn't exist.
    """
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            # Check if the table exists
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            )
            if cursor.fetchone() is None:
                print(f"Table '{table_name}' does not exist.")
                return None

            # Construct the query with optional WHERE clause
            base_query = f"SELECT * FROM {table_name}"
            if where_clause:
                query = f"{base_query} WHERE {where_clause}"
            else:
                query = base_query

            df = pd.read_sql_query(query, conn)

            # Convert 'timestamp' column to datetime if it exists
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", utc=True)

            print(f"Data loaded from table '{table_name}'.")
            return df

    except sqlite3.Error as e:
        print(f"Error loading data from table '{table_name}': {e}")
        raise e


def run_query(query, params):
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(query, conn, params=params)
        return df
