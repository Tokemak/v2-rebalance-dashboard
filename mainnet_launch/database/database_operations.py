import sqlite3
from typing import Optional

import pandas as pd

from mainnet_launch.constants import DB_FILE, AutopoolConstants, ChainData
from mainnet_launch.database.should_update_database import (
    write_timestamp_table_was_last_updated,
)

# TODO, make the timestamps cleaner by not handling them after reading the db but instead here.


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
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
    return df


def add_new_rows(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection) -> None:
    """
    Adds new rows from the DataFrame to an existing table using INSERT with NOT EXISTS to avoid duplicates.

    Parameters:
        df (pd.DataFrame): DataFrame containing new data to be inserted.
        table_name (str): Name of the target table.
        conn (sqlite3.Connection): SQLite database connection.
    """
    try:
        # Write the DataFrame to a temporary table
        df.to_sql("temp_table", conn, index=False, if_exists="replace", method="multi", chunksize=10_000)

        # Build the INSERT query using NOT EXISTS to prevent duplicates
        columns = ", ".join(df.columns)
        insert_query = f"""
        INSERT INTO {table_name} ({columns})
        SELECT {columns}
        FROM temp_table
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {table_name} 
            WHERE {" AND ".join([f"{table_name}.{col} = temp_table.{col}" for col in df.columns])}
        )
        """

        conn.execute(insert_query)
        conn.execute("DROP TABLE IF EXISTS temp_table")
        print(f"Inserted new rows into '{table_name}' while avoiding duplicates.")

    except (sqlite3.Error, OverflowError) as e:
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
        local_df = df.copy()

        if "timestamp" in full_df.columns:
            full_df["timestamp"] = pd.to_datetime(full_df["timestamp"], format="ISO8601", utc=True)
            local_df["timestamp"] = pd.to_datetime(local_df["timestamp"], format="ISO8601", utc=True)

        is_subset = local_df.apply(tuple, axis=1).isin(full_df.apply(tuple, axis=1)).all()
        if not is_subset:
            print(local_df.head())
            print(local_df.dtypes)
            print(local_df.shape)
            print(full_df.head())
            print(full_df.dtypes)
            print(full_df.shape)
            raise ValueError(f"Data inconsistency detected in table '{table_name}'")

        if full_df.duplicated().any():
            duplicate_rows = full_df[full_df.duplicated()]
            raise ValueError(f"Duplicate rows detected {table_name}':\n{duplicate_rows}")
    except sqlite3.Error as e:
        print(f"Error verifying rows in table '{table_name}': {e}")
        raise


def does_table_exist(table_name: str) -> bool:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        )
        table_exists = cursor.fetchone() is not None
        return table_exists


def write_dataframe_to_table(df: pd.DataFrame, table_name: str, verify_data_stored_properly: bool = True) -> None:
    """
    Writes a DataFrame to the specified table in the database.
    If the table does not exist, it is created. Otherwise, new rows are added.

    Parameters:
        df (pd.DataFrame): DataFrame containing data to be written.
        table_name (str): Name of the target table.
    """
    if df is None:
        raise ValueError("df cannot be None")

    table_exists = does_table_exist(table_name)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            df_to_write = convert_timestamps_to_iso(df)

            if not table_exists:
                df_to_write.to_sql(table_name, conn, index=False, if_exists="fail", method="multi", chunksize=10_000)
                print(f"Table '{table_name}' created and data inserted.")
            else:
                add_new_rows(df_to_write, table_name, conn)
                print(f"Table '{table_name}'already exists and data inserted.")

            if verify_data_stored_properly:
                verify_rows_saved(df_to_write, table_name, conn)

            cursor = conn.cursor()
            write_timestamp_table_was_last_updated(table_name, cursor)

    except sqlite3.Error as e:
        print(f"Database error during write operation on table '{table_name}': {e}")
        raise
    except ValueError as ve:
        print(ve)
        raise


def load_table(table_name: str, where_clause: Optional[str] = None, params=None) -> Optional[pd.DataFrame]:
    """
    Loads data from the specified table if it exists and applies the given WHERE clause.

    Parameters:
        table_name (str): Name of the table to load.
        where_clause (Optional[str]): SQL WHERE clause to filter the data.
                                      Example: "autopool_eth_addr = '0x123...'"

    Returns:
        Optional[pd.DataFrame]: DataFrame containing the queried data or None if the table doesn't exist.
    """
    table_exists = does_table_exist(table_name)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            if not table_exists:
                print(f"Table '{table_name}' does not exist.")
                return None

            base_query = f"SELECT * FROM {table_name}"
            if where_clause:
                query = f"{base_query} WHERE {where_clause}"
            else:
                query = base_query

            df = pd.read_sql_query(query, conn, params)

            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)

            print(f"Data loaded from table '{table_name}'.")
            return df

    except sqlite3.Error as e:
        print(f"Error loading data from table '{table_name}': {e}")
        raise e


def run_read_only_query(query: str, params: tuple | None) -> pd.DataFrame:
    with sqlite3.connect(DB_FILE) as conn:
        df = pd.read_sql_query(query, conn, params=params)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)

        if df.duplicated().any():
            duplicate_rows = df[df.duplicated()]
            raise ValueError(f"Duplicate rows read from db {query=}':\n{duplicate_rows=}")

        return df


def get_earliest_block_from_table_with_autopool(table_name: str, autopool: AutopoolConstants) -> int:
    if does_table_exist(table_name):
        query = f"""
        SELECT max(block) as highest_found_block from {table_name}
        
        WHERE autopool = ?
        
        """
        params = (autopool.name,)
        df = run_read_only_query(query, params)

        possible_highest_block = df["highest_found_block"].values[0]
        if possible_highest_block is None:
            return autopool.chain.block_autopool_first_deployed
        else:
            return int(df["highest_found_block"].values[0])
    else:
        return autopool.chain.block_autopool_first_deployed


def get_earliest_block_from_table_with_chain(table_name: str, chain: ChainData) -> int:
    if does_table_exist(table_name):
        query = f"""
        SELECT max(block) as highest_found_block from {table_name}
        
        WHERE chain = ?
        
        """
        params = (chain.name,)
        df = run_read_only_query(query, params)

        possible_highest_block = df["highest_found_block"].values[0]
        if possible_highest_block is None:
            return chain.block_autopool_first_deployed
        else:
            return int(df["highest_found_block"].values[0])
    else:
        return chain.block_autopool_first_deployed


def get_all_rows_in_table_by_autopool(table_name: str, autopool: AutopoolConstants) -> pd.DataFrame:
    params = (autopool.name,)

    query = f"""
    
    SELECT * from {table_name}
    
    WHERE autopool = ?
    
    """

    df = run_read_only_query(query, params)
    if "timestamp" in df.columns:
        df = df.set_index("timestamp")
    return df


def get_all_rows_in_table_by_chain(table_name: str, chain: ChainData) -> pd.DataFrame:
    params = (chain.name,)

    query = f"""
    
    SELECT * from {table_name}
    
    WHERE chain = ?
    
    """

    df = run_read_only_query(query, params)
    if "timestamp" in df.columns:
        df = df.set_index("timestamp")
    return df



def drop_table(table_name: str) -> None:
    """
    Drops a table from the database if it exists.

    Parameters:
        table_name (str): Name of the table to be dropped.
    """
    if not table_name:
        raise ValueError("table_name cannot be None or empty")

    try:
        with sqlite3.connect(DB_FILE) as conn:
            drop_query = f"DROP TABLE IF EXISTS {table_name}"
            conn.execute(drop_query)
            print(f"Table '{table_name}' dropped successfully.")
    except sqlite3.Error as e:
        print(f"Database error while dropping table '{table_name}': {e}")
        raise


# if __name__ == '__main__':
#     drop_table('ASSET_DISCOUNT_TABLE')