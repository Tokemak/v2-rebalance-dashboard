import sqlite3
import pandas as pd
from mainnet_launch.constants import DB_DIR


TX_HASH_TO_GAS_INFO_DB = DB_DIR / "tx_hash_to_gas_info.db"
MULTICALL_LOGS_DB = DB_DIR / "multicall_logs.db"


def _initialize_multicall_logs_db():
    """Initialize the multicall_logs table in the SQLite database"""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS multicall_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chain_id INTEGER,
                block INTEGER,
                calls TEXT,
                responses TEXT
            )
        """
        )
        conn.commit()


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


def _load_tx_hash_to_gas_info(hashes: list[str]) -> pd.DataFrame:
    """Load gas info from SQLite database for specified hashes and return as a DataFrame."""
    if len(hashes) == 0:
        # Return an empty DataFrame if no hashes are provided
        return pd.DataFrame(columns=["hash", "gas_price", "gas_used"])

    hashes = [h.lower() for h in hashes]

    placeholders = ",".join("?" for _ in hashes)
    query = f"SELECT * FROM gas_info WHERE hash IN ({placeholders})"

    with sqlite3.connect(TX_HASH_TO_GAS_INFO_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(query, hashes)
        rows = cursor.fetchall()

    gas_df = pd.DataFrame(rows, columns=["hash", "gas_price", "gas_used"])
    return gas_df


def _save_tx_hash_to_gas_info(gas_df: pd.DataFrame) -> None:
    """Save tx_hash_to_gas_info DataFrame to SQLite database without updating existing records."""
    if any([col not in gas_df.columns for col in ["hash", "gas_price", "gas_used"]]):
        raise ValueError(f"can't save gas_df because it does not have the correct columns {gas_df.columns=}")
    # Ensure 'hash' column is in lowercase
    gas_df["hash"] = gas_df["hash"].str.lower()
    gas_df["gas_price"] = gas_df["gas_price"].astype(int)
    gas_df["gas_used"] = gas_df["gas_used"].astype(int)

    data_to_insert = gas_df[["hash", "gas_price", "gas_used"]].values.tolist()

    with sqlite3.connect(TX_HASH_TO_GAS_INFO_DB) as conn:
        cursor = conn.cursor()
        cursor.executemany(
            "INSERT OR IGNORE INTO gas_info (hash, gas_price, gas_used) VALUES (?, ?, ?)",
            data_to_insert,
        )
        conn.commit()


def _initalize_all_databases():
    _initalize_tx_hash_to_gas_info_db()
    _initialize_multicall_logs_db()


if __name__ == "__main__":
    _initalize_all_databases()
