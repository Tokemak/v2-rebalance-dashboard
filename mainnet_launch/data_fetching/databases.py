import sqlite3
from mainnet_launch.constants import DB_DIR


TX_HASH_TO_GAS_INFO_DB = DB_DIR / "tx_hash_to_gas_info.db"
MULTICALL_LOGS_DB = DB_DIR / "multicall_logs.db"


def _initialize_multicall_hash_response_db():
    """Initialize the multicall_logs table in the SQLite database."""
    with sqlite3.connect(MULTICALL_LOGS_DB) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS multicall_logs (
                multicall_hash TEXT PRIMARY KEY,   -- SHA256 hash stored as a string
                response TEXT                      -- JSON string to store the response dictionary
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


def _initalize_all_databases():
    _initalize_tx_hash_to_gas_info_db()
    _initialize_multicall_hash_response_db()


if __name__ == "__main__":
    _initalize_all_databases()
