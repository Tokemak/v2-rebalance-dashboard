import sqlite3
import json

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
            zip(hashes_to_insert, (json.dumps(response) for response in responses_to_insert)),
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
    cached_hash_to_response = {row[0]: json.loads((row[1])) for row in rows}

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


def _initalize_all_databases():
    _initalize_tx_hash_to_gas_info_db()
    _initialize_multicall_hash_response_db()


if __name__ == "__main__":
    _initalize_all_databases()
