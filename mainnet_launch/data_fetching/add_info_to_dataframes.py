import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from web3.exceptions import TransactionNotFound


from mainnet_launch.app.app_config import NUM_GAS_INFO_FETCHING_THREADS
from mainnet_launch.constants.constants import ChainData, ETH_CHAIN
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.database.database_operations import (
    run_read_only_query,
    write_dataframe_to_table,
    does_table_exist,
    load_table,
    DB_FILE,
)


TIMESTAMP_BLOCK_CHAIN_TABLE = "TIMESTAMP_BLOCK_CHAIN_TABLE"
TX_HASH_TO_GAS_INFO_TABLE = "TX_HASH_TO_GAS_INFO_TABLE"


def initialize_tx_hash_to_gas_info_db() -> None:
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TX_HASH_TO_GAS_INFO_TABLE} (
                hash TEXT PRIMARY KEY,
                gas_price INTEGER,
                gas_used INTEGER
            )
            """
        )

    print(f"Initialized table '{TX_HASH_TO_GAS_INFO_TABLE}'.")


def _load_tx_hash_to_gas_info(hashes: list[str]) -> pd.DataFrame:
    """Returns all gas info for the hashes if they exist"""
    if len(hashes) == 0:
        return pd.DataFrame(columns=["hash", "gas_price", "gas_used"])

    normalized_hashes = [h.lower() for h in hashes]

    placeholders = ",".join("?" for _ in normalized_hashes)
    where_clause = f"hash IN ({placeholders})"

    gas_df = load_table(table_name=TX_HASH_TO_GAS_INFO_TABLE, where_clause=where_clause, params=normalized_hashes)
    if gas_df.empty:
        return pd.DataFrame(columns=["hash", "gas_price", "gas_used"])

    return gas_df


def _fetch_tx_hash_gas_info(tx_hash: str, chain: ChainData) -> dict[str, int]:
    try:
        tx_receipt = chain.client.eth.get_transaction_receipt(tx_hash)
        tx = chain.client.eth.get_transaction(tx_hash)
        gas_price = tx["gasPrice"]
        gas_used = tx_receipt["gasUsed"]

        gas_info = {
            "hash": tx_hash.lower(),
            "gas_price": int(gas_price),
            "gas_used": int(gas_used),
        }

        return gas_info

    except TransactionNotFound:
        error_msg = f"Failed to find transaction {tx_hash} on {chain.name}."
        print(error_msg)
        raise TransactionNotFound(error_msg)
    except Exception as e:
        error_msg = f"An error occurred while fetching transaction {tx_hash} on {chain.name}: {e}"
        print(error_msg)
        raise Exception(error_msg) from e


def _fetch_missing_gas_costs(hashes_to_fetch: list[str], chain: ChainData) -> pd.DataFrame:
    if len(hashes_to_fetch) > 0:
        fetched_data = []
        with ThreadPoolExecutor(max_workers=NUM_GAS_INFO_FETCHING_THREADS) as executor:
            futures = {executor.submit(_fetch_tx_hash_gas_info, h, chain): h for h in hashes_to_fetch}
            for future in as_completed(futures):
                fetched_data.append(future.result())

        return pd.DataFrame(fetched_data)
    else:
        return pd.DataFrame(columns=["hash", "gas_price", "gas_used"])


def add_transaction_gas_info_to_df_with_tx_hash(df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    """Add gas_price and gas_used gasCostInETH to df"""

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"df is not a dataFrame when it should be a data frame {type(df)=}")

    # Drop existing gas-related columns if they exist
    gas_columns = ["gas_price", "gas_used", "gasCostInETH"]
    existing_gas_columns = [col for col in gas_columns if col in df.columns]
    if existing_gas_columns:
        df = df.drop(columns=existing_gas_columns)

    # Ensure 'hash' column exists
    if "hash" not in df.columns:
        raise ValueError(f"'hash' must be in the DataFrame columns. Current columns: {df.columns.tolist()}")

    if len(df) == 0:
        return df

    df_hashes = set(df["hash"].tolist())
    existing_gas_info = _load_tx_hash_to_gas_info(df_hashes)
    existing_hash_set = set(existing_gas_info["hash"])
    hashes_to_fetch = [h for h in df_hashes if h not in existing_hash_set]

    if hashes_to_fetch:
        new_gas_info_df = _fetch_missing_gas_costs(hashes_to_fetch, chain)
        write_dataframe_to_table(new_gas_info_df, TX_HASH_TO_GAS_INFO_TABLE)
        gas_cost_df = pd.concat([existing_gas_info, new_gas_info_df], axis=0)
    else:
        gas_cost_df = existing_gas_info

    gas_cost_df.drop_duplicates(inplace=True)
    df = df.reset_index(drop=True).merge(gas_cost_df, how="left", on="hash", validate="many_to_one")

    df["gasCostInETH"] = (df["gas_price"] * df["gas_used"]) / 1e18
    return df


def determine_blocks_not_on_disk(blocks, chain) -> list[int]:
    # consider rewriting in pure sql
    placeholders = ", ".join("?" for _ in blocks)
    params = blocks + [chain.name]

    query = f"SELECT block, chain FROM {TIMESTAMP_BLOCK_CHAIN_TABLE} WHERE block IN ({placeholders}) AND chain = ?"
    block_df = run_read_only_query(query, params=params)

    found_blocks = block_df["block"].astype(int).to_list()

    missing_blocks = [b for b in blocks if b not in found_blocks]
    return missing_blocks


def ensure_block_timestamp_chain_df_contains_all_wanted_blocks(blocks, chain):
    missing_blocks = determine_blocks_not_on_disk(blocks, chain)

    if len(missing_blocks) > 0:
        new_rows_df = get_raw_state_by_blocks([], missing_blocks, chain=chain, include_block_number=True).reset_index()
        new_rows_df["chain"] = chain.name
        write_dataframe_to_table(new_rows_df, TIMESTAMP_BLOCK_CHAIN_TABLE)

    now_missing_blocks = determine_blocks_not_on_disk(blocks, chain)
    if len(now_missing_blocks) != 0:
        raise ValueError("failed to save all needed blocks", now_missing_blocks, chain)


def load_blocks_timestamp_chain_df(blocks, chain) -> pd.DataFrame:
    placeholders = ", ".join("?" for _ in blocks)
    params = blocks + [chain.name]
    query = f"SELECT block, timestamp FROM {TIMESTAMP_BLOCK_CHAIN_TABLE} WHERE block IN ({placeholders}) AND chain = ?"
    block_timestamp_chain_df = run_read_only_query(query, params=params)
    return block_timestamp_chain_df


def add_timestamp_to_df_with_block_column(df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    """Add the timestamp to the df at the index if block is in the columns"""
    if "block" not in df.columns:
        raise ValueError(f"block must be in {df.columns=}")
    if len(df) == 0:
        df.index = pd.DatetimeIndex([], name="timestamp", tz="UTC")
        return df

    if not does_table_exist(TIMESTAMP_BLOCK_CHAIN_TABLE):
        # if the table does not exist, add an init rows
        new_rows_df = get_raw_state_by_blocks(
            [], [18_000_000], chain=ETH_CHAIN, include_block_number=True, semaphore_limits=(1, 1, 1)
        ).reset_index()
        new_rows_df["chain"] = chain.name
        write_dataframe_to_table(new_rows_df, TIMESTAMP_BLOCK_CHAIN_TABLE)

    blocks = list(set(df["block"]))
    ensure_block_timestamp_chain_df_contains_all_wanted_blocks(blocks, chain)
    block_and_timestamp_df = load_blocks_timestamp_chain_df(blocks, chain)

    df = pd.merge(df, block_and_timestamp_df, on="block", how="left")
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)
    df.set_index("timestamp", inplace=True)
    return df
