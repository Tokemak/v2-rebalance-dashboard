# helper methods to add data to dataframes

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import os

import pandas as pd
import json
from filelock import FileLock

from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.constants import eth_client, TX_HASH_TO_GAS_COSTS_PATH


LOCK_FILE = "tx_hash_to_gas_info.lock"


def add_timestamp_to_df_with_block_column(df: pd.DataFrame) -> pd.DataFrame:
    """Add the timestamp to the df at the index if block is in the columns"""
    if "block" not in df.columns:
        raise ValueError(f"block must be in {df.columns=}")
    if len(df) == 0:
        return df

    blocks = list(set(df["block"]))
    # calling with empty calls gets the block:timestamp
    block_and_timestamp_df = get_raw_state_by_blocks([], blocks, include_block_number=True).reset_index()
    df = pd.merge(df, block_and_timestamp_df, on="block", how="left")
    df.set_index("timestamp", inplace=True)
    return df


def _fetch_tx_hash_gas_info(tx_hash: str) -> dict[str, dict[str, int]]:
    tx_receipt = eth_client.eth.get_transaction_receipt(tx_hash)
    tx = eth_client.eth.get_transaction(tx_hash)
    gas_price, gas_used = tx["gasPrice"], tx_receipt["gasUsed"]
    gas_cost_in_eth = float(eth_client.fromWei(gas_price * gas_used, "ether"))
    return {str.lower(tx_hash): {"gas_price": gas_price, "gas_used": gas_used, "gas_cost_in_eth": gas_cost_in_eth}}


def _load_tx_hash_to_gas_info():
    """Load tx_hash_to_gas_info from disk, if available."""
    if not os.path.exists(TX_HASH_TO_GAS_COSTS_PATH):
        return {}

    # this should prevent multiple processes reading and writing to TX_HASH_TO_GAS_COSTS_PATH at the same time
    # (untested)
    with FileLock(LOCK_FILE):
        try:
            with open(TX_HASH_TO_GAS_COSTS_PATH, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            # if we want read from it for whatever reason then
            os.remove(TX_HASH_TO_GAS_COSTS_PATH)
            return {}


def _save_tx_hash_to_gas_info(data):
    """Save tx_hash_to_gas_info to disk in a thread-safe manner."""
    with FileLock(LOCK_FILE):
        with open(TX_HASH_TO_GAS_COSTS_PATH, "w") as f:
            json.dump(data, f)


def add_transaction_gas_info_to_df_with_tx_hash(df: pd.DataFrame) -> pd.DataFrame:
    """Add gas_price and gas_used and gas_cost_in_eth to"""
    if "hash" not in df.columns:
        raise ValueError(f"hash must be in {df.columns=}")
    if len(df) == 0:
        return df

    # Load existing tx_hash_to_gas_info data from disk
    tx_hash_to_gas_info = _load_tx_hash_to_gas_info()
    hashes_to_fetch = [h for h in df["hash"].unique() if h not in tx_hash_to_gas_info]

    # Fetch missing gas info only for unknown transaction hashes
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_tx_hash_gas_info, h): h for h in hashes_to_fetch}
        for future in as_completed(futures):
            new_tx_hash_to_gas_info = future.result()
            tx_hash_to_gas_info.update(new_tx_hash_to_gas_info)

    # Save updated tx_hash_to_gas_info back to disk
    _save_tx_hash_to_gas_info(tx_hash_to_gas_info)

    # Add gas info columns to the DataFrame
    df["gas_price"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_price"])
    df["gas_used"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_used"])
    df["gas_cost_in_eth"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_cost_in_eth"])
    return df


# def add_transaction_gas_info_to_df_with_tx_hash(df: pd.DataFrame) -> pd.DataFrame:
#     # replace other methods with this function when possible
#     if "hash" not in df.columns:
#         raise ValueError(f"hash must be in {df.columns=}")
#     if len(df) == 0:
#         return df

#     tx_hash_to_gas_info = {}
#     hashes_to_fetch = df["hash"].unique()

#     with ThreadPoolExecutor(max_workers=8) as executor:
#         futures = {executor.submit(_fetch_tx_hash_gas_info, h): h for h in hashes_to_fetch}
#         for future in as_completed(futures):
#             new_tx_hash_to_gas_info = future.result()
#             tx_hash_to_gas_info.update(new_tx_hash_to_gas_info)

#     df["gas_price"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_price"])
#     df["gas_used"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_used"])
#     df["gas_cost_in_eth"] = df["hash"].apply(lambda h: tx_hash_to_gas_info[h]["gas_cost_in_eth"])
#     return df
