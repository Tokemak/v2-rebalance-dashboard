import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from web3.exceptions import TransactionNotFound

from mainnet_launch.constants import ChainData, ETH_CHAIN
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.data_fetching.databases import TX_HASH_TO_GAS_INFO_DB


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
    gas_df["gas_price"] = gas_df["gas_price"].astype(int)
    gas_df["gas_used"] = gas_df["gas_used"].astype(int)
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


def _fetch_tx_hash_gas_info(tx_hash: str, chain: ChainData) -> dict:
    try:
        tx_receipt = chain.client.eth.get_transaction_receipt(tx_hash)
        tx = chain.client.eth.get_transaction(tx_hash)
        gas_price = tx["gasPrice"]
        gas_used = tx_receipt["gasUsed"]

        return {
            "hash": tx_hash.lower(),
            "gas_price": int(gas_price),
            "gas_used": int(gas_used),
        }
    except TransactionNotFound:
        raise TransactionNotFound(f"Failed to find transaction {tx_hash} on {chain.name}")


def fetch_missing_gas_costs(hashes_to_fetch: list[str], chain: ChainData) -> pd.DataFrame:
    if len(hashes_to_fetch) > 0:
        fetched_data = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(_fetch_tx_hash_gas_info, h, chain): h for h in hashes_to_fetch}
            for future in as_completed(futures):
                fetched_data.append(future.result())

        return pd.DataFrame(fetched_data)
    else:
        return pd.DataFrame(columns=["hash", "gas_price", "gas_used"])


def add_transaction_gas_info_to_df_with_tx_hash(df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    """Add gas_price and gas_used to the DataFrame."""
    if ("gas_price" in df.columns) or ("gas_used" in df.columns) or ("gasCostInETH" in df.columns):
        # if there are already gas columns here, feel free to drop them
        df.drop(columns=["gas_price", "gas_used", "gasCostInETH"], inplace=True)

    if "hash" not in df.columns:
        raise ValueError(f"'hash' must be in {df.columns=}")

    if len(df) == 0:
        return df

    df["hash"] = df["hash"].str.lower()
    df_hashes = df["hash"].unique().tolist()
    existing_gas_info = _load_tx_hash_to_gas_info(df_hashes)
    hashes_to_fetch = [h for h in df_hashes if h not in existing_gas_info["hash"]]
    new_gas_info_df = fetch_missing_gas_costs(hashes_to_fetch, chain)

    _save_tx_hash_to_gas_info(new_gas_info_df)
    gas_cost_df = pd.concat([existing_gas_info, new_gas_info_df], ignore_index=True)
    try:
        df = df.merge(gas_cost_df, how="left", on=["hash"])
        df["gasCostInETH"] = df.apply(lambda row: (row["gas_price"] * row["gas_used"]) / 1e18, axis=1)
    except Exception as e:
        print(e)
        print(df.head())
        print(df.shape, df.columns)
        print(gas_cost_df.shape)
        print(gas_cost_df.columns)

        pass
    return df


def add_timestamp_to_df_with_block_column(df: pd.DataFrame, chain: ChainData) -> pd.DataFrame:
    """Add the timestamp to the df at the index if block is in the columns"""
    if "block" not in df.columns:
        raise ValueError(f"block must be in {df.columns=}")
    if len(df) == 0:
        return df

    blocks = list(set(df["block"]))
    # calling with empty calls gets the block:timestamp
    block_and_timestamp_df = get_raw_state_by_blocks([], blocks, chain=chain, include_block_number=True).reset_index()
    df = pd.merge(df, block_and_timestamp_df, on="block", how="left")
    df.set_index("timestamp", inplace=True)
    return df


if __name__ == "__main__":
    from mainnet_launch.data_fetching.get_events import get_each_event_in_contract, fetch_events
    from mainnet_launch.constants import DESTINATION_VAULT_REGISTRY, ETH_CHAIN, BASE_CHAIN
    from mainnet_launch.abis.abis import DESTINATION_VAULT_REGISTRY_ABI

    eth_contract = ETH_CHAIN.client.eth.contract(
        DESTINATION_VAULT_REGISTRY(ETH_CHAIN), abi=DESTINATION_VAULT_REGISTRY_ABI
    )
    df = fetch_events(eth_contract.events.DestinationVaultRegistered)
    print(df.head())
    df = add_transaction_gas_info_to_df_with_tx_hash(df, ETH_CHAIN)
    print(df.head(1).values)

    base_contract = BASE_CHAIN.client.eth.contract(
        DESTINATION_VAULT_REGISTRY(BASE_CHAIN), abi=DESTINATION_VAULT_REGISTRY_ABI
    )
    df = fetch_events(base_contract.events.DestinationVaultRegistered)
    print(df.head())
    df = add_transaction_gas_info_to_df_with_tx_hash(df, BASE_CHAIN)
    print(df.head(1).values)
