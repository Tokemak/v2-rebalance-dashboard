import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from mainnet_launch.constants import DB_DIR, Chain

TX_HASH_TO_GAS_INFO_DB = DB_DIR / "tx_hash_to_gas_info.db"


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

        cursor.execute(query, hashes)
        rows = cursor.fetchall()

    gas_df = pd.DataFrame(rows, columns=["hash", "gas_price", "gas_used"])
    return gas_df


def _save_tx_hash_to_gas_info(gas_df: pd.DataFrame):
    """Save tx_hash_to_gas_info DataFrame to SQLite database without updating existing records."""
    # Ensure 'hash' column is in lowercase
    gas_df["hash"] = gas_df["hash"].str.lower()
    # Explicitly cast data to appropriate types
    gas_df["gas_price"] = gas_df["gas_price"].astype(int)
    gas_df["gas_used"] = gas_df["gas_used"].astype(int)

    data_to_insert = gas_df[["hash", "gas_price", "gas_used"]].values.tolist()

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

        cursor.executemany(
            "INSERT OR IGNORE INTO gas_info (hash, gas_price, gas_used) VALUES (?, ?, ?)",
            data_to_insert,
        )
        conn.commit()
        print(f"inserted len(gas_df)={len(gas_df)}")


def _fetch_tx_hash_gas_info(tx_hash: str, chain: Chain) -> dict:
    tx_receipt = chain.client.eth.get_transaction_receipt(tx_hash)
    tx = chain.client.eth.get_transaction(tx_hash)
    gas_price = tx["gasPrice"]
    gas_used = tx_receipt["gasUsed"]

    return {
        "hash": tx_hash.lower(),
        "gas_price": int(gas_price),
        "gas_used": int(gas_used),
    }


def add_transaction_gas_info_to_df_with_tx_hash(df: pd.DataFrame, chain: Chain) -> pd.DataFrame:
    """Add gas_price and gas_used to the DataFrame."""
    if "hash" not in df.columns:
        raise ValueError(f"'hash' must be in {df.columns=}")
    if len(df) == 0:
        return df

    df["hash"] = df["hash"].str.lower()
    df_hashes = df["hash"].unique().tolist()

    existing_gas_info = _load_tx_hash_to_gas_info(df_hashes)

    existing_hashes = set(existing_gas_info["hash"])
    hashes_to_fetch = [h for h in df_hashes if h not in existing_hashes]

    fetched_data = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_tx_hash_gas_info, h, chain): h for h in hashes_to_fetch}
        for future in as_completed(futures):
            try:
                fetched_data.append(future.result())
            except Exception as e:
                print(f"Error fetching gas info for hash: {futures[future]} - {e}")

    if len(fetched_data) > 0:
        new_gas_info_df = pd.DataFrame(fetched_data)
        _save_tx_hash_to_gas_info(new_gas_info_df)
        updated_gas_info = pd.concat([existing_gas_info, new_gas_info_df], ignore_index=True)
    else:
        updated_gas_info = existing_gas_info

    df = df.merge(updated_gas_info, how="left", on="hash")
    print(df.columns)
    df["gas_price_in_eth"] = df.apply(lambda row: (row["gas_price"] * row["gas_used"]) / 1e18, axis=1)
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
    df = add_transaction_gas_info_to_df_with_tx_hash(df, ETH_CHAIN)
    print(df.head(1).values)
