from datetime import datetime, timedelta, timezone

import pandas as pd
import os
import requests
import threading
import time

from mainnet_launch.database.schema.full import Blocks
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
)
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks, build_blocks_to_use

from mainnet_launch.constants import ALL_CHAINS, ChainData


def add_blocks_from_dataframe_to_database(df: pd.DataFrame):
    if df.index.name == "timestamp":
        df["datetime"] = df.index

    required_columns = {"datetime", "block", "chain_id"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")

    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    new_rows = [Blocks.from_record(r) for r in df.to_dict(orient="records")]
    insert_avoid_conflicts(new_rows, Blocks, [Blocks.block, Blocks.chain_id])


def build_last_second_of_each_day_since_inception(chain: ChainData):
    start_datetime = (
        datetime.fromtimestamp(chain.start_unix_timestamp, tz=timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        - timedelta(seconds=1)
        + timedelta(days=1)
    )  # when autoETH was timedelta
    last_second_of_yesterday = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(seconds=1)

    last_second_of_each_day_since_inception = [start_datetime]

    while last_second_of_each_day_since_inception[-1] < last_second_of_yesterday:
        start_datetime += timedelta(days=1)
        last_second_of_each_day_since_inception.append(start_datetime)

    return last_second_of_each_day_since_inception


def determine_what_days_have_highest_block_found(chain: ChainData) -> pd.DataFrame:
    # I still think this assumes we will previously have a block, timestmap of each day
    blocks = build_blocks_to_use(chain)
    if not blocks:
        return pd.DataFrame(columns=["date"])
    block_after = [b + 1 for b in blocks]

    df_before = get_raw_state_by_blocks([], blocks, chain, include_block_number=True).reset_index()
    df_before["date"] = df_before["timestamp"].dt.date
    df_after = get_raw_state_by_blocks([], block_after, chain, include_block_number=True).reset_index()
    df_after["date"] = df_after["timestamp"].dt.date

    days_with_highest_block_found_df = df_before[df_before["date"].values != df_after["date"].values].copy()
    return days_with_highest_block_found_df


def ensure_blocks_is_current():
    """Make sure that we have a the highest block for each day (UTC) since Sep-10-2024 (when autoETH was deployed)"""

    for chain in ALL_CHAINS:
        last_second_of_each_day_since_inception = build_last_second_of_each_day_since_inception(chain)
        days_with_highest_block_found_df = determine_what_days_have_highest_block_found(chain)

        existing_dates = set(days_with_highest_block_found_df["date"])

        seconds_to_get = [
            int(dt.timestamp()) for dt in last_second_of_each_day_since_inception if dt.date() not in existing_dates
        ]
        highest_block_of_each_day_to_add = []
        for last_second_of_day in seconds_to_get:
            block_before = get_block_by_timestamp_etherscan(last_second_of_day, chain, closest="before")
            highest_block_of_each_day_to_add.append(block_before)

        ensure_all_blocks_are_in_table(highest_block_of_each_day_to_add, chain)


_etherscan_semaphore = threading.BoundedSemaphore(5)


def get_block_by_timestamp_etherscan(unix_timestamp: int, chain: ChainData, closest: str) -> int:
    """
    Fetch the first block after the given UNIX timestamp
    using Etherscan's getblocknobytime endpoint.
    """
    with _etherscan_semaphore:
        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": str(unix_timestamp),
            "closest": closest,
            "chainid": str(chain.chain_id),
            "apikey": os.getenv("ETHERSCAN_API_KEY"),
        }
        for i in range(4):
            try:
                resp = requests.get("https://api.etherscan.io/v2/api", params=params)
                block = int(resp.json()["result"])
                return block
            except Exception as e:
                if i < 3:
                    time.sleep(1**i)
                else:
                    raise e


def ensure_all_blocks_are_in_table(blocks: list[int], chain: ChainData) -> None:
    blocks_to_add = get_subset_not_already_in_column(
        table=Blocks, column=Blocks.block, values=blocks, where_clause=Blocks.chain_id == chain.chain_id
    )
    if blocks_to_add:
        df = get_raw_state_by_blocks([], blocks, chain, include_block_number=True)
        df["chain_id"] = chain.chain_id
        add_blocks_from_dataframe_to_database(df)


if __name__ == "__main__":
    ensure_blocks_is_current()
