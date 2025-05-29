from datetime import datetime, timedelta, timezone
import time
from urllib.parse import urlparse
import random

import pandas as pd
import os
import requests

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
            block_before = get_block_after_timestamp_from_alchemy(last_second_of_day, chain, direction="BEFORE")
            highest_block_of_each_day_to_add.append(block_before)

        ensure_all_blocks_are_in_table(highest_block_of_each_day_to_add, chain)


def get_block_after_timestamp_from_alchemy(
    unix_timestamp: int,
    chain: ChainData,
    direction: str = "AFTER",  # or BEFORE
) -> int:
    """
    Fetch the first block before or after the given UNIX timestamp on chain
    """
    if not isinstance(unix_timestamp, int):
        raise TypeError(f"{unix_timestamp=} should be an integer")
    rpc_url = os.environ["ALCHEMY_URL"]
    parsed = urlparse(rpc_url)
    api_key = parsed.path.rsplit("/", 1)[-1]

    endpoint = f"https://api.g.alchemy.com/data/v1/{api_key}/utility/blocks/by-timestamp"

    headers = {"Authorization": api_key}  #

    params = {
        "networks": [chain.name + "-mainnet"],
        "timestamp": str(unix_timestamp),
        "direction": direction.upper(),  # “BEFORE” or “AFTER”
    }

    headers = {"Authorization": "<Authorization>"}
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(endpoint, headers=headers, params=params, timeout=(5, 15))
            resp.raise_for_status()
            return int(resp.json()["data"][0]["block"]["number"])
        except Exception as e:
            if attempt == max_retries:
                raise e
            delay = 2 ** (attempt)
            print(f"[Attempt {attempt}/{max_retries}] Error: {e!r}. Retrying in {delay:.1f}s…")
            time.sleep(delay + random.uniform(0, 1))


def ensure_all_blocks_are_in_table(blocks: list[int], chain: ChainData) -> list[Blocks]:
    blocks_to_add = get_subset_not_already_in_column(
        table=Blocks, column=Blocks.block, values=blocks, where_clause=Blocks.chain_id == chain.chain_id
    )
    if blocks_to_add:
        df = get_raw_state_by_blocks([], blocks, chain, include_block_number=True)
        df["chain_id"] = chain.chain_id
        add_blocks_from_dataframe_to_database(df)


if __name__ == "__main__":
    ensure_blocks_is_current()
