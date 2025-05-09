from datetime import datetime, timedelta, timezone
import time


import pandas as pd
import os
import requests
from dotenv import load_dotenv
import numpy as np

from mainnet_launch.database.schema.full import Blocks
from mainnet_launch.database.schema.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    get_highest_value_in_field_where,
)
from mainnet_launch.constants import ALL_CHAINS, ChainData


load_dotenv()


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


def _fetch_block_df_from_subgraph(
    chain: ChainData,
    timestamps: list[int] | None = None,
    page_size: int = 1000,
) -> pd.DataFrame:
    timestamps_less_15_seconds = []
    for t in timestamps:
        for i in range(40):  # there should be a block at least in the 20 seconds prior to end of timestamp
            timestamps_less_15_seconds.append(t - i)

    match chain.name:
        case "eth":
            url = os.environ["mainnet_blocks_subgraph"]
        case "base":
            url = os.environ["base_blocks_subgraph"]
        case _:
            raise ValueError(f"Unsupported chain: {chain.name}")

    records: list[dict] = []

    groups = np.array_split(
        timestamps_less_15_seconds,
        (len(timestamps_less_15_seconds) // min(len(timestamps_less_15_seconds), page_size)) + 1,
    )
    for timestamps in groups:

        ts_filter = f"timestamp_in: [{','.join(map(str, timestamps))}]"
        where_clause = f"where: {{{ts_filter}}}"

        query = f"""
        {{
          blocks(first:{page_size}, {where_clause}
                 orderBy:number, orderDirection:desc) {{
            number
            timestamp
          }}
        }}"""
        r = requests.post(url, json={"query": query}, timeout=15)
        r.raise_for_status()
        page = r.json()["data"]["blocks"]
        records.extend(page)
        time.sleep(0.2)  # don't overwhelm API.  Not certain if needed
        # print(len(records))

    df = pd.DataFrame.from_records(records)
    df["timestamp"] = df["timestamp"].astype(int)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["block"] = df["number"].astype(int)
    df["chain_id"] = chain.chain_id
    return df[["block", "datetime", "timestamp", "chain_id"]]


def ensure_blocks_is_current():
    """Make sure that we have a the highest block for each day (UTC) since Sep-10-2024 (when autoETH was deployed)"""
    for chain in ALL_CHAINS:
        # this is not correct it assumes taht you have all the blocks already h that is not a good assumption
        highest_datetime = get_highest_value_in_field_where(
            Blocks, Blocks.datetime, where_clause=Blocks.chain_id == chain.chain_id
        )

        if highest_datetime is None:
            highest_datetime = datetime.fromtimestamp(1726365887, tz=timezone.utc)  # when autoETH was deployed

        yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)
        if highest_datetime.date() < yesterday.date():
            highest_timestamp = int(highest_datetime.timestamp())
            new_timestamps = _compute_highest_timestamp_of_each_day(highest_timestamp)
            blocks_df = _fetch_block_df_from_subgraph(chain, new_timestamps)
            add_blocks_from_dataframe_to_database(blocks_df)


def _compute_highest_timestamp_of_each_day(start_timestamp: int) -> list[int]:
    start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
    first_second_of_today = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = []
    current_day = start_dt.replace(hour=23, minute=59, second=59, microsecond=0)

    while current_day < first_second_of_today:
        timestamps.append(int(current_day.timestamp()))
        current_day += timedelta(days=1)

    return timestamps


def ensure_all_blocks_are_in_table(blocks: list[int], chain: ChainData) -> list[Blocks]:

    from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks

    blocks_to_add = get_subset_not_already_in_column(
        table=Blocks, column=Blocks.block, values=blocks, where_clause=Blocks.chain_id == chain.chain_id
    )
    if blocks_to_add:
        df = get_raw_state_by_blocks([], blocks, chain, include_block_number=True)
        df["chain_id"] = chain.chain_id
        add_blocks_from_dataframe_to_database(df)


if __name__ == "__main__":
    ensure_blocks_is_current()
