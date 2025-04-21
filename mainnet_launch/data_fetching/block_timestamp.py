from datetime import datetime, timedelta, timezone
import time

from sqlalchemy.exc import SQLAlchemyError

import pandas as pd
import os
import requests
from dotenv import load_dotenv
import numpy as np
from sqlalchemy.dialects.postgresql import insert as pg_insert

from mainnet_launch.database.schema.full import Blocks, Session
from mainnet_launch.constants import ALL_CHAINS, ChainData


load_dotenv()


def add_blocks_from_dataframe_to_database(df: pd.DataFrame, chunk_size: int = 1000):
    if df.index.name == "timestamp":
        df["datetime"] = df.index

    required_columns = {"datetime", "block", "chain_id"}
    if not required_columns.issubset(df.columns):
        missing = required_columns - set(df.columns)
        raise ValueError(f"DataFrame is missing required columns: {missing}")

    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    records = df.to_dict(orient="records")

    stmt = (
        pg_insert(Blocks)
        .values(records[0])  # shape placeholder; executemany will handle the rest
        .on_conflict_do_nothing(index_elements=[Blocks.block, Blocks.chain_id])
    )

    try:
        # 6) transaction‐scoped session: commits on success, rolls back on error
        with Session.begin() as session:
            # 7) chunk large batches to avoid blowing out query size
            for i in range(0, len(records), chunk_size):
                batch = records[i : i + chunk_size]
                session.execute(stmt, batch)
    except SQLAlchemyError as e:
        # 8) preserve full traceback for easier debugging
        raise RuntimeError("Failed to insert blocks") from e


def _compute_highest_timestamp_of_each_day(start_timestamp: int = 1726365887) -> list[int]:
    # 1726365887 timestamp of block when autoETH was deployed
    start_dt = datetime.fromtimestamp(start_timestamp, tz=timezone.utc)
    yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)

    timestamps = []
    current_day = start_dt.replace(hour=23, minute=59, second=59, microsecond=0)

    while current_day.date() <= yesterday.date():
        timestamps.append(int(current_day.timestamp()))
        current_day += timedelta(days=1)

    return timestamps


def _fetch_block_df_from_subgraph(
    chain: ChainData,
    timestamps: list[int] | None = None,
    page_size: int = 1000,
) -> pd.DataFrame:
    timestamps_less_15_seconds = []
    for t in timestamps:
        for i in range(50):
            timestamps_less_15_seconds.append(t - i)

    match chain.name:
        case "eth":
            url = os.environ["mainnet_blocks_subgraph"]
        case "base":
            url = os.environ["base_blocks_subgraph"]
        case _:
            raise ValueError(f"Unsupported chain: {chain.name}")

    records: list[dict] = []
    skip = 0

    groups = np.array_split(timestamps_less_15_seconds, 20)
    for t in groups:

        ts_filter = f"timestamp_in: [{','.join(map(str, t))}]"
        where_clause = f"where: {{{ts_filter}}}"

        query = f"""
        {{
          blocks(first:{page_size}, {where_clause}
                 orderBy:number, orderDirection:desc) {{
            number
            timestamp
          }}
        }}"""
        r = requests.post(url, json={"query": query}, timeout=30)
        r.raise_for_status()
        page = r.json()["data"]["blocks"]
        records.extend(page)
        skip += page_size
        time.sleep(0.2)  # don't overwhelm API.  Not certain if needed
        # print(len(records))

    df = pd.DataFrame.from_records(records)
    df["datetime"] = pd.to_datetime(df["timestamp"].astype(int), unit="s", utc=True)
    df["block"] = df["number"].astype(int)
    df["chain_id"] = chain.chain_id
    df["date"] = df["datetime"].dt.date
    valid_blocks = df.groupby("date")["block"].max()
    block_df = df[df["block"].isin(valid_blocks)].reset_index(drop=True)
    block_df = block_df.sort_values("datetime")
    return df[["block", "datetime", "chain_id"]]


def main():

    timestamps = _compute_highest_timestamp_of_each_day()

    for chain in ALL_CHAINS:
        blocks_df = _fetch_block_df_from_subgraph(chain, timestamps)
        print(blocks_df.shape)
        add_blocks_from_dataframe_to_database(blocks_df)


if __name__ == "__main__":
    main()
