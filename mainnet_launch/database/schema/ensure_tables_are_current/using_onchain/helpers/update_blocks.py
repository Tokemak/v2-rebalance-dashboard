import pandas as pd

from mainnet_launch.database.schema.full import Blocks
from mainnet_launch.database.postgres_operations import (
    insert_avoid_conflicts,
    get_subset_not_already_in_column,
    _exec_sql_and_cache,
)
from mainnet_launch.data_fetching.defi_llama.fetch_timestamp import (
    fetch_blocks_by_unix_timestamps_defillama,
)
from mainnet_launch.data_fetching.get_state_by_block import get_raw_state_by_blocks
from mainnet_launch.constants import *

"""
- We need the top and bottom block of each day
- Exact top and exact bottom block are not necessary, as long as there is at least 23:59 minutes between the top and bottom block timestamps for each day it is good enough
- This is because some chains have inconsistent block times
- Some an have several (sequential blocks) with the same timestamp (sonic)
"""


DAY_COMPLETENESS_SECONDS = (24 * 60 * 60) - 60  # 23 hours 59 minutes


def _determine_missing_timestamps(chain: ChainData) -> list[int]:
    """
    Day is complete iff max(datetime_utc) - min(datetime_utc) >= 23h59m.

    Returns unix timestamps (seconds) for days we consider incomplete, excluding the current UTC day.
    """

    q_days = f"""
    select
      gs::date as day,
      extract(epoch from gs::date)::bigint as unix_timestamp
    from generate_series(
      to_timestamp({chain.start_unix_timestamp})::date,
      (now() at time zone 'UTC')::date,
      interval '1 day'
    ) as gs
    order by day asc
    """

    # Pull both block-number bounds and timestamp bounds per UTC day
    q_per_day = f"""
    select
      (datetime at time zone 'UTC')::date as day,
      min(block) as smallest_block_today,
      max(block) as largest_block_today,
      min(datetime at time zone 'UTC') as smallest_block_datetime_utc,
      max(datetime at time zone 'UTC') as largest_block_datetime_utc
    from blocks
    where chain_id = {chain.chain_id}
    group by 1
    order by day asc
    """

    days_df = _exec_sql_and_cache(q_days)
    if days_df is None or days_df.empty:
        return []

    per_day_df = _exec_sql_and_cache(q_per_day)

    # If there are no blocks at all, everything before today is missing
    if per_day_df is None or per_day_df.empty:
        today = pd.Timestamp.now(tz="UTC").normalize()
        return days_df.loc[days_df["day"] < today.date(), "unix_timestamp"].astype("int64").tolist()

    df = days_df.merge(per_day_df, on="day", how="left").sort_values("day", ascending=True)

    # Exclude current UTC day (cannot be complete by definition)
    today = pd.Timestamp.now(tz="UTC").normalize().date()
    df = df[df["day"] < today]
    df = df.sort_index()
    # Compute per-day coverage in seconds
    df["coverage_seconds"] = (
        pd.to_datetime(df["largest_block_datetime_utc"], utc=True)
        - pd.to_datetime(df["smallest_block_datetime_utc"], utc=True)
    ).dt.total_seconds()

    # Day is complete if we have timestamps and >= 23h59m coverage
    df["day_complete"] = df["coverage_seconds"].notna() & (df["coverage_seconds"] >= DAY_COMPLETENESS_SECONDS)

    missing_df = df[~df["day_complete"]]

    if missing_df.empty:
        return []

    print(f"Chain {chain.name} missing days")
    print(missing_df.head(12))
    missing_ts = set()

    for unix_ts in missing_df["unix_timestamp"]:
        # note: this fetches some redundent blocks, but it is fine
        # can optimize later if needed fetching a few extra blocks for each day is reasonable
        N = 15  # I want to fetch blocks a bit before and after the target timestamps to be safe
        missing_ts.add(int(unix_ts) + N)
        missing_ts.add(int(unix_ts) - N)
        missing_ts.add(int(unix_ts + 86400 - N))
        missing_ts.add(int(unix_ts + 86400 + N))

    print(f"determined {len(missing_ts)} missing timestamps for chain {chain.name}")

    return sorted(list(missing_ts))


def ensure_all_blocks_are_in_table(blocks: list[int], chain: ChainData):
    """
    Inserts missing blocks for the chain. Returns count of blocks that were missing (attempted inserts).
    """
    if not blocks:
        return

    blocks_to_add = get_subset_not_already_in_column(
        table=Blocks,
        column=Blocks.block,
        values=blocks,
        where_clause=Blocks.chain_id == chain.chain_id,
    )

    if blocks_to_add:
        df = get_raw_state_by_blocks([], blocks_to_add, chain, include_block_number=True)
        df["chain_id"] = chain.chain_id
        df["datetime"] = pd.to_datetime(df.index, utc=True)
        print("blocks_fetched")
        print(df.head(12))
        new_rows = [Blocks.from_record(r) for r in df.to_dict(orient="records")]
        insert_avoid_conflicts(new_rows, Blocks)


def ensure_blocks_is_current():
    """
    Make sure we have the boundary blocks needed to infer each UTC day's "top block":
        largest_block_today + 1 == smallest_block_tomorrow

    Handles empty/spotty tables by generating the full day series in SQL.
    """
    # sonic keeps fetching even though I would think it should be current
    for chain in ALL_CHAINS:
        unix_timestamps = _determine_missing_timestamps(chain)
        if len(unix_timestamps) == 0:
            print(f"{chain.name} blocks table is already current.")
            continue

        blocks_to_add = fetch_blocks_by_unix_timestamps_defillama(
            unix_timestamps=unix_timestamps,
            chain=chain,
        )
        print(f"{blocks_to_add=} for {len(unix_timestamps)} missing timestamps")
        print("Fetched blocks from DeFiLlama:", blocks_to_add)

        ensure_all_blocks_are_in_table(blocks_to_add, chain)

        print(f"[{chain.name}] timestamps_needed={len(unix_timestamps)} " f"fetched_unique_blocks={len(blocks_to_add)}")


if __name__ == "__main__":
    from mainnet_launch.constants import profile_function

    profile_function(ensure_blocks_is_current)
