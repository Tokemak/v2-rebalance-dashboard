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



CHAIN_TO_DEFI_LLAMA_SLUG = {
    ETH_CHAIN: "ethereum",
    BASE_CHAIN: "base",
    SONIC_CHAIN: "sonic",
    ARBITRUM_CHAIN: "arbitrum",
    PLASMA_CHAIN: "plasma",
    LINEA_CHAIN: "linea",  # not tested
}

def _determine_missing_timestamps(chain: ChainData) -> list[int]:
    """
    3-query version:
      1) all UTC days since inception (calendar)
      2) per-day aggregates from blocks
      3) join/lead logic in pandas to decide which midnights we still need
    """

    # 1) Calendar days (UTC) since inception
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

    # 2) Per-day aggregates present in blocks table
    q_per_day = f"""
    select
      (datetime at time zone 'UTC')::date as day,
      max(block) as largest_block_today,
      min(block) as smallest_block_today
    from blocks
    where chain_id = {chain.chain_id}
    group by 1
    order by day asc
    """

    days_df = _exec_sql_and_cache(q_days)
    if days_df is None or days_df.empty:
        return []

    per_day_df = _exec_sql_and_cache(q_per_day)
    if per_day_df is None or per_day_df.empty:
        # No blocks at all: we need every day's midnight timestamp
        return days_df["unix_timestamp"].astype("int64").tolist()

    # 3) Merge + lead in pandas
    df = days_df.merge(per_day_df, on="day", how="left")

    df["smallest_block_tomorrow"] = df["smallest_block_today"].shift(-1)

    # we "know the top of today" iff both present and boundary continuity holds
    df["we_know_the_top_block_of_today"] = (
        df["largest_block_today"].notna()
        & df["smallest_block_tomorrow"].notna()
        & ((df["largest_block_today"] + 1) == df["smallest_block_tomorrow"])
    )

    missing = df.loc[~df["we_know_the_top_block_of_today"], "unix_timestamp"]

    return missing.astype("int64").tolist()



def ensure_all_blocks_are_in_table(blocks: list[int], chain: ChainData):
    """
    Inserts missing blocks for the chain. Returns count of blocks that were missing (attempted inserts).
    """
    if not blocks:
        return 0

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

        new_rows = [Blocks.from_record(r) for r in df.to_dict(orient="records")]
        insert_avoid_conflicts(new_rows, Blocks)


def ensure_blocks_is_current():
    """
    Make sure we have the boundary blocks needed to infer each UTC day's "top block":
        largest_block_today + 1 == smallest_block_tomorrow

    Handles empty/spotty tables by generating the full day series in SQL.
    """
    for chain in ALL_CHAINS:
        unix_timestamps = _determine_missing_timestamps(chain)

        blocks_to_add, failures = fetch_blocks_by_unix_timestamps_defillama(
            unix_timestamps=unix_timestamps,
            chain=chain,
            closest='after'
        )

        ensure_all_blocks_are_in_table(list(blocks_to_add), chain)

        print(
            f"[{chain.name}] timestamps_needed={len(unix_timestamps)} "
            f"fetched_unique_blocks={len(blocks_to_add)}"
            f"failures={len(failures)}"
        )


if __name__ == "__main__":
    ensure_blocks_is_current()
