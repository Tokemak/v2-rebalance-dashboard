import pandas as pd
from multicall import Multicall, Call
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import nest_asyncio
import asyncio
import random
from mainnet_launch.app.app_config import SEMAPHORE_LIMITS_FOR_MULTICALL
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    get_all_rows_in_table_by_chain,
)
from mainnet_launch.database.should_update_database import should_update_table


from mainnet_launch.constants import ChainData, TokemakAddress, ALL_CHAINS

# needed to run these functions in a jupyter notebook
nest_asyncio.apply()


MULTICALL2_DEPLOYMENT_BLOCK = 12336033
MULTICALL_V3 = TokemakAddress(
    eth="0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696", base="0xcA11bde05977b3631167028862bE2a173976CA11"
)


def get_state_by_one_block(calls: list[Call], block: int, chain: ChainData):
    return asyncio.run(safe_get_raw_state_by_block_one_block(calls, int(block), chain))


async def safe_get_raw_state_by_block_one_block(calls: list[Call], block: int, chain: ChainData) -> dict:
    multicall = Multicall(calls=calls, block_id=block, _w3=chain.client, require_success=False)
    response = await multicall.coroutine()
    return response


def build_get_address_eth_balance_call(name: str, addr: str, chain: ChainData) -> Call:
    """Use the multicallV3 contract to get the normalized eth balance of an address"""
    return Call(
        MULTICALL_V3(chain),
        ["getEthBalance(address)(uint256)", addr],
        [(name, safe_normalize_with_bool_success)],
    )


def _build_default_block_and_timestamp_calls(chain: ChainData):
    get_block_call = Call(
        MULTICALL_V3(chain),
        ["getBlockNumber()(uint256)"],
        [("block", identity_with_bool_success)],
    )

    get_timestamp_call = Call(
        MULTICALL_V3(chain),
        ["getCurrentBlockTimestamp()(uint256)"],
        [("timestamp", identity_with_bool_success)],
    )
    return get_block_call, get_timestamp_call


def _data_fetch_builder(semaphore: asyncio.Semaphore, responses: list, failed_multicalls: list):
    async def _fetch_data(multicall: Multicall):
        async with semaphore:
            try:
                response = await multicall.coroutine()
                responses.append(response)
            except Exception:
                # if (e.args[0]["code"] == -32000) | (e.args[0]["code"] == 502): bad historical call, rate limited
                failed_multicalls.append(multicall)

    return _fetch_data


def get_raw_state_by_blocks(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple[int] = SEMAPHORE_LIMITS_FOR_MULTICALL,
    include_block_number: bool = False,
) -> pd.DataFrame:
    if len(blocks) == 0:
        raise ValueError("Blocks cannot be empty")

    return asyncio.run(async_safe_get_raw_state_by_block(calls, blocks, chain, semaphore_limits, include_block_number))


async def async_safe_get_raw_state_by_block(
    calls: list[Call],
    blocks: list[int],
    chain: ChainData,
    semaphore_limits: tuple[int] = SEMAPHORE_LIMITS_FOR_MULTICALL,
    include_block_number: bool = False,
) -> pd.DataFrame:
    """
    Fetch a DataFame of each call in calls for each block in blocks fast
    """
    blocks_as_ints = [int(b) for b in blocks]
    # note only works after the multicall_v3 contract was deployed
    # block 12336033 (Apr-29-2021) on mainnet
    # block 5022 (Jun-15-2023) on Base
    # mostly a non issue but keep in mind that this only works on recent (within last 3 years) data

    get_block_call, get_timestamp_call = _build_default_block_and_timestamp_calls(chain)
    pending_multicalls = [
        Multicall(
            calls=[*calls, get_block_call, get_timestamp_call],
            block_id=int(block),
            _w3=chain.client,
            require_success=False,
        )
        for block in blocks_as_ints
    ]

    responses = []
    failed_multicalls = []
    calls_remaining = [m for m in pending_multicalls]
    for semaphore_limit in semaphore_limits:
        # make a lot of calls very fast, then slowly back off and remake the calls that failed
        semaphore = asyncio.Semaphore(semaphore_limit)
        failed_multicalls = []
        _ratelimited_async_data_fetcher = _data_fetch_builder(semaphore, responses, failed_multicalls)
        await asyncio.gather(*[_ratelimited_async_data_fetcher(m) for m in calls_remaining])

        calls_remaining = [f for f in failed_multicalls]
        if len(calls_remaining) == 0:
            break

    df = pd.DataFrame.from_records(responses)
    if len(df) == 0:
        print(
            "failed to fetch any data. consider trying again if expected to get data, but with a smaller semaphore_limit"
        )
        print(f"{len(blocks_as_ints)=} {blocks_as_ints[0]=} {blocks_as_ints[-1]=}")
        print(f"{calls=}")

    df.set_index("timestamp", inplace=True)
    df.index = pd.to_datetime(df.index, unit="s", utc=True)
    df.sort_index(inplace=True)
    df["block"] = df["block"].astype(int)
    if not include_block_number:
        df.drop(columns="block", inplace=True)
    if len(df) > 0:
        current_date = datetime.datetime.now(datetime.timezone.utc).date()
        current_utc_datetime = datetime.datetime.combine(
            current_date, datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc)
        )
        df = df[df.index < current_utc_datetime].copy()
    return df


def safe_normalize_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e18
    return None


def safe_normalize_6_with_bool_success(success: int, value: int):
    if success:
        return int(value) / 1e6
    return None


def to_str_with_bool_success(success, value):
    if success:
        return str(value)
    return None


def identity_with_bool_success(success, value):
    if success:
        return value
    return None


def identity_function(value):
    return value


BLOCKS_TO_USE_TABLE = "BLOCKS_TO_USE_TABLE"


def _fetch_blocks_to_use_from_external_source(start_block: int, end_block: int, chain: ChainData) -> pd.DataFrame:
    """Returns a table of block, timestamp in chain for the highest block in each day between start and end block"""
    block_timestamp_cache = {}

    def get_block_timestamp(block: int, chain: ChainData) -> pd.Timestamp:
        if block not in block_timestamp_cache:
            max_attempts = 3
            current_attempt = 0
            while current_attempt < max_attempts:
                try:
                    block_timestamp_cache[block] = pd.to_datetime(
                        chain.client.eth.get_block(int(block)).timestamp, unit="s", utc=True
                    )
                    break
                except Exception as e:
                    if current_attempt < max_attempts:
                        raise e
                    else:
                        time.sleep((2**current_attempt) + random.uniform(0, 1))
                        current_attempt += 1

        return block_timestamp_cache[block]

    def is_last_block_of_day(block: int, chain: ChainData) -> bool:
        # a block is the highest block in a day
        # if and only if that the (block + 1).date() is the day after block.date()
        current_day = get_block_timestamp(block, chain).date()
        possible_next_day = get_block_timestamp(block + 1, chain).date()
        return (possible_next_day - current_day) == pd.Timedelta("1 days")

    def find_last_block_of_day(block: int, chain: ChainData) -> None | tuple[int, pd.Timestamp]:
        initial_time = get_block_timestamp(block, chain)
        # don't save anything for the current day
        if initial_time.date() == datetime.datetime.now().date():
            return None
        day_start = initial_time.normalize()  # midnigh of this day, (in the past)
        current_block = block
        while not is_last_block_of_day(current_block, chain):
            current_time = get_block_timestamp(current_block, chain)
            seconds_difference = (day_start - current_time).total_seconds()
            offset = seconds_difference // int(chain.approx_seconds_per_block)
            current_block += offset
        final_timestamp = get_block_timestamp(current_block, chain)
        return (current_block, final_timestamp)

    blocks = [b for b in range(start_block, end_block, 84600 // int(2 * chain.approx_seconds_per_block))]
    results = []
    with ThreadPoolExecutor(max_workers=len(blocks)) as executor:
        futures = [executor.submit(find_last_block_of_day, b, chain) for b in blocks]
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

    df = pd.DataFrame.from_records(results, columns=["block", "timestamp"]).sort_values("block").drop_duplicates()
    df["chain"] = chain.name
    return df


def _add_to_blocks_to_use_table():
    if should_update_table(BLOCKS_TO_USE_TABLE, max_latency=pd.Timedelta("23 hour")):
        for chain in ALL_CHAINS:
            highest_block = get_earliest_block_from_table_with_chain(BLOCKS_TO_USE_TABLE, chain)
            df = _fetch_blocks_to_use_from_external_source(highest_block, chain.client.eth.block_number, chain)
            if len(df) > 0:
                write_dataframe_to_table(df, BLOCKS_TO_USE_TABLE)


def build_blocks_to_use(chain: ChainData, start_block: int | None = None, end_block: int | None = None) -> list[int]:

    _add_to_blocks_to_use_table()

    df = get_all_rows_in_table_by_chain(BLOCKS_TO_USE_TABLE, chain)
    if end_block is None:
        end_block = df["block"].max()
    daily_df = df.resample("1D").last()
    blocks = daily_df["block"].to_list()
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    return [int(b) for b in blocks if (b >= start_block) and (b <= end_block)]


if __name__ == "__main__":
    from mainnet_launch.constants import *
    from mainnet_launch.database.database_operations import drop_table

    drop_table(BLOCKS_TO_USE_TABLE)

    @time_decorator
    def a():

        _add_to_blocks_to_use_table()

    a()
