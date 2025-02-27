import pandas as pd
from multicall import Multicall, Call
import datetime
import streamlit as st

import nest_asyncio
import asyncio
from mainnet_launch.app.app_config import STREAMLIT_IN_MEMORY_CACHE_TIME, SEMAPHORE_LIMITS_FOR_MULTICALL
from mainnet_launch.database.database_operations import (
    write_dataframe_to_table,
    get_earliest_block_from_table_with_chain,
    get_all_rows_in_table_by_chain,
    drop_table,
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


def _add_to_blocks_to_use_table():
    # drop_table(BLOCKS_TO_USE_TABLE)
    if should_update_table(BLOCKS_TO_USE_TABLE, max_latency=pd.Timedelta("1 hour")):
        for chain in ALL_CHAINS:
            highest_block = get_earliest_block_from_table_with_chain(BLOCKS_TO_USE_TABLE, chain)
            hour_blocks = _build_blocks_to_use_dont_clip(chain, start_block=highest_block, approx_num_blocks_per_day=48)

            # Retrieve the raw state for the given blocks, including block numbers
            df = get_raw_state_by_blocks([], hour_blocks, chain, include_block_number=True)
            df["chain"] = chain.name
            df = df.reset_index()
            write_dataframe_to_table(df, BLOCKS_TO_USE_TABLE)


# get the highest block for each day
def build_blocks_to_use(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 4
) -> list[int]:

    _add_to_blocks_to_use_table()

    df = get_all_rows_in_table_by_chain(BLOCKS_TO_USE_TABLE, chain)
    if end_block is None:
        end_block = df["block"].max()
    daily_df = df.resample("1D").last()
    blocks = daily_df["block"].to_list()
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    return [int(b) for b in blocks if (b > start_block) and (b <= end_block)]

    # """Returns a block approx every 6 hours. by default between when autopool was first deployed to the current block"""
    # # this is not the number of seconds between blocks is not constant
    # start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    # first_minute_of_current_day = datetime.datetime.combine(
    #     datetime.datetime.now(datetime.timezone.utc).date(), datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc)
    # )
    # # this is not correct
    # end_block = chain.client.eth.block_number if end_block is None else end_block
    # end_block_date_time = pd.to_datetime(chain.client.eth.get_block(end_block).timestamp, unit="s", utc=True)
    # blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day

    # while end_block_date_time > first_minute_of_current_day:
    #     end_block = end_block - blocks_hop
    #     end_block_date_time = pd.to_datetime(chain.client.eth.get_block(end_block).timestamp, unit="s", utc=True)
    # blocks = [b for b in range(start_block, end_block, blocks_hop)]
    # return blocks


def _build_blocks_to_use_dont_clip(
    chain: ChainData, start_block: int | None = None, end_block: int | None = None, approx_num_blocks_per_day: int = 48
) -> list[int]:
    """Returns a block approx every 6 hours. by default between when autopool was first deployed to the current block"""
    # this is not the number of seconds between blocks is not constant
    start_block = chain.block_autopool_first_deployed if start_block is None else start_block
    end_block = chain.client.eth.block_number if end_block is None else end_block
    blocks_hop = int(86400 / chain.approx_seconds_per_block) // approx_num_blocks_per_day

    blocks = [b for b in range(start_block, end_block, blocks_hop)]
    return blocks


if __name__ == "__main__":
    from mainnet_launch.constants import ETH_CHAIN

    b = build_blocks_to_use(ETH_CHAIN)
    print(b)
